package amqp

import (
	"errors"
	"fmt"
	"sync"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
)

// Session is one AMQP session. Its state is owned by the connection's frame
// loop, except for the sender lookup table, which is guarded by mu.
type Session struct {
	conn       *conn
	inChannel  uint16
	outChannel uint16

	// Incoming flow state.
	nextIncomingID uint32
	incomingWindow uint32

	// Outgoing flow state.
	nextOutgoingID       uint32
	nextDeliveryID       uint32
	remoteIncomingWindow uint32
	maxRemoteWindow      uint32

	linksIn  map[uint32]linkEndpoint // keyed by the peer's handle
	linksOut map[uint32]linkEndpoint // keyed by our handle

	// unsettledOut tracks deliveries we sent that the peer has not settled,
	// keyed by delivery-id.
	unsettledOut map[uint32]*outgoing

	// detaching holds the peer handles of links this side detached (or
	// refused) whose detach reply has not arrived yet. Frames for these
	// handles are ignored.
	detaching map[uint32]struct{}

	mu        sync.Mutex
	byTarget  map[string]*SenderLink
	byName    map[string]*SenderLink
	closeOnce sync.Once
}

// linkEndpoint is implemented by *ReceiverLink and *SenderLink.
type linkEndpoint interface {
	base() *link
}

// sessionError is an error that ends the session but keeps the connection.
type sessionError struct{ err *frames.Error }

func (e *sessionError) Error() string { return e.err.Error() }

func newSession(c *conn, in, out uint16, b *frames.Begin) *Session {
	return &Session{
		conn:                 c,
		inChannel:            in,
		outChannel:           out,
		nextIncomingID:       b.NextOutgoingID,
		incomingWindow:       DefaultSessionWindow,
		remoteIncomingWindow: b.IncomingWindow,
		maxRemoteWindow:      b.IncomingWindow,
		linksIn:              make(map[uint32]linkEndpoint),
		linksOut:             make(map[uint32]linkEndpoint),
		unsettledOut:         make(map[uint32]*outgoing),
		detaching:            make(map[uint32]struct{}),
		byTarget:             make(map[string]*SenderLink),
		byName:               make(map[string]*SenderLink),
	}
}

// ReplyLink returns the link this server sends replies on for a request
// whose reply-to is replyTo: the link with that target address or, as some
// clients (such as the JavaScript SDK) expect, with that name.
func (s *Session) ReplyLink(replyTo string) *SenderLink {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.byTarget[replyTo]; ok {
		return l
	}
	return s.byName[replyTo]
}

func (s *Session) write(body frames.Body, payload []byte) error {
	return s.conn.writeFrame(frames.TypeAMQP, s.outChannel, body, payload)
}

func (s *Session) handleFrame(body frames.Body) error {
	switch b := body.(type) {
	case *frames.Attach:
		return s.handleAttach(b)
	case *frames.Flow:
		return s.handleFlow(b)
	case *frames.Transfer:
		return s.handleTransfer(b)
	case *frames.Disposition:
		return s.handleDisposition(b)
	case *frames.Detach:
		return s.handleDetach(b)
	case *frames.End:
		s.teardown()
		delete(s.conn.sessionsIn, s.inChannel)
		delete(s.conn.sessionsOut, s.outChannel)
		return s.write(&frames.End{}, nil)
	}
	return &connError{NewError(CondIllegalState, fmt.Sprintf("unexpected %T on session", body))}
}

func (s *Session) freeHandle() uint32 {
	for h := uint32(0); ; h++ {
		if _, used := s.linksOut[h]; !used {
			return h
		}
	}
}

func (s *Session) handleAttach(a *frames.Attach) error {
	delete(s.detaching, a.Handle)
	if _, ok := s.linksIn[a.Handle]; ok {
		return &sessionError{NewError(CondHandleInUse, fmt.Sprintf("handle %d already attached", a.Handle))}
	}
	l := newLink(s, a)
	if a.Role == frames.RoleSender {
		return s.attachReceiver(&ReceiverLink{link: l})
	}
	return s.attachSender(&SenderLink{link: l})
}

func (s *Session) register(e linkEndpoint) {
	l := e.base()
	s.linksIn[l.remoteHandle] = e
	s.linksOut[l.handle] = e
}

func (s *Session) unregister(e linkEndpoint) {
	l := e.base()
	delete(s.linksIn, l.remoteHandle)
	delete(s.linksOut, l.handle)
	if sl, ok := e.(*SenderLink); ok && sl.target != nil {
		s.mu.Lock()
		if s.byTarget[sl.target.Address] == sl {
			delete(s.byTarget, sl.target.Address)
		}
		if s.byName[sl.name] == sl {
			delete(s.byName, sl.name)
		}
		s.mu.Unlock()
	}
	l.cancel()
}

// refuse completes the attach handshake for a refused link and detaches it
// with err, as AMQP 1.0 section 2.6.3 describes.
func (s *Session) refuse(l *link, role frames.Role, err error) error {
	reply := &frames.Attach{
		Name:               l.name,
		Handle:             l.handle,
		Role:               role,
		SenderSettleMode:   l.remote.SenderSettleMode,
		ReceiverSettleMode: l.remote.ReceiverSettleMode,
	}
	// The terminus this side would have created is left null.
	if role == frames.RoleReceiver {
		reply.Source = l.remote.Source
	} else {
		reply.Target = l.remote.Target
		reply.Coordinator = l.remote.Coordinator
		zero := uint32(0)
		reply.InitialDeliveryCount = &zero
	}
	if werr := s.write(reply, nil); werr != nil {
		return werr
	}
	l.cancel()
	s.detaching[l.remoteHandle] = struct{}{}
	return s.write(&frames.Detach{Handle: l.handle, Closed: true, Error: asAMQPError(err)}, nil)
}

func (s *Session) attachReceiver(r *ReceiverLink) error {
	l := r.link
	r.maxCredit = s.conn.server.receiverCredit()
	r.deliveryCount = 0
	if l.remote.InitialDeliveryCount != nil {
		r.deliveryCount = *l.remote.InitialDeliveryCount
	}
	handler, err := s.conn.server.Handler.AttachReceiver(l.ctx, r)
	if err != nil {
		return s.refuse(l, frames.RoleReceiver, err)
	}
	r.handler = handler
	s.register(r)
	if err := s.write(&frames.Attach{
		Name:               l.name,
		Handle:             l.handle,
		Role:               frames.RoleReceiver,
		SenderSettleMode:   l.remote.SenderSettleMode,
		ReceiverSettleMode: l.remote.ReceiverSettleMode,
		Source:             l.remote.Source,
		Target:             l.remote.Target,
		Coordinator:        l.remote.Coordinator,
		MaxMessageSize:     r.maxMessageSize(),
		Properties:         l.Properties,
	}, nil); err != nil {
		return err
	}
	r.start()
	return r.grantCredit()
}

func (s *Session) attachSender(sl *SenderLink) error {
	l := sl.link
	sl.source = cloneSource(l.remote.Source)
	sl.target = l.remote.Target
	err := s.conn.server.Handler.AttachSender(l.ctx, sl)
	if errors.Is(err, ErrAttachPending) {
		// The handler answers later through CompleteAttach. Reserve the
		// handles so the peer's frames for this link are recognized.
		sl.attachPending = true
		s.register(sl)
		return nil
	}
	if err != nil {
		return s.refuse(l, frames.RoleSender, err)
	}
	s.register(sl)
	return s.acceptSender(sl)
}

// acceptSender sends the attach reply of an accepted sending link.
func (s *Session) acceptSender(sl *SenderLink) error {
	l := sl.link
	s.mu.Lock()
	if sl.target != nil && sl.target.Address != "" {
		s.byTarget[sl.target.Address] = sl
	}
	s.byName[sl.name] = sl
	s.mu.Unlock()
	zero := uint32(0)
	return s.write(&frames.Attach{
		Name:                 l.name,
		Handle:               l.handle,
		Role:                 frames.RoleSender,
		SenderSettleMode:     l.remote.SenderSettleMode,
		ReceiverSettleMode:   l.remote.ReceiverSettleMode,
		Source:               sl.source,
		Target:               sl.target,
		InitialDeliveryCount: &zero,
		MaxMessageSize:       s.conn.server.MaxMessageSize,
		Properties:           l.Properties,
	}, nil)
}

func cloneSource(src *frames.Source) *frames.Source {
	if src == nil {
		return nil
	}
	c := *src
	c.Filter = append(c.Filter[:0:0], src.Filter...)
	return &c
}

func (s *Session) handleFlow(f *frames.Flow) error {
	// Recompute how many transfers the peer can still accept (2.5.6).
	nextIncoming := uint32(0)
	if f.NextIncomingID != nil {
		nextIncoming = *f.NextIncomingID
	}
	s.remoteIncomingWindow = nextIncoming + f.IncomingWindow - s.nextOutgoingID
	s.maxRemoteWindow = max(s.maxRemoteWindow, f.IncomingWindow)

	if f.Handle == nil {
		if f.Echo {
			return s.sendSessionFlow()
		}
		return s.flushAll()
	}
	e, ok := s.linksIn[*f.Handle]
	if !ok {
		if _, detaching := s.detaching[*f.Handle]; detaching {
			return nil
		}
		return &sessionError{NewError(CondUnattachedHandle, fmt.Sprintf("flow on unattached handle %d", *f.Handle))}
	}
	switch l := e.(type) {
	case *SenderLink:
		if err := l.handleFlow(f); err != nil {
			return err
		}
	case *ReceiverLink:
		if f.Echo {
			if err := l.sendFlow(); err != nil {
				return err
			}
		}
	}
	return s.flushAll()
}

func (s *Session) sendSessionFlow() error {
	next := s.nextIncomingID
	return s.write(&frames.Flow{
		NextIncomingID: &next,
		IncomingWindow: s.incomingWindow,
		NextOutgoingID: s.nextOutgoingID,
		OutgoingWindow: DefaultSessionWindow,
	}, nil)
}

// flushAll lets every sending link use newly available session window.
func (s *Session) flushAll() error {
	for _, e := range s.linksOut {
		if l, ok := e.(*SenderLink); ok {
			if err := l.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Session) handleTransfer(t *frames.Transfer) error {
	if s.incomingWindow == 0 {
		return &sessionError{NewError(CondWindowViolation, "transfer beyond incoming window")}
	}
	s.nextIncomingID++
	s.incomingWindow--
	if s.incomingWindow < DefaultSessionWindow/2 {
		s.incomingWindow = DefaultSessionWindow
		if err := s.sendSessionFlow(); err != nil {
			return err
		}
	}
	e, ok := s.linksIn[t.Handle]
	if !ok {
		if _, detaching := s.detaching[t.Handle]; detaching {
			return nil
		}
		return &sessionError{NewError(CondUnattachedHandle, fmt.Sprintf("transfer on unattached handle %d", t.Handle))}
	}
	r, ok := e.(*ReceiverLink)
	if !ok {
		return &sessionError{NewError(CondErrantLink, "transfer on a link where the peer is the receiver")}
	}
	return r.handleTransfer(t)
}

func (s *Session) handleDisposition(d *frames.Disposition) error {
	last := d.First
	if d.Last != nil {
		last = *d.Last
	}
	if last-d.First > 1<<20 {
		return &sessionError{NewError(CondInvalidField, "disposition range too large")}
	}
	if d.Role == frames.RoleSender {
		// The peer settled deliveries it sent us in receiver-settle-mode
		// second. Nothing remains to track for them.
		return nil
	}
	for id := d.First; ; id++ {
		if o, ok := s.unsettledOut[id]; ok {
			if err := s.settleOutgoing(id, o, d); err != nil {
				return err
			}
		}
		if id == last {
			break
		}
	}
	return nil
}

// settleOutgoing processes the peer's disposition of a delivery this side
// sent. Without a terminal state there is nothing to do yet.
func (s *Session) settleOutgoing(id uint32, o *outgoing, d *frames.Disposition) error {
	if d.State == nil {
		if d.Settled {
			delete(s.unsettledOut, id)
		}
		return nil
	}
	delete(s.unsettledOut, id)
	if d.Settled {
		// Receiver settle mode first: the outcome is final.
		if o.onSettle != nil {
			o.onSettle(d.State, func(frames.DeliveryState) {})
		}
		return nil
	}
	// Receiver settle mode second: the peer waits for this side to settle
	// with the final outcome.
	if o.onSettle == nil {
		return s.write(&frames.Disposition{Role: frames.RoleSender, First: id, Settled: true, State: d.State}, nil)
	}
	var once sync.Once
	o.onSettle(d.State, func(final frames.DeliveryState) {
		once.Do(func() {
			// settle may run on the frame loop itself, where a synchronous
			// post would deadlock.
			go s.conn.post(func() {
				if err := s.write(&frames.Disposition{Role: frames.RoleSender, First: id, Settled: true, State: final}, nil); err != nil {
					s.conn.shutdown(NewError(CondInternalError, err.Error()))
				}
			})
		})
	})
	return nil
}

func (s *Session) handleDetach(d *frames.Detach) error {
	e, ok := s.linksIn[d.Handle]
	if !ok {
		if _, detaching := s.detaching[d.Handle]; detaching {
			// The peer's reply to a detach this side initiated.
			delete(s.detaching, d.Handle)
			return nil
		}
		return &sessionError{NewError(CondUnattachedHandle, fmt.Sprintf("detach on unattached handle %d", d.Handle))}
	}
	if d.Error != nil {
		s.conn.log.Debug("peer detached link with error", "link", e.base().name, "err", d.Error)
	}
	s.unregister(e)
	if sl, ok := e.(*SenderLink); ok {
		sl.failPending(ErrLinkDetached)
		if sl.attachPending {
			// The peer gave up before the attach completed. Answer the
			// attach without a terminus so the exchange stays well formed.
			sl.attachPending = false
			zero := uint32(0)
			if err := s.write(&frames.Attach{
				Name:                 sl.name,
				Handle:               sl.handle,
				Role:                 frames.RoleSender,
				Target:               sl.remote.Target,
				InitialDeliveryCount: &zero,
			}, nil); err != nil {
				return err
			}
		}
	}
	return s.write(&frames.Detach{Handle: e.base().handle, Closed: d.Closed}, nil)
}

// detachLink detaches a link locally with err.
func (s *Session) detachLink(e linkEndpoint, err *frames.Error) error {
	s.unregister(e)
	s.detaching[e.base().remoteHandle] = struct{}{}
	if sl, ok := e.(*SenderLink); ok {
		sl.failPending(ErrLinkDetached)
	}
	return s.write(&frames.Detach{Handle: e.base().handle, Closed: true, Error: err}, nil)
}

// teardown cancels every link of the session.
func (s *Session) teardown() {
	s.closeOnce.Do(func() {
		for _, e := range s.linksIn {
			s.unregister(e)
			if sl, ok := e.(*SenderLink); ok {
				sl.failPending(ErrConnClosed)
			}
		}
	})
}
