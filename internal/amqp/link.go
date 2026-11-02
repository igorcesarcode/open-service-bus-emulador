package amqp

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
)

// link holds the state shared by both link directions.
type link struct {
	session      *Session
	name         string
	handle       uint32 // ours
	remoteHandle uint32
	remote       *frames.Attach

	// Properties are sent in the attach reply. Handlers may set them.
	Properties encoding.Map

	ctx    context.Context
	cancel context.CancelFunc
}

func newLink(s *Session, a *frames.Attach) *link {
	ctx, cancel := context.WithCancel(s.conn.ctx)
	return &link{
		session:      s,
		name:         a.Name,
		handle:       s.freeHandle(),
		remoteHandle: a.Handle,
		remote:       a,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (l *link) base() *link { return l }

// Name returns the link name chosen by the peer.
func (l *link) Name() string { return l.name }

// RemoteAttach returns the attach frame the peer sent.
func (l *link) RemoteAttach() *frames.Attach { return l.remote }

// Session returns the session the link belongs to.
func (l *link) Session() *Session { return l.session }

// Context returns a context that is canceled when the link detaches or
// the connection closes.
func (l *link) Context() context.Context { return l.ctx }

// ReceiverLink is a link on which the peer sends messages to this server.
type ReceiverLink struct {
	*link

	// MaxMessageSize, when set by the handler before the attach completes,
	// replaces the server's limit for this link: it is advertised to the
	// peer and enforced.
	MaxMessageSize uint64

	handler MessageHandler

	// Flow control state, owned by the frame loop.
	maxCredit     uint32
	peerCredit    uint32
	deliveryCount uint32
	outstanding   uint32
	partial       *Delivery

	deliveries chan *Delivery
}

func (r *ReceiverLink) maxMessageSize() uint64 {
	if r.MaxMessageSize > 0 {
		return r.MaxMessageSize
	}
	return r.session.conn.server.MaxMessageSize
}

// Address returns the target address the peer sends to.
func (r *ReceiverLink) Address() string {
	if r.remote.Target == nil {
		return ""
	}
	return r.remote.Target.Address
}

// Coordinator returns the coordinator terminus of a link to a transaction
// coordinator, or nil for other links.
func (r *ReceiverLink) Coordinator() *frames.Coordinator { return r.remote.Coordinator }

// Source returns the peer's source terminus.
func (r *ReceiverLink) Source() *frames.Source { return r.remote.Source }

// Target returns the peer's target terminus.
func (r *ReceiverLink) Target() *frames.Target { return r.remote.Target }

// start launches the goroutine that hands deliveries to the handler.
func (r *ReceiverLink) start() {
	r.deliveries = make(chan *Delivery, r.maxCredit)
	go func() {
		for {
			select {
			case d := <-r.deliveries:
				state := r.handler.HandleMessage(r.ctx, d)
				if state == nil {
					state = &frames.Accepted{}
				}
				r.session.conn.post(func() { r.complete(d, state) })
			case <-r.ctx.Done():
				return
			}
		}
	}()
}

// grantCredit tops up the peer's credit so that outstanding plus credit
// equals maxCredit.
func (r *ReceiverLink) grantCredit() error {
	r.peerCredit = r.maxCredit - r.outstanding
	return r.sendFlow()
}

func (r *ReceiverLink) sendFlow() error {
	s := r.session
	next := s.nextIncomingID
	handle := r.handle
	count := r.deliveryCount
	credit := r.peerCredit
	return s.write(&frames.Flow{
		NextIncomingID: &next,
		IncomingWindow: s.incomingWindow,
		NextOutgoingID: s.nextOutgoingID,
		OutgoingWindow: DefaultSessionWindow,
		Handle:         &handle,
		DeliveryCount:  &count,
		LinkCredit:     &credit,
	}, nil)
}

func (r *ReceiverLink) handleTransfer(t *frames.Transfer) error {
	if r.partial == nil {
		if r.peerCredit == 0 {
			return r.session.detachLink(r, NewError(CondTransferLimitExceeded, "transfer without credit"))
		}
		if t.DeliveryID == nil {
			return &sessionError{NewError(CondInvalidField, "first transfer of a delivery has no delivery-id")}
		}
		d := &Delivery{ID: *t.DeliveryID, Tag: t.DeliveryTag, State: t.State, link: r}
		if t.MessageFormat != nil {
			d.Format = *t.MessageFormat
		}
		r.partial = d
	}
	d := r.partial
	if t.Aborted {
		r.partial = nil
		return nil
	}
	d.Settled = d.Settled || t.Settled
	if !d.oversized {
		if d.Payload == nil && !t.More {
			// The whole delivery is in this frame, whose buffer nothing
			// else uses: keep the slice instead of copying it.
			d.Payload = t.Payload
		} else {
			d.Payload = append(d.Payload, t.Payload...)
		}
		if limit := r.maxMessageSize(); limit > 0 && uint64(len(d.Payload)) > limit {
			// The rest of the delivery is read and dropped; the delivery is
			// rejected once complete, and the link stays usable.
			d.oversized = true
			d.Payload = nil
		}
	}
	if t.More {
		return nil
	}
	r.partial = nil
	r.peerCredit--
	r.deliveryCount++
	r.outstanding++
	if d.oversized {
		r.complete(d, &frames.Rejected{Error: NewError(CondMessageSizeExceeded,
			fmt.Sprintf("message exceeds %d bytes", r.maxMessageSize()))})
		return nil
	}
	// Capacity equals maxCredit and outstanding never exceeds it, so this
	// send does not block.
	r.deliveries <- d
	return nil
}

// complete reports the outcome of d and replenishes credit. It runs on the
// frame loop.
func (r *ReceiverLink) complete(d *Delivery, state frames.DeliveryState) {
	s := r.session
	if _, attached := s.linksOut[r.handle]; !attached || s.linksOut[r.handle] != linkEndpoint(r) {
		return
	}
	r.outstanding--
	if !d.Settled {
		settled := r.remote.ReceiverSettle() == frames.ReceiverSettleModeFirst
		if err := s.write(&frames.Disposition{
			Role:    frames.RoleReceiver,
			First:   d.ID,
			Settled: settled,
			State:   state,
		}, nil); err != nil {
			s.conn.shutdown(NewError(CondInternalError, err.Error()))
			return
		}
	}
	if r.peerCredit <= r.maxCredit/2 {
		if err := r.grantCredit(); err != nil {
			s.conn.shutdown(NewError(CondInternalError, err.Error()))
		}
	}
}

// SenderLink is a link on which this server sends messages to the peer.
type SenderLink struct {
	*link

	source *frames.Source
	target *frames.Target

	// ManualDrain, when set during AttachSender, leaves drain requests
	// pending until CompleteDrain is called, so the handler can deliver
	// available messages first. Otherwise a drain completes as soon as no
	// message is queued.
	ManualDrain bool

	// Flow control state, owned by the frame loop.
	credit        uint32
	deliveryCount uint32
	drain         bool
	pending       []*outgoing
	nextTag       uint64
	// attachPending is set while the handler has not completed the attach.
	attachPending bool

	flowMu      sync.Mutex
	flowChanged chan struct{}
}

// outgoing is a message waiting for credit or for its outcome.
type outgoing struct {
	payload  []byte
	opts     SendOptions
	done     chan error
	onSettle SettleFunc
}

// SettleFunc receives the outcome the peer reported for an unsettled
// delivery. When the peer left the delivery unsettled (receiver settle
// mode second), the delivery stays open until settle is called with the
// final outcome, which is sent to the peer; settle may be called from any
// goroutine. When the peer already settled, settle does nothing.
type SettleFunc func(state frames.DeliveryState, settle func(final frames.DeliveryState))

// SendOptions configures SenderLink.Send.
type SendOptions struct {
	// Tag is the delivery tag. If empty, a unique tag is generated.
	Tag []byte
	// Settled sends the delivery pre-settled. It is forced on when the
	// peer requested sender-settle-mode settled.
	Settled bool
	// MessageFormat is the transfer message-format.
	MessageFormat uint32
	// OnSettle, if set, is called on the frame loop with the outcome the
	// peer reports for an unsettled delivery. It must not block. Without
	// it, the peer's outcome is accepted as final.
	OnSettle SettleFunc
}

// Address returns the source address the peer receives from.
func (l *SenderLink) Address() string {
	if l.source == nil {
		return ""
	}
	return l.source.Address
}

// Source returns the source terminus echoed to the peer. AttachSender may
// modify it, for example to report the value of a filter.
func (l *SenderLink) Source() *frames.Source { return l.source }

// Target returns the peer's target terminus.
func (l *SenderLink) Target() *frames.Target { return l.target }

// Send transfers an encoded message to the peer. It blocks until the peer
// grants credit and the transfer is written, ctx is done or the link
// detaches. Send does not wait for the delivery outcome.
func (l *SenderLink) Send(ctx context.Context, payload []byte, opts SendOptions) error {
	o := &outgoing{payload: payload, opts: opts, done: make(chan error, 1), onSettle: opts.OnSettle}
	if !l.session.conn.post(func() { l.enqueue(o) }) {
		return ErrConnClosed
	}
	select {
	case err := <-o.done:
		return err
	case <-ctx.Done():
		// Withdraw the message unless it was written in the meantime.
		if !l.session.conn.post(func() { l.withdraw(o, ctx.Err()) }) {
			return ErrConnClosed
		}
		return <-o.done
	}
}

func (l *SenderLink) enqueue(o *outgoing) {
	if l.ctx.Err() != nil {
		o.done <- ErrLinkDetached
		return
	}
	l.pending = append(l.pending, o)
	if err := l.flush(); err != nil {
		l.session.conn.shutdown(NewError(CondInternalError, err.Error()))
	}
}

func (l *SenderLink) withdraw(o *outgoing, err error) {
	for i, p := range l.pending {
		if p == o {
			l.pending = append(l.pending[:i], l.pending[i+1:]...)
			o.done <- err
			return
		}
	}
}

func (l *SenderLink) failPending(err error) {
	for _, o := range l.pending {
		o.done <- err
	}
	l.pending = nil
}

func (l *SenderLink) handleFlow(f *frames.Flow) error {
	// The receiver's view of delivery-count may lag ours; credit is
	// relative to it (2.6.7).
	count := l.deliveryCount
	if f.DeliveryCount != nil {
		count = *f.DeliveryCount
	}
	if f.LinkCredit != nil {
		l.credit = count + *f.LinkCredit - l.deliveryCount
	}
	l.drain = f.Drain
	l.signalFlow()
	if err := l.flush(); err != nil {
		return err
	}
	if f.Echo {
		return l.sendFlow()
	}
	return nil
}

// signalFlow wakes goroutines waiting in FlowChanged.
func (l *SenderLink) signalFlow() {
	l.flowMu.Lock()
	defer l.flowMu.Unlock()
	if l.flowChanged != nil {
		close(l.flowChanged)
		l.flowChanged = nil
	}
}

// CompleteAttach finishes an attach that AttachSender left pending by
// returning ErrAttachPending. A nil err accepts the link, echoing the
// current Source and Properties; otherwise the link is refused with err.
// It must be called from a goroutine other than the AttachSender call, and
// does nothing if the peer already detached.
func (l *SenderLink) CompleteAttach(err error) error {
	errc := make(chan error, 1)
	if !l.session.conn.post(func() {
		if !l.attachPending || l.ctx.Err() != nil {
			errc <- ErrLinkDetached
			return
		}
		l.attachPending = false
		s := l.session
		if err != nil {
			s.unregister(l)
			errc <- s.refuse(l.link, frames.RoleSender, err)
			return
		}
		if werr := s.acceptSender(l); werr != nil {
			errc <- werr
			return
		}
		errc <- l.flush()
	}) {
		return ErrConnClosed
	}
	return <-errc
}

// Detach closes the link from the server side, reporting err to the peer.
// Pending sends fail with ErrLinkDetached.
func (l *SenderLink) Detach(err *frames.Error) error {
	errc := make(chan error, 1)
	if !l.session.conn.post(func() {
		s := l.session
		if l.ctx.Err() != nil {
			errc <- nil
			return
		}
		if l.attachPending {
			l.attachPending = false
			s.unregister(l)
			errc <- s.refuse(l.link, frames.RoleSender, err)
			return
		}
		errc <- s.detachLink(l, err)
	}) {
		return ErrConnClosed
	}
	return <-errc
}

// FlowChanged returns a channel that is closed the next time the peer
// changes the link's credit or drain state. Obtain it before calling
// Credit so no change is missed.
func (l *SenderLink) FlowChanged() <-chan struct{} {
	l.flowMu.Lock()
	defer l.flowMu.Unlock()
	if l.flowChanged == nil {
		l.flowChanged = make(chan struct{})
	}
	return l.flowChanged
}

// Credit returns the credit available for messages not yet queued by Send
// and whether the peer requested a drain.
func (l *SenderLink) Credit() (credit uint32, drain bool, err error) {
	type result struct {
		credit uint32
		drain  bool
		err    error
	}
	ch := make(chan result, 1)
	if !l.session.conn.post(func() {
		if l.ctx.Err() != nil {
			ch <- result{err: ErrLinkDetached}
			return
		}
		avail := uint32(0)
		if q := uint32(len(l.pending)); l.credit > q {
			avail = l.credit - q
		}
		ch <- result{credit: avail, drain: l.drain}
	}) {
		return 0, false, ErrConnClosed
	}
	r := <-ch
	return r.credit, r.drain, r.err
}

// CompleteDrain ends a pending drain request once every queued message was
// sent: the remaining credit is consumed and the peer is told the drain is
// done. It does nothing if no drain is pending.
func (l *SenderLink) CompleteDrain() error {
	errc := make(chan error, 1)
	if !l.session.conn.post(func() {
		if l.ctx.Err() != nil || !l.drain || len(l.pending) > 0 {
			errc <- nil
			return
		}
		errc <- l.finishDrain()
	}) {
		return ErrConnClosed
	}
	return <-errc
}

// finishDrain consumes the remaining credit and tells the receiver the
// drain is complete, even if no credit was left (2.6.7).
func (l *SenderLink) finishDrain() error {
	l.deliveryCount += l.credit
	l.credit = 0
	err := l.sendFlow()
	l.drain = false
	return err
}

func (l *SenderLink) sendFlow() error {
	s := l.session
	next := s.nextIncomingID
	handle := l.handle
	count := l.deliveryCount
	credit := l.credit
	available := uint32(len(l.pending))
	return s.write(&frames.Flow{
		NextIncomingID: &next,
		IncomingWindow: s.incomingWindow,
		NextOutgoingID: s.nextOutgoingID,
		OutgoingWindow: DefaultSessionWindow,
		Handle:         &handle,
		DeliveryCount:  &count,
		LinkCredit:     &credit,
		Available:      &available,
		Drain:          l.drain,
	}, nil)
}

// flush sends pending messages while credit and session window allow, then
// completes a requested drain.
func (l *SenderLink) flush() error {
	if l.ctx.Err() != nil || l.attachPending {
		return nil
	}
	for len(l.pending) > 0 && l.credit > 0 {
		o := l.pending[0]
		sent, err := l.transfer(o)
		if err != nil {
			return err
		}
		if !sent {
			break
		}
		l.pending = l.pending[1:]
	}
	if l.drain && len(l.pending) == 0 && !l.ManualDrain {
		return l.finishDrain()
	}
	return nil
}

// transfer writes o as one or more transfer frames. It reports false when
// the session window is too small to start the delivery.
func (l *SenderLink) transfer(o *outgoing) (bool, error) {
	s := l.session
	c := s.conn

	settled := o.opts.Settled || l.remote.SenderSettle() == frames.SenderSettleModeSettled
	tag := o.opts.Tag
	if len(tag) == 0 {
		tag = binary.BigEndian.AppendUint64(nil, l.nextTag)
		l.nextTag++
	}
	deliveryID := s.nextDeliveryID
	format := o.opts.MessageFormat

	first := &frames.Transfer{
		Handle:        l.handle,
		DeliveryID:    &deliveryID,
		DeliveryTag:   tag,
		MessageFormat: &format,
		Settled:       settled,
		More:          true,
	}
	overhead, err := frames.PerformativeSize(first)
	if err != nil {
		return false, err
	}
	chunk := int(min(c.maxFrameOut, 1<<24)) - frames.HeaderSize - overhead
	if chunk <= 0 {
		return false, fmt.Errorf("max-frame-size %d too small for transfer", c.maxFrameOut)
	}
	nframes := max(1, (len(o.payload)+chunk-1)/chunk)
	if uint32(nframes) > s.remoteIncomingWindow && s.remoteIncomingWindow < s.maxRemoteWindow {
		return false, nil
	}
	if s.remoteIncomingWindow == 0 {
		return false, nil
	}

	s.nextDeliveryID++
	payload := o.payload
	t := first
	for {
		n := min(chunk, len(payload))
		t.More = n < len(payload)
		if err := s.write(t, payload[:n]); err != nil {
			return false, err
		}
		s.nextOutgoingID++
		if s.remoteIncomingWindow > 0 {
			s.remoteIncomingWindow--
		}
		payload = payload[n:]
		if !t.More {
			break
		}
		t = &frames.Transfer{Handle: l.handle}
	}

	l.credit--
	l.deliveryCount++
	if !settled {
		s.unsettledOut[deliveryID] = o
	}
	o.done <- nil
	return true, nil
}
