package amqp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
)

// conn is one AMQP connection. All protocol state is owned by the frame
// loop goroutine (run); other goroutines interact with it through post.
type conn struct {
	server *Server
	nc     net.Conn
	log    *slog.Logger
	br     *bufio.Reader

	ctx    context.Context
	cancel context.CancelFunc

	// Negotiated during open.
	remoteOpen   *frames.Open
	maxFrameIn   uint32
	maxFrameOut  uint32
	remoteIdle   time.Duration
	channelMax   uint16
	saslIdentity string

	sessionsIn  map[uint16]*Session
	sessionsOut map[uint16]*Session

	reqs     chan func()
	done     chan struct{}
	stopOnce sync.Once
	stopErr  *frames.Error
	stopMu   sync.Mutex
	stopCh   chan struct{}

	writeMu   sync.Mutex
	lastWrite atomic.Int64
}

func newConn(s *Server, nc net.Conn) *conn {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), connDataKey{}, &sync.Map{}))
	return &conn{
		server:      s,
		nc:          nc,
		log:         s.logger().With("remote", nc.RemoteAddr().String()),
		br:          bufio.NewReaderSize(nc, 64*1024),
		ctx:         ctx,
		cancel:      cancel,
		sessionsIn:  make(map[uint16]*Session),
		sessionsOut: make(map[uint16]*Session),
		reqs:        make(chan func()),
		done:        make(chan struct{}),
		stopCh:      make(chan struct{}),
	}
}

// shutdown asks the frame loop to close the connection with err.
func (c *conn) shutdown(err *frames.Error) {
	c.stopOnce.Do(func() {
		c.stopMu.Lock()
		c.stopErr = err
		c.stopMu.Unlock()
		close(c.stopCh)
	})
}

// post runs fn on the frame loop. It reports false if the connection has
// already finished.
func (c *conn) post(fn func()) bool {
	select {
	case c.reqs <- fn:
		return true
	case <-c.done:
		return false
	}
}

func (c *conn) serve() {
	defer c.nc.Close()
	defer c.cancel()
	defer close(c.done)

	// A peer that connects but never completes the handshake must not hold
	// up shutdown, so unblock its reads when shutdown is requested.
	handshakeDone := make(chan struct{})
	go func() {
		select {
		case <-c.stopCh:
			c.nc.SetDeadline(time.Now())
		case <-handshakeDone:
		}
	}()
	err := c.handshake()
	close(handshakeDone)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			c.log.Debug("amqp handshake failed", "err", err)
		}
		return
	}
	c.log.Debug("amqp connection open", "container", c.remoteOpen.ContainerID, "hostname", c.remoteOpen.Hostname)
	if c.server.OnConnect != nil {
		c.server.OnConnect(c.ctx)
	}
	if c.server.OnDisconnect != nil {
		defer c.server.OnDisconnect(c.ctx)
	}

	incoming := make(chan frames.Frame)
	readErr := make(chan error, 1)
	go c.readLoop(incoming, readErr)

	stopHeartbeat := c.startHeartbeat()
	defer stopHeartbeat()

	err = c.run(incoming, readErr)
	c.teardown()
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		c.log.Debug("amqp connection closed", "err", err)
	} else {
		c.log.Debug("amqp connection closed by the peer", "container", c.remoteOpen.ContainerID)
	}
}

// handshake performs protocol header exchange, optional SASL and open.
func (c *conn) handshake() error {
	c.nc.SetDeadline(time.Now().Add(handshakeTimeout))
	defer c.nc.SetDeadline(time.Time{})

	hdr, err := c.readProtocolHeader()
	if err != nil {
		return err
	}
	if hdr == frames.ProtocolHeaderSASL {
		if err := c.negotiateSASL(); err != nil {
			return err
		}
		if hdr, err = c.readProtocolHeader(); err != nil {
			return err
		}
	}
	if hdr != frames.ProtocolHeaderAMQP {
		// Reply with the header we support, then close, as the spec requires.
		c.write(frames.ProtocolHeaderAMQP[:])
		return fmt.Errorf("unsupported protocol header % x", hdr)
	}
	if err := c.write(frames.ProtocolHeaderAMQP[:]); err != nil {
		return err
	}

	f, err := frames.ReadFrame(c.br, c.server.maxFrameSize())
	if err != nil {
		return err
	}
	open, ok := f.Body.(*frames.Open)
	if !ok || f.Type != frames.TypeAMQP {
		return fmt.Errorf("first frame is %T, want open", f.Body)
	}
	c.remoteOpen = open
	if data := ConnData(c.ctx); data != nil {
		data.Store(hostnameKey{}, open.Hostname)
	}
	c.maxFrameIn = c.server.maxFrameSize()
	c.maxFrameOut = 1<<32 - 1
	if open.MaxFrameSize != nil {
		c.maxFrameOut = max(*open.MaxFrameSize, frames.MinMaxFrameSize)
	}
	c.channelMax = 1<<16 - 1
	if open.ChannelMax != nil {
		c.channelMax = *open.ChannelMax
	}
	c.remoteIdle = open.IdleTimeout

	maxFrame := c.maxFrameIn
	channelMax := c.channelMax
	return c.writeFrame(frames.TypeAMQP, 0, &frames.Open{
		ContainerID:  c.server.containerID(),
		MaxFrameSize: &maxFrame,
		ChannelMax:   &channelMax,
		IdleTimeout:  c.server.IdleTimeout,
	}, nil)
}

func (s *Server) containerID() string {
	if s.ContainerID == "" {
		return "open-service-bus"
	}
	return s.ContainerID
}

func (c *conn) readProtocolHeader() ([8]byte, error) {
	var hdr [8]byte
	_, err := io.ReadFull(c.br, hdr[:])
	return hdr, err
}

// negotiateSASL accepts the ANONYMOUS, PLAIN and MSSBCBS mechanisms.
// Credentials are not checked here: Service Bus clients authenticate through
// CBS, and MSSBCBS, the mechanism the .NET SDK insists on, only says so.
func (c *conn) negotiateSASL() error {
	if err := c.write(frames.ProtocolHeaderSASL[:]); err != nil {
		return err
	}
	mechs := &frames.SASLMechanisms{Mechanisms: []encoding.Symbol{"ANONYMOUS", "PLAIN", "MSSBCBS"}}
	if err := c.writeFrame(frames.TypeSASL, 0, mechs, nil); err != nil {
		return err
	}
	f, err := frames.ReadFrame(c.br, frames.MinMaxFrameSize*8)
	if err != nil {
		return err
	}
	init, ok := f.Body.(*frames.SASLInit)
	if !ok {
		return fmt.Errorf("expected sasl-init, got %T", f.Body)
	}
	code := frames.SASLCodeOK
	switch init.Mechanism {
	case "ANONYMOUS", "MSSBCBS":
	case "PLAIN":
		// The initial response is "authzid\x00authcid\x00password".
		if parts := splitNUL(init.InitialResponse); len(parts) == 3 {
			c.saslIdentity = string(parts[1])
		}
	default:
		code = frames.SASLCodeAuth
	}
	if err := c.writeFrame(frames.TypeSASL, 0, &frames.SASLOutcome{Code: code}, nil); err != nil {
		return err
	}
	if code != frames.SASLCodeOK {
		return fmt.Errorf("unsupported SASL mechanism %q", init.Mechanism)
	}
	return nil
}

func splitNUL(b []byte) [][]byte {
	var parts [][]byte
	start := 0
	for i, x := range b {
		if x == 0 {
			parts = append(parts, b[start:i])
			start = i + 1
		}
	}
	return append(parts, b[start:])
}

func (c *conn) readLoop(out chan<- frames.Frame, errc chan<- error) {
	for {
		if idle := c.server.IdleTimeout; idle > 0 {
			c.nc.SetReadDeadline(time.Now().Add(2 * idle))
		}
		f, err := frames.ReadFrame(c.br, c.maxFrameIn)
		if err != nil {
			errc <- err
			return
		}
		if f.Body == nil {
			continue // heartbeat
		}
		select {
		case out <- f:
		case <-c.done:
			return
		}
	}
}

// startHeartbeat sends empty frames often enough to satisfy the peer's
// idle timeout. It returns a function that stops the heartbeat.
func (c *conn) startHeartbeat() func() {
	if c.remoteIdle <= 0 {
		return func() {}
	}
	interval := c.remoteIdle / 2
	t := time.NewTicker(interval)
	stop := make(chan struct{})
	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if time.Since(time.Unix(0, c.lastWrite.Load())) >= interval {
					if err := c.writeFrame(frames.TypeAMQP, 0, nil, nil); err != nil {
						return
					}
				}
			case <-stop:
				return
			}
		}
	}()
	return func() { close(stop) }
}

// run is the frame loop. It returns when the connection must close.
func (c *conn) run(incoming <-chan frames.Frame, readErr <-chan error) error {
	for {
		select {
		case f := <-incoming:
			if err := c.handleFrame(f); err != nil {
				var closeReq *closeRequested
				if errors.As(err, &closeReq) {
					return nil
				}
				c.closeWithError(asConnError(err))
				return err
			}
		case fn := <-c.reqs:
			fn()
		case err := <-readErr:
			if errors.Is(err, frames.ErrFrameTooLarge) {
				c.closeWithError(NewError(CondFramingError, err.Error()))
			}
			return err
		case <-c.stopCh:
			c.stopMu.Lock()
			e := c.stopErr
			c.stopMu.Unlock()
			c.closeWithError(e)
			return nil
		}
	}
}

// closeRequested signals that the peer closed the connection cleanly.
type closeRequested struct{}

func (*closeRequested) Error() string { return "peer closed connection" }

// connError is an error that closes the whole connection.
type connError struct{ err *frames.Error }

func (e *connError) Error() string { return e.err.Error() }

func asConnError(err error) *frames.Error {
	var ce *connError
	if errors.As(err, &ce) {
		return ce.err
	}
	var fe *frames.Error
	if errors.As(err, &fe) {
		return fe
	}
	return NewError(CondInternalError, err.Error())
}

func (c *conn) closeWithError(e *frames.Error) {
	if err := c.writeFrame(frames.TypeAMQP, 0, &frames.Close{Error: e}, nil); err != nil {
		c.log.Debug("writing close", "err", err)
	}
}

// teardown releases every session and link after the frame loop exits.
func (c *conn) teardown() {
	for _, s := range c.sessionsIn {
		s.teardown()
	}
	c.sessionsIn = nil
	c.sessionsOut = nil
}

func (c *conn) handleFrame(f frames.Frame) error {
	if c.log.Enabled(c.ctx, slog.LevelDebug-4) {
		c.log.Log(c.ctx, slog.LevelDebug-4, "amqp recv", "channel", f.Channel, "frame", fmt.Sprintf("%+v", f.Body))
	}
	if f.Type != frames.TypeAMQP {
		return &connError{NewError(CondFramingError, "unexpected SASL frame after open")}
	}
	switch b := f.Body.(type) {
	case *frames.Begin:
		return c.handleBegin(f.Channel, b)
	case *frames.Close:
		if b.Error != nil {
			c.log.Debug("peer closed connection with error", "err", b.Error)
		}
		c.writeFrame(frames.TypeAMQP, 0, &frames.Close{}, nil)
		return &closeRequested{}
	case *frames.Open:
		return &connError{NewError(CondIllegalState, "duplicate open")}
	}
	s, ok := c.sessionsIn[f.Channel]
	if !ok {
		return &connError{NewError(CondIllegalState, fmt.Sprintf("frame %T on unattached channel %d", f.Body, f.Channel))}
	}
	if err := s.handleFrame(f.Body); err != nil {
		var se *sessionError
		if errors.As(err, &se) {
			return c.endSession(s, se.err)
		}
		return err
	}
	return nil
}

func (c *conn) handleBegin(channel uint16, b *frames.Begin) error {
	if b.RemoteChannel != nil {
		return &connError{NewError(CondNotAllowed, "server does not initiate sessions")}
	}
	if _, ok := c.sessionsIn[channel]; ok {
		return &connError{NewError(CondIllegalState, fmt.Sprintf("channel %d already in use", channel))}
	}
	out, ok := c.freeOutChannel()
	if !ok {
		return &connError{NewError(CondResourceLimitExceeded, "no free channels")}
	}
	s := newSession(c, channel, out, b)
	c.sessionsIn[channel] = s
	c.sessionsOut[out] = s

	remote := channel
	handleMax := uint32(1<<32 - 1)
	return c.writeFrame(frames.TypeAMQP, out, &frames.Begin{
		RemoteChannel:  &remote,
		NextOutgoingID: s.nextOutgoingID,
		IncomingWindow: s.incomingWindow,
		OutgoingWindow: DefaultSessionWindow,
		HandleMax:      &handleMax,
	}, nil)
}

func (c *conn) freeOutChannel() (uint16, bool) {
	for ch := uint32(0); ch <= uint32(c.channelMax); ch++ {
		if _, used := c.sessionsOut[uint16(ch)]; !used {
			return uint16(ch), true
		}
	}
	return 0, false
}

// endSession ends s locally, reporting err to the peer.
func (c *conn) endSession(s *Session, err *frames.Error) error {
	s.teardown()
	delete(c.sessionsIn, s.inChannel)
	delete(c.sessionsOut, s.outChannel)
	return c.writeFrame(frames.TypeAMQP, s.outChannel, &frames.End{Error: err}, nil)
}

func (c *conn) write(b []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.nc.Write(b)
	c.lastWrite.Store(time.Now().UnixNano())
	return err
}

// writeFrame encodes and writes one frame. A nil body writes an empty frame.
func (c *conn) writeFrame(typ uint8, channel uint16, body frames.Body, payload []byte) error {
	buf, err := frames.AppendFrame(nil, typ, channel, body, payload)
	if err != nil {
		return err
	}
	if c.log.Enabled(c.ctx, slog.LevelDebug-4) && body != nil {
		c.log.Log(c.ctx, slog.LevelDebug-4, "amqp send", "channel", channel, "frame", fmt.Sprintf("%+v", body))
	}
	return c.write(buf)
}

type connDataKey struct{}

// ConnData returns a map that lives as long as the connection a link
// context belongs to, for handlers that keep per-connection state such as
// the claims of CBS tokens. It is nil for a context of another origin.
func ConnData(ctx context.Context) *sync.Map {
	m, _ := ctx.Value(connDataKey{}).(*sync.Map)
	return m
}

type hostnameKey struct{}

// ConnHostname returns the hostname the peer of the connection put in its
// open frame, which names the namespace it wants. It is "" before the open
// and for a context of another origin.
func ConnHostname(ctx context.Context) string {
	if data := ConnData(ctx); data != nil {
		if h, ok := data.Load(hostnameKey{}); ok {
			return h.(string)
		}
	}
	return ""
}
