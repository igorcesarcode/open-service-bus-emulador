// Package amqp implements the server side of the AMQP 1.0 protocol:
// SASL negotiation, connections, sessions, links, flow control and
// settlement. Message semantics are delegated to a Handler.
package amqp

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Defaults for Server fields left at their zero value.
const (
	DefaultMaxFrameSize   = 64 * 1024
	DefaultReceiverCredit = 500
	DefaultSessionWindow  = 5000
	handshakeTimeout      = 30 * time.Second
)

// Server accepts AMQP 1.0 connections and routes their links to a Handler.
type Server struct {
	// Handler decides how attached links behave. It must be set.
	Handler Handler
	// ContainerID identifies this server in the open frame.
	ContainerID string
	// OnConnect and OnDisconnect, if set, are called when a connection has
	// opened, and when an opened connection ends. The context is the
	// connection's: ConnHostname and ConnData work on it.
	OnConnect, OnDisconnect func(ctx context.Context)
	// MaxFrameSize is the largest frame accepted from peers.
	MaxFrameSize uint32
	// MaxMessageSize is advertised on attach and enforced on incoming
	// deliveries. Zero means no limit.
	MaxMessageSize uint64
	// IdleTimeout, if positive, is advertised to peers, which must then
	// send a frame at least this often.
	IdleTimeout time.Duration
	// ReceiverCredit is the link credit granted to peers that send messages.
	ReceiverCredit uint32
	// Logger receives connection diagnostics. Nil disables logging.
	Logger *slog.Logger

	mu        sync.Mutex
	listeners map[net.Listener]struct{}
	conns     map[*conn]struct{}
	closed    bool
	wg        sync.WaitGroup
}

func (s *Server) logger() *slog.Logger {
	if s.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.Logger
}

func (s *Server) maxFrameSize() uint32 {
	if s.MaxFrameSize == 0 {
		return DefaultMaxFrameSize
	}
	return s.MaxFrameSize
}

func (s *Server) receiverCredit() uint32 {
	if s.ReceiverCredit == 0 {
		return DefaultReceiverCredit
	}
	return s.ReceiverCredit
}

// Serve accepts connections on l until Close is called. It always returns
// a non-nil error; after Close it returns ErrServerClosed.
func (s *Server) Serve(l net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrServerClosed
	}
	if s.listeners == nil {
		s.listeners = make(map[net.Listener]struct{})
	}
	s.listeners[l] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.listeners, l)
		s.mu.Unlock()
	}()

	for {
		nc, err := l.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return ErrServerClosed
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return err
		}
		s.wg.Go(func() { s.ServeConn(nc) })
	}
}

// ServeConn serves a single connection and returns when it closes. The
// connection is closed on return.
func (s *Server) ServeConn(nc net.Conn) {
	c := newConn(s, nc)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		nc.Close()
		return
	}
	if s.conns == nil {
		s.conns = make(map[*conn]struct{})
	}
	s.conns[c] = struct{}{}
	s.mu.Unlock()

	c.serve()

	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// Close stops all listeners, closes every connection with
// amqp:connection:forced and waits for connection goroutines to finish.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	for l := range s.listeners {
		l.Close()
	}
	conns := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, c := range conns {
		c.shutdown(NewError(CondConnectionForced, "server shutting down"))
	}
	s.wg.Wait()
	return nil
}

// Shutdown is like Close but gives up waiting when ctx is done.
func (s *Server) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.Close()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
