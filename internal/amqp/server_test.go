package amqp_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	goamqp "github.com/Azure/go-amqp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
)

// testHandler accepts every link. Messages sent by clients are recorded,
// and links clients receive on are exposed so tests can push messages.
type testHandler struct {
	mu       sync.Mutex
	received [][]byte
	senders  chan *amqp.SenderLink
	refuse   map[string]bool
	outcome  frames.DeliveryState
	// pending receives links attached to "pending/..." addresses, whose
	// attach is left pending.
	pending chan *amqp.SenderLink
}

func newTestHandler() *testHandler {
	return &testHandler{
		senders: make(chan *amqp.SenderLink, 10),
		refuse:  map[string]bool{},
		pending: make(chan *amqp.SenderLink, 10),
	}
}

func (h *testHandler) AttachReceiver(ctx context.Context, l *amqp.ReceiverLink) (amqp.MessageHandler, error) {
	if h.refuse[l.Address()] {
		return nil, amqp.NewError(amqp.CondNotFound, "refused "+l.Address())
	}
	return amqp.MessageHandlerFunc(func(ctx context.Context, d *amqp.Delivery) frames.DeliveryState {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.received = append(h.received, d.Payload)
		return h.outcome
	}), nil
}

func (h *testHandler) AttachSender(ctx context.Context, l *amqp.SenderLink) error {
	if h.refuse[l.Address()] {
		return amqp.NewError(amqp.CondNotFound, "refused "+l.Address())
	}
	if strings.HasPrefix(l.Address(), "pending/") {
		h.pending <- l
		return amqp.ErrAttachPending
	}
	h.senders <- l
	return nil
}

func (h *testHandler) receivedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.received)
}

func startServer(t *testing.T, s *amqp.Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ln) }()
	t.Cleanup(func() {
		s.Close()
		if err := <-done; !errors.Is(err, amqp.ErrServerClosed) {
			t.Errorf("Serve() = %v, want ErrServerClosed", err)
		}
	})
	return "amqp://" + ln.Addr().String()
}

func dial(t *testing.T, addr string, opts *goamqp.ConnOptions) (*goamqp.Conn, *goamqp.Session) {
	t.Helper()
	ctx := testContext(t)
	if opts == nil {
		opts = &goamqp.ConnOptions{SASLType: goamqp.SASLTypeAnonymous()}
	}
	conn, err := goamqp.Dial(ctx, addr, opts)
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		t.Fatalf("NewSession() error: %v", err)
	}
	return conn, sess
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestConnectWithoutSASL(t *testing.T) {
	t.Parallel()
	addr := startServer(t, &amqp.Server{Handler: newTestHandler()})
	dial(t, addr, &goamqp.ConnOptions{})
}

func TestConnectWithSASLPlain(t *testing.T) {
	t.Parallel()
	addr := startServer(t, &amqp.Server{Handler: newTestHandler()})
	dial(t, addr, &goamqp.ConnOptions{SASLType: goamqp.SASLTypePlain("user", "secret")})
}

func TestClientSendsMessages(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	// Small credit and frame size force credit renewal and multi-frame
	// deliveries.
	addr := startServer(t, &amqp.Server{Handler: h, ReceiverCredit: 3, MaxFrameSize: 1024})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	sender, err := sess.NewSender(ctx, "queue", nil)
	if err != nil {
		t.Fatalf("NewSender() error: %v", err)
	}
	big := bytes.Repeat([]byte("x"), 10_000)
	for i := range 10 {
		data := []byte{byte(i)}
		if i == 5 {
			data = big
		}
		if err := sender.Send(ctx, goamqp.NewMessage(data), nil); err != nil {
			t.Fatalf("Send(%d) error: %v", i, err)
		}
	}
	if got := h.receivedCount(); got != 10 {
		t.Fatalf("handler received %d messages, want 10", got)
	}
	m, err := frames.UnmarshalMessage(h.received[5])
	if err != nil {
		t.Fatalf("UnmarshalMessage() error: %v", err)
	}
	if !bytes.Equal(m.Data[0], big) {
		t.Errorf("multi-frame message body corrupted")
	}
}

func TestClientSendRejected(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	h.outcome = &frames.Rejected{Error: amqp.NewError("test:rejected", "no")}
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	sender, err := sess.NewSender(ctx, "queue", nil)
	if err != nil {
		t.Fatalf("NewSender() error: %v", err)
	}
	err = sender.Send(ctx, goamqp.NewMessage([]byte("x")), nil)
	var amqpErr *goamqp.Error
	if !errors.As(err, &amqpErr) || amqpErr.Condition != "test:rejected" {
		t.Errorf("Send() error = %v, want condition test:rejected", err)
	}
}

func TestAttachRefused(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	h.refuse["missing"] = true
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	_, err := sess.NewSender(ctx, "missing", nil)
	var amqpErr *goamqp.Error
	if !errors.As(err, &amqpErr) || amqpErr.Condition != goamqp.ErrCondNotFound {
		t.Errorf("NewSender() error = %v, want amqp:not-found", err)
	}
	_, err = sess.NewReceiver(ctx, "missing", nil)
	if !errors.As(err, &amqpErr) || amqpErr.Condition != goamqp.ErrCondNotFound {
		t.Errorf("NewReceiver() error = %v, want amqp:not-found", err)
	}
	// The session must survive refused links.
	if _, err := sess.NewSender(ctx, "queue", nil); err != nil {
		t.Errorf("NewSender() after refusal error: %v", err)
	}
}

func TestServerSendsMessages(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	conn, sess := dial(t, addr, &goamqp.ConnOptions{SASLType: goamqp.SASLTypeAnonymous(), MaxFrameSize: 512})
	_ = conn
	ctx := testContext(t)

	receiver, err := sess.NewReceiver(ctx, "queue", &goamqp.ReceiverOptions{
		Credit:                    2,
		RequestedSenderSettleMode: goamqp.SenderSettleModeSettled.Ptr(),
	})
	if err != nil {
		t.Fatalf("NewReceiver() error: %v", err)
	}
	link := <-h.senders

	bodies := [][]byte{[]byte("a"), bytes.Repeat([]byte("b"), 5000), []byte("c"), []byte("d")}
	sendErr := make(chan error, 1)
	go func() {
		for _, b := range bodies {
			payload, err := frames.MarshalMessage(&frames.Message{Data: [][]byte{b}})
			if err != nil {
				sendErr <- err
				return
			}
			if err := link.Send(ctx, payload, amqp.SendOptions{}); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()
	for i, want := range bodies {
		msg, err := receiver.Receive(ctx, nil)
		if err != nil {
			t.Fatalf("Receive(%d) error: %v", i, err)
		}
		if !bytes.Equal(msg.GetData(), want) {
			t.Errorf("message %d body has %d bytes, want %d", i, len(msg.GetData()), len(want))
		}
	}
	if err := <-sendErr; err != nil {
		t.Errorf("Send() error: %v", err)
	}
}

func TestServerSendUnsettledReportsOutcome(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	receiver, err := sess.NewReceiver(ctx, "queue", &goamqp.ReceiverOptions{Credit: 1})
	if err != nil {
		t.Fatalf("NewReceiver() error: %v", err)
	}
	link := <-h.senders
	outcomes := make(chan frames.DeliveryState, 1)
	payload, _ := frames.MarshalMessage(&frames.Message{Data: [][]byte{[]byte("x")}})
	if err := link.Send(ctx, payload, amqp.SendOptions{
		OnSettle: func(s frames.DeliveryState, settle func(frames.DeliveryState)) { outcomes <- s; settle(s) },
	}); err != nil {
		t.Fatalf("Send() error: %v", err)
	}
	msg, err := receiver.Receive(ctx, nil)
	if err != nil {
		t.Fatalf("Receive() error: %v", err)
	}
	if err := receiver.RejectMessage(ctx, msg, &goamqp.Error{Condition: "com.microsoft:dead-letter"}); err != nil {
		t.Fatalf("RejectMessage() error: %v", err)
	}
	select {
	case s := <-outcomes:
		r, ok := s.(*frames.Rejected)
		if !ok || r.Error == nil || r.Error.Condition != "com.microsoft:dead-letter" {
			t.Errorf("outcome = %#v, want rejected with com.microsoft:dead-letter", s)
		}
	case <-ctx.Done():
		t.Fatal("no outcome reported")
	}
}

func TestSendBlocksWithoutCredit(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	receiver, err := sess.NewReceiver(ctx, "queue", &goamqp.ReceiverOptions{Credit: -1})
	if err != nil {
		t.Fatalf("NewReceiver() error: %v", err)
	}
	link := <-h.senders
	payload, _ := frames.MarshalMessage(&frames.Message{Data: [][]byte{[]byte("x")}})

	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := link.Send(short, payload, amqp.SendOptions{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() without credit = %v, want DeadlineExceeded", err)
	}

	sendErr := make(chan error, 1)
	go func() { sendErr <- link.Send(ctx, payload, amqp.SendOptions{Settled: true}) }()
	if err := receiver.IssueCredit(1); err != nil {
		t.Fatalf("IssueCredit() error: %v", err)
	}
	if _, err := receiver.Receive(ctx, nil); err != nil {
		t.Fatalf("Receive() error: %v", err)
	}
	if err := <-sendErr; err != nil {
		t.Errorf("Send() after credit error: %v", err)
	}
}

func TestDrainWithNothingToSend(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	receiver, err := sess.NewReceiver(ctx, "queue", &goamqp.ReceiverOptions{Credit: -1})
	if err != nil {
		t.Fatalf("NewReceiver() error: %v", err)
	}
	<-h.senders
	if err := receiver.IssueCredit(5); err != nil {
		t.Fatalf("IssueCredit() error: %v", err)
	}
	drainCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := receiver.DrainCredit(drainCtx, nil); err != nil {
		t.Errorf("DrainCredit() error: %v", err)
	}
}

func TestDetachAndReattach(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	for i := range 3 {
		sender, err := sess.NewSender(ctx, "queue", nil)
		if err != nil {
			t.Fatalf("NewSender(%d) error: %v", i, err)
		}
		if err := sender.Send(ctx, goamqp.NewMessage([]byte("x")), nil); err != nil {
			t.Fatalf("Send(%d) error: %v", i, err)
		}
		if err := sender.Close(ctx); err != nil {
			t.Fatalf("Close(%d) error: %v", i, err)
		}
	}
	if err := sess.Close(ctx); err != nil {
		t.Errorf("session Close() error: %v", err)
	}
}

func TestServerCloseDisconnectsClients(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	s := &amqp.Server{Handler: h}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error: %v", err)
	}
	go s.Serve(ln)
	conn, sess := dial(t, "amqp://"+ln.Addr().String(), nil)
	_ = conn
	ctx := testContext(t)
	receiver, err := sess.NewReceiver(ctx, "queue", nil)
	if err != nil {
		t.Fatalf("NewReceiver() error: %v", err)
	}
	<-h.senders

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	_, err = receiver.Receive(ctx, nil)
	var connErr *goamqp.ConnError
	if !errors.As(err, &connErr) {
		t.Errorf("Receive() after server close = %v, want *amqp.ConnError", err)
	}
}

func TestHeartbeatsKeepIdleConnectionAlive(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, &goamqp.ConnOptions{
		SASLType:    goamqp.SASLTypeAnonymous(),
		IdleTimeout: 200 * time.Millisecond,
	})
	time.Sleep(600 * time.Millisecond)
	ctx := testContext(t)
	sender, err := sess.NewSender(ctx, "queue", nil)
	if err != nil {
		t.Fatalf("NewSender() after idle period error: %v", err)
	}
	if err := sender.Send(ctx, goamqp.NewMessage([]byte("x")), nil); err != nil {
		t.Errorf("Send() after idle period error: %v", err)
	}
}

// TestServerSendReceiverSettleModeSecond covers the exchange peek-lock
// receivers use: the receiver reports an unsettled outcome and waits for
// the sender to settle.
func TestServerSendReceiverSettleModeSecond(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	receiver, err := sess.NewReceiver(ctx, "queue", &goamqp.ReceiverOptions{
		Credit:         1,
		SettlementMode: goamqp.ReceiverSettleModeSecond.Ptr(),
	})
	if err != nil {
		t.Fatalf("NewReceiver() error: %v", err)
	}
	link := <-h.senders
	outcomes := make(chan frames.DeliveryState, 1)
	payload, _ := frames.MarshalMessage(&frames.Message{Data: [][]byte{[]byte("x")}})
	if err := link.Send(ctx, payload, amqp.SendOptions{
		OnSettle: func(s frames.DeliveryState, settle func(frames.DeliveryState)) { outcomes <- s; settle(s) },
	}); err != nil {
		t.Fatalf("Send() error: %v", err)
	}
	msg, err := receiver.Receive(ctx, nil)
	if err != nil {
		t.Fatalf("Receive() error: %v", err)
	}
	// AcceptMessage returns only after the server settles the delivery.
	if err := receiver.AcceptMessage(ctx, msg); err != nil {
		t.Fatalf("AcceptMessage() error: %v", err)
	}
	select {
	case s := <-outcomes:
		if _, ok := s.(*frames.Accepted); !ok {
			t.Errorf("outcome = %#v, want accepted", s)
		}
	case <-ctx.Done():
		t.Fatal("no outcome reported")
	}
}

// TestDeferredAttach leaves an attach pending while other links of the
// same session keep working, then accepts it with a modified source.
func TestDeferredAttach(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	type result struct {
		r   *goamqp.Receiver
		err error
	}
	attached := make(chan result, 1)
	go func() {
		r, err := sess.NewReceiver(ctx, "pending/a", &goamqp.ReceiverOptions{
			Filters: []goamqp.LinkFilter{goamqp.NewLinkFilter("com.microsoft:session-filter", 0x00000137000000C, nil)},
		})
		attached <- result{r, err}
	}()
	link := <-h.pending

	// The session keeps serving other links meanwhile.
	sender, err := sess.NewSender(ctx, "queue", nil)
	if err != nil {
		t.Fatalf("NewSender() during a pending attach error: %v", err)
	}
	if err := sender.Send(ctx, goamqp.NewMessage([]byte("x")), nil); err != nil {
		t.Fatalf("Send() during a pending attach error: %v", err)
	}
	select {
	case r := <-attached:
		t.Fatalf("NewReceiver() returned (%v) before the attach completed", r.err)
	default:
	}

	// Credit granted before completion is honored afterwards.
	payload, _ := frames.MarshalMessage(&frames.Message{Data: [][]byte{[]byte("hello")}})
	sendErr := make(chan error, 1)
	go func() { sendErr <- link.Send(ctx, payload, amqp.SendOptions{Settled: true}) }()

	// Filter values are described types; the client reads the value back.
	link.Source().Filter.Set(encoding.Symbol("com.microsoft:session-filter"),
		encoding.Described{Descriptor: uint64(0x00000137000000C), Value: "chosen"})
	if err := link.CompleteAttach(nil); err != nil {
		t.Fatalf("CompleteAttach() error: %v", err)
	}
	res := <-attached
	if res.err != nil {
		t.Fatalf("NewReceiver() error: %v", res.err)
	}
	if got := res.r.LinkSourceFilterValue("com.microsoft:session-filter"); got != "chosen" {
		t.Errorf("LinkSourceFilterValue() = %v, want chosen", got)
	}
	msg, err := res.r.Receive(ctx, nil)
	if err != nil {
		t.Fatalf("Receive() error: %v", err)
	}
	if string(msg.GetData()) != "hello" {
		t.Errorf("Receive() body = %q, want hello", msg.GetData())
	}
	if err := <-sendErr; err != nil {
		t.Errorf("Send() error: %v", err)
	}
}

func TestDeferredAttachRefused(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	errc := make(chan error, 1)
	go func() {
		_, err := sess.NewReceiver(ctx, "pending/b", nil)
		errc <- err
	}()
	link := <-h.pending
	if err := link.CompleteAttach(amqp.NewError("com.microsoft:timeout", "no session available")); err != nil {
		t.Fatalf("CompleteAttach(error) error: %v", err)
	}
	var amqpErr *goamqp.Error
	if err := <-errc; !errors.As(err, &amqpErr) || amqpErr.Condition != "com.microsoft:timeout" {
		t.Errorf("NewReceiver() error = %v, want com.microsoft:timeout", err)
	}
	// The session survives.
	if _, err := sess.NewSender(ctx, "queue", nil); err != nil {
		t.Errorf("NewSender() after refusal error: %v", err)
	}
}

// TestDeferredAttachClientGivesUp covers a client whose attach times out:
// it abandons the link and detaches once the server finally answers.
func TestDeferredAttachClientGivesUp(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := sess.NewReceiver(short, "pending/c", nil); err == nil {
		t.Fatal("NewReceiver() with a pending attach succeeded before completion")
	}
	link := <-h.pending
	link.CompleteAttach(nil) // the client already gave up
	// New links clean up the abandoned one and keep working.
	sender, err := sess.NewSender(ctx, "queue", nil)
	if err != nil {
		t.Fatalf("NewSender() after an abandoned attach error: %v", err)
	}
	if err := sender.Send(ctx, goamqp.NewMessage([]byte("x")), nil); err != nil {
		t.Errorf("Send() after an abandoned attach error: %v", err)
	}
}

func TestServerDetach(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	addr := startServer(t, &amqp.Server{Handler: h})
	_, sess := dial(t, addr, nil)
	ctx := testContext(t)

	receiver, err := sess.NewReceiver(ctx, "queue", nil)
	if err != nil {
		t.Fatalf("NewReceiver() error: %v", err)
	}
	link := <-h.senders
	if err := link.Detach(amqp.NewError(amqp.CondNotFound, "entity deleted")); err != nil {
		t.Fatalf("Detach() error: %v", err)
	}
	_, err = receiver.Receive(ctx, nil)
	var linkErr *goamqp.LinkError
	if !errors.As(err, &linkErr) || linkErr.RemoteErr == nil || linkErr.RemoteErr.Condition != goamqp.ErrCondNotFound {
		t.Errorf("Receive() after server detach = %v, want link error amqp:not-found", err)
	}
	if _, err := sess.NewReceiver(ctx, "queue", nil); err != nil {
		t.Errorf("NewReceiver() after server detach error: %v", err)
	}
}
