package integration_test

import (
	"bufio"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func txnState(id []byte, outcome frames.DeliveryState) *frames.TransactionalState {
	return &frames.TransactionalState{TxnID: id, Outcome: outcome}
}

func wantAccepted(t *testing.T, what string, st frames.DeliveryState, id []byte) {
	t.Helper()
	ts, ok := st.(*frames.TransactionalState)
	if !ok {
		t.Fatalf("%s: state %T %+v, want transactional-state", what, st, st)
	}
	if _, ok := ts.Outcome.(*frames.Accepted); !ok {
		t.Fatalf("%s: outcome %T %+v, want accepted", what, ts.Outcome, ts.Outcome)
	}
}

func wantRejected(t *testing.T, what string, st frames.DeliveryState, cond encoding.Symbol) {
	t.Helper()
	r, ok := st.(*frames.Rejected)
	if !ok || r.Error == nil {
		t.Fatalf("%s: state %T %+v, want rejected", what, st, st)
	}
	if cond != "" && r.Error.Condition != cond {
		t.Errorf("%s: condition %q (%s), want %q", what, r.Error.Condition, r.Error.Description, cond)
	}
}

func peekCount(t *testing.T, client *azservicebus.Client, queue string, opts *azservicebus.ReceiverOptions) int {
	t.Helper()
	r := newReceiver(t, client, queue, opts)
	msgs, err := r.PeekMessages(testContext(t), 100, nil)
	if err != nil {
		t.Fatalf("PeekMessages(%s) error: %v", queue, err)
	}
	return len(msgs)
}

func TestTransactionSendCommitAndRollback(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	c := dialRaw(t, srv)
	coord := c.attachCoordinator()
	sender := c.attachSender("s1", "orders", nil)

	// Committed: the messages appear together, only at the discharge.
	id := c.declare(coord)
	wantAccepted(t, "send 1", c.transfer(sender, rawMessage(t, "a", ""), txnState(id, nil)), id)
	wantAccepted(t, "send 2", c.transfer(sender, rawMessage(t, "b", ""), txnState(id, nil)), id)
	if n := peekCount(t, client, "orders", nil); n != 0 {
		t.Fatalf("messages visible before the commit = %d, want 0", n)
	}
	if st := c.discharge(coord, id, false); st == nil {
		t.Fatal("no outcome for the commit")
	} else if _, ok := st.(*frames.Accepted); !ok {
		t.Fatalf("commit outcome = %T %+v, want accepted", st, st)
	}
	r := newReceiver(t, client, "orders", &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	if diff := cmp.Diff([]string{"a", "b"}, sortedBodies(receive(t, r, 2))); diff != "" {
		t.Errorf("committed messages mismatch (-want +got):\n%s", diff)
	}

	// Rolled back: nothing appears.
	id = c.declare(coord)
	wantAccepted(t, "send 3", c.transfer(sender, rawMessage(t, "never", ""), txnState(id, nil)), id)
	if st := c.discharge(coord, id, true); st == nil {
		t.Fatal("no outcome for the rollback")
	} else if _, ok := st.(*frames.Accepted); !ok {
		t.Fatalf("rollback outcome = %T %+v, want accepted", st, st)
	}
	if n := peekCount(t, client, "orders", nil); n != 0 {
		t.Errorf("messages after the rollback = %d, want 0", n)
	}

	// A discharged transaction is gone.
	wantRejected(t, "discharge twice", c.discharge(coord, id, false), "amqp:transaction:unknown-id")
	wantRejected(t, "send in a discharged transaction", c.transfer(sender, rawMessage(t, "late", ""), txnState(id, nil)), "amqp:transaction:unknown-id")

	// A send outside any transaction still works on the same link.
	if _, ok := c.transfer(sender, rawMessage(t, "plain", ""), nil).(*frames.Accepted); !ok {
		t.Error("a plain send on the same link was not accepted")
	}
}

func TestTransactionSettleCommitAndRollback(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{queue("work", func(p *broker.Properties) { p.LockDuration = time.Minute })}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "work"), bodies("m1", "m2", "m3")...)
	ctx := testContext(t)

	c := dialRaw(t, srv)
	coord := c.attachCoordinator()
	// Credit for exactly the three messages, so that redelivered ones go to
	// the SDK's receiver and not back to this one.
	c.attachReceiver("r1", "work", 3)
	m1, m2, m3 := c.receive(), c.receive(), c.receive()

	// Complete m1, abandon m2 and dead-letter m3 in one transaction.
	id := c.declare(coord)
	wantSettled := func(name string, st frames.DeliveryState) {
		t.Helper()
		if _, ok := st.(*frames.TransactionalState); !ok {
			t.Fatalf("%s: answer %T %+v, want transactional-state", name, st, st)
		}
	}
	wantSettled("m1", c.dispose(m1.id, txnState(id, &frames.Accepted{})))
	wantSettled("m2", c.dispose(m2.id, txnState(id, &frames.Modified{DeliveryFailed: true})))
	wantSettled("m3", c.dispose(m3.id, txnState(id, &frames.Rejected{Error: &frames.Error{
		Condition: "com.microsoft:dead-letter",
		Info:      encoding.Map{{Key: encoding.Symbol("DeadLetterReason"), Value: "Poison"}},
	}})))
	// Nothing changed yet: all three are still locked and counted.
	if n := peekCount(t, client, "work", nil); n != 3 {
		t.Fatalf("messages before the commit = %d, want 3", n)
	}
	if st := c.discharge(coord, id, false); st == nil {
		t.Fatal("no outcome")
	} else if _, ok := st.(*frames.Accepted); !ok {
		t.Fatalf("commit = %T %+v", st, st)
	}
	if n := peekCount(t, client, "work", nil); n != 1 {
		t.Errorf("messages after the commit = %d, want only the abandoned one", n)
	}
	if n := peekCount(t, client, "work", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter}); n != 1 {
		t.Errorf("dead-lettered after the commit = %d, want 1", n)
	}
	// The abandoned message can be received again, with its delivery count up.
	again := receive(t, newReceiver(t, client, "work", nil), 1)
	if string(again[0].Body) != "m2" || again[0].DeliveryCount != 2 {
		t.Errorf("redelivered = %q count %d, want m2 and 2", again[0].Body, again[0].DeliveryCount)
	}
	_ = ctx

	// A rolled back settlement leaves the message locked, then it frees up
	// when the lock lapses, here by releasing the receiver's connection.
	send(t, newSender(t, client, "work"), bodies("m4")...)
	// Only m4 is ready: m2 is locked by the SDK receiver.
	c.grantCredit(1)
	m4 := c.receive()
	id = c.declare(coord)
	wantSettled("m4", c.dispose(m4.id, txnState(id, &frames.Accepted{})))
	c.discharge(coord, id, true)
	if n := peekCount(t, client, "work", nil); n != 2 { // m2 (locked by the SDK) and m4
		t.Errorf("messages after the rollback = %d, want m2 and m4 still there", n)
	}
}

func TestTransactionSendVia(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("input", "output")})
	client := newClient(t, srv)
	send(t, newSender(t, client, "input"),
		&azservicebus.Message{Body: []byte("order-1"), Subject: strPtr("order.created")},
		&azservicebus.Message{Body: []byte("order-2"), Subject: strPtr("order.created")})
	c := dialRaw(t, srv)
	coord := c.attachCoordinator()
	c.attachReceiver("r1", "input", 2)
	// The send-via link goes through the entity the transaction receives
	// from and is meant for the destination in its properties.
	via := c.attachSender("via", "input", encoding.Map{{Key: encoding.Symbol("com.microsoft:transfer-destination-address"), Value: "output"}})
	// A settled delivery stays settled, so each transaction below takes a
	// message of its own.
	in, in2 := c.receive(), c.receive()

	// Rolled back first: input keeps the message locked, output stays empty.
	id := c.declare(coord)
	wantAccepted(t, "via send", c.transfer(via, rawMessage(t, "shipped", "order.shipped"), txnState(id, nil)), id)
	c.dispose(in.id, txnState(id, &frames.Accepted{}))
	if n := peekCount(t, client, "output", nil); n != 0 {
		t.Fatalf("output before the commit = %d", n)
	}
	c.discharge(coord, id, true)
	if n := peekCount(t, client, "output", nil); n != 0 {
		t.Errorf("output after the rollback = %d, want 0", n)
	}
	if n := peekCount(t, client, "input", nil); n != 2 {
		t.Errorf("input after the rollback = %d, want 2", n)
	}

	// Committed: the input message is consumed and the output appears, both
	// or neither.
	id = c.declare(coord)
	wantAccepted(t, "via send", c.transfer(via, rawMessage(t, "shipped", "order.shipped"), txnState(id, nil)), id)
	c.dispose(in2.id, txnState(id, &frames.Accepted{}))
	if _, ok := c.discharge(coord, id, false).(*frames.Accepted); !ok {
		t.Fatal("commit not accepted")
	}
	if n := peekCount(t, client, "input", nil); n != 1 {
		t.Errorf("input after the commit = %d, want 1: the message of the rolled back transaction", n)
	}
	got := receive(t, newReceiver(t, client, "output", nil), 1)
	if string(got[0].Body) != "shipped" || str(got[0].Subject) != "order.shipped" {
		t.Errorf("output = %q %q", got[0].Body, str(got[0].Subject))
	}
}

func TestTransactionCrossEntityRules(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("a", "b", "c")})
	client := newClient(t, srv)
	send(t, newSender(t, client, "a"), bodies("from-a", "from-a2")...)
	send(t, newSender(t, client, "c"), bodies("from-c")...)

	c := dialRaw(t, srv)
	coord := c.attachCoordinator()
	toB := c.attachSender("to-b", "b", nil)
	toC := c.attachSender("to-c", "c", nil)
	c.attachReceiver("ra", "a", 2)
	c.attachReceiver("rc", "c", 1)
	byBody := map[string]received{}
	for i := 0; i < 3; i++ {
		m := c.receive()
		byBody[m.body] = m
	}
	ma, ma2, mc := byBody["from-a"], byBody["from-a2"], byBody["from-c"]

	// Sends to two entities with no receive: not allowed.
	id := c.declare(coord)
	wantAccepted(t, "send to b", c.transfer(toB, rawMessage(t, "x", ""), txnState(id, nil)), id)
	wantRejected(t, "send to c in the same transaction", c.transfer(toC, rawMessage(t, "y", ""), txnState(id, nil)), "amqp:not-allowed")
	// A settlement on an entity other than the one the sends were bound to.
	wantRejected(t, "settle on a", c.dispose(ma.id, txnState(id, &frames.Accepted{})), "amqp:not-allowed")
	c.discharge(coord, id, true)

	// After a receive on a, a send to b goes via a, and a settlement on c
	// is not allowed.
	id = c.declare(coord)
	if _, ok := c.dispose(ma2.id, txnState(id, &frames.Accepted{})).(*frames.TransactionalState); !ok {
		t.Fatal("settle on a was refused")
	}
	wantAccepted(t, "send to b after a receive on a", c.transfer(toB, rawMessage(t, "relay", ""), txnState(id, nil)), id)
	wantRejected(t, "settle on c", c.dispose(mc.id, txnState(id, &frames.Accepted{})), "amqp:not-allowed")
	if _, ok := c.discharge(coord, id, false).(*frames.Accepted); !ok {
		t.Fatal("commit not accepted")
	}
	if got := bodiesOf(receive(t, newReceiver(t, client, "b", nil), 1)); !cmp.Equal(got, []string{"relay"}) {
		t.Errorf("b = %v", got)
	}
}

func TestTransactionViaDisabledDestinationGoesToTransferDeadLetter(t *testing.T) {
	t.Parallel()
	off := broker.DefaultProperties(broker.KindQueue)
	off.Status = broker.StatusSendDisabled
	srv := startServer(t, server.Options{Queues: []server.Queue{{Name: "input"}, {Name: "off", Properties: off}}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "input"), bodies("order")...)
	c := dialRaw(t, srv)
	coord := c.attachCoordinator()
	c.attachReceiver("r1", "input", 10)
	via := c.attachSender("via", "input", encoding.Map{{Key: encoding.Symbol("com.microsoft:transfer-destination-address"), Value: "off"}})
	in := c.receive()

	id := c.declare(coord)
	wantAccepted(t, "via send", c.transfer(via, rawMessage(t, "shipped", ""), txnState(id, nil)), id)
	c.dispose(in.id, txnState(id, &frames.Accepted{}))
	// The commit succeeds: the input message is consumed and the output
	// message that could not be transferred waits in the transfer
	// dead-letter queue of the via entity, as the documentation says.
	if _, ok := c.discharge(coord, id, false).(*frames.Accepted); !ok {
		t.Fatal("commit not accepted")
	}
	tdlq := &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueTransfer, ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete}
	msgs := receive(t, newReceiver(t, client, "input", tdlq), 1)
	if string(msgs[0].Body) != "shipped" || str(msgs[0].DeadLetterReason) != "TransferFailed" {
		t.Errorf("TDLQ message = %q reason %q", msgs[0].Body, str(msgs[0].DeadLetterReason))
	}
	if n := peekCount(t, client, "input", nil); n != 0 {
		t.Errorf("input after the commit = %d", n)
	}
}

func TestTransactionTimeout(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders"), TxnTimeout: 300 * time.Millisecond})
	client := newClient(t, srv)
	c := dialRaw(t, srv)
	coord := c.attachCoordinator()
	sender := c.attachSender("s1", "orders", nil)
	id := c.declare(coord)
	wantAccepted(t, "send", c.transfer(sender, rawMessage(t, "slow", ""), txnState(id, nil)), id)
	time.Sleep(500 * time.Millisecond)
	wantRejected(t, "commit after the timeout", c.discharge(coord, id, false), "amqp:transaction:timeout")
	if n := peekCount(t, client, "orders", nil); n != 0 {
		t.Errorf("messages after the timeout = %d, want 0", n)
	}
}

func TestTransactionCommitFailureRollsEverythingBack(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("work", func(p *broker.Properties) { p.LockDuration = 400 * time.Millisecond }),
		{Name: "out"},
	}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "work"), bodies("m")...)
	c := dialRaw(t, srv)
	coord := c.attachCoordinator()
	c.attachReceiver("r1", "work", 10)
	out := c.attachSender("out", "out", nil)
	m := c.receive()

	id := c.declare(coord)
	c.dispose(m.id, txnState(id, &frames.Accepted{}))
	wantAccepted(t, "send", c.transfer(out, rawMessage(t, "result", ""), txnState(id, nil)), id)
	// The lock lapses before the commit, so the settlement fails and the
	// send goes with it.
	time.Sleep(700 * time.Millisecond)
	wantRejected(t, "commit with a lost lock", c.discharge(coord, id, false), "amqp:transaction:rollback")
	if n := peekCount(t, client, "out", nil); n != 0 {
		t.Errorf("out after a failed commit = %d, want 0: the send must roll back too", n)
	}
}

func TestTransactionUnknownID(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	c := dialRaw(t, srv)
	c.attachCoordinator()
	sender := c.attachSender("s1", "orders", nil)
	wantRejected(t, "send in an unknown transaction", c.transfer(sender, rawMessage(t, "x", ""), txnState([]byte("nope"), nil)), "amqp:transaction:unknown-id")
}

func strPtr(s string) *string { return &s }

// TestSASLMechanisms checks the mechanisms the server offers and accepts:
// the .NET SDK refuses to connect without MSSBCBS.
func TestSASLMechanisms(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{})
	for _, mech := range []string{"ANONYMOUS", "PLAIN", "MSSBCBS"} {
		conn, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(10 * time.Second))
		hdr := frames.ProtocolHeaderSASL
		conn.Write(hdr[:])
		br := bufio.NewReader(conn)
		var got [8]byte
		if _, err := io.ReadFull(br, got[:]); err != nil {
			t.Fatalf("%s: reading the header: %v", mech, err)
		}
		f, err := frames.ReadFrame(br, 1<<16)
		if err != nil {
			t.Fatal(err)
		}
		offered := f.Body.(*frames.SASLMechanisms).Mechanisms
		if !slices.Contains(offered, encoding.Symbol(mech)) {
			t.Errorf("mechanisms offered = %v, want %s among them", offered, mech)
		}
		buf, _ := frames.AppendFrame(nil, frames.TypeSASL, 0, &frames.SASLInit{Mechanism: encoding.Symbol(mech), InitialResponse: []byte("\x00u\x00p")}, nil)
		conn.Write(buf)
		f, err = frames.ReadFrame(br, 1<<16)
		if err != nil {
			t.Fatal(err)
		}
		if out, ok := f.Body.(*frames.SASLOutcome); !ok || out.Code != frames.SASLCodeOK {
			t.Errorf("%s outcome = %+v, want ok", mech, f.Body)
		}
		conn.Close()
	}
}
