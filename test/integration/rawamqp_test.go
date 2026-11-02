package integration_test

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// rawClient speaks AMQP 1.0 frame by frame. The Go and JavaScript SDKs have
// no transactions, so the tests drive the coordinator the way the .NET
// SDK's controller does. Steps run in lockstep: each waits for the frame it
// needs and drops the flow frames in between.
type rawClient struct {
	t      *testing.T
	conn   net.Conn
	br     *bufio.Reader
	handle uint32
	nextID uint32
	// transfers received while waiting for something else.
	pending []*frames.Transfer
	// state of the last receiver link, for flow frames.
	recvHandle    uint32
	recvDelivered uint32
	recvCredit    uint32
}

func dialRaw(t *testing.T, srv *server.Server) *rawClient {
	t.Helper()
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	c := &rawClient{t: t, conn: conn, br: bufio.NewReader(conn)}
	c.write(ProtocolHeader())
	var hdr [8]byte
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.br.Read(hdr[:]); err != nil {
		t.Fatalf("reading the protocol header: %v", err)
	}
	max := uint32(1 << 20)
	c.send(0, &frames.Open{ContainerID: "raw-client", MaxFrameSize: &max})
	c.expect(func(b frames.Body) bool { _, ok := b.(*frames.Open); return ok })
	c.send(0, &frames.Begin{NextOutgoingID: 0, IncomingWindow: 1000, OutgoingWindow: 1000})
	c.expect(func(b frames.Body) bool { _, ok := b.(*frames.Begin); return ok })
	return c
}

// ProtocolHeader is the plain AMQP header.
func ProtocolHeader() []byte { h := frames.ProtocolHeaderAMQP; return h[:] }

func (c *rawClient) write(raw []byte) {
	c.t.Helper()
	if _, err := c.conn.Write(raw); err != nil {
		c.t.Fatal(err)
	}
}

func (c *rawClient) send(channel uint16, body frames.Body) { c.sendPayload(channel, body, nil) }

func (c *rawClient) sendPayload(channel uint16, body frames.Body, payload []byte) {
	c.t.Helper()
	buf, err := frames.AppendFrame(nil, frames.TypeAMQP, channel, body, payload)
	if err != nil {
		c.t.Fatalf("encoding %T: %v", body, err)
	}
	if _, err := c.conn.Write(buf); err != nil {
		c.t.Fatal(err)
	}
}

// expect reads frames until one matches, dropping the others, and returns it.
func (c *rawClient) expect(match func(frames.Body) bool) frames.Body {
	c.t.Helper()
	for {
		c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		f, err := frames.ReadFrame(c.br, 1<<20)
		if err != nil {
			c.t.Fatalf("waiting for a frame: %v", err)
		}
		if f.Body != nil && match(f.Body) {
			return f.Body
		}
		if tr, ok := f.Body.(*frames.Transfer); ok {
			c.pending = append(c.pending, tr)
		}
	}
}

func (c *rawClient) newHandle() uint32 { h := c.handle; c.handle++; return h }

// attachSender opens a link on which the client sends to target, with the
// given link properties, and waits for the server's attach and credit.
func (c *rawClient) attachSender(name, target string, props encoding.Map) uint32 {
	c.t.Helper()
	h := c.newHandle()
	zero := uint32(0)
	c.send(0, &frames.Attach{
		Name: name, Handle: h, Role: frames.RoleSender,
		Source: &frames.Source{}, Target: &frames.Target{Address: target},
		InitialDeliveryCount: &zero, Properties: props,
	})
	return c.finishSenderAttach(h)
}

func (c *rawClient) finishSenderAttach(h uint32) uint32 {
	c.t.Helper()
	body := c.expect(func(b frames.Body) bool { _, ok := b.(*frames.Attach); return ok }).(*frames.Attach)
	if body.Target == nil && body.Coordinator == nil {
		c.t.Fatalf("the server refused the link: %+v", body)
	}
	c.expect(func(b frames.Body) bool {
		f, ok := b.(*frames.Flow)
		return ok && f.LinkCredit != nil && *f.LinkCredit > 0
	})
	return h
}

// attachCoordinator opens the link to the transaction coordinator.
func (c *rawClient) attachCoordinator() uint32 {
	c.t.Helper()
	h := c.newHandle()
	zero := uint32(0)
	c.send(0, &frames.Attach{
		Name: "txn-controller", Handle: h, Role: frames.RoleSender,
		Source:               &frames.Source{},
		Coordinator:          &frames.Coordinator{Capabilities: []encoding.Symbol{frames.LocalTransactions}},
		InitialDeliveryCount: &zero,
	})
	return c.finishSenderAttach(h)
}

// attachReceiver opens a link on which the client receives from source,
// granting the credit.
func (c *rawClient) attachReceiver(name, source string, credit uint32) uint32 {
	c.t.Helper()
	h := c.newHandle()
	second := uint8(frames.ReceiverSettleModeSecond)
	c.send(0, &frames.Attach{
		Name: name, Handle: h, Role: frames.RoleReceiver,
		ReceiverSettleMode: &second,
		Source:             &frames.Source{Address: source}, Target: &frames.Target{Address: name},
	})
	att := c.expect(func(b frames.Body) bool { _, ok := b.(*frames.Attach); return ok }).(*frames.Attach)
	if att.Source == nil {
		c.t.Fatalf("the server refused the receiver link: %+v", att)
	}
	c.recvHandle, c.recvDelivered = h, 0
	c.grantCredit(credit)
	return h
}

// grantCredit gives the last receiver link n more credit.
func (c *rawClient) grantCredit(n uint32) {
	c.t.Helper()
	h := c.recvHandle
	next := c.nextID
	total := c.recvDelivered
	c.recvCredit += n
	c.send(0, &frames.Flow{
		NextIncomingID: &next, IncomingWindow: 1000, NextOutgoingID: c.nextID, OutgoingWindow: 1000,
		Handle: &h, DeliveryCount: &total, LinkCredit: &c.recvCredit,
	})
}

// transfer sends one delivery and returns the state of the disposition the
// server answers with.
func (c *rawClient) transfer(handle uint32, payload []byte, state frames.DeliveryState) frames.DeliveryState {
	c.t.Helper()
	id := c.nextID
	c.nextID++
	zero := uint32(0)
	c.sendPayload(0, &frames.Transfer{
		Handle: handle, DeliveryID: &id, DeliveryTag: []byte(fmt.Sprintf("tag-%d", id)),
		MessageFormat: &zero, State: state,
	}, payload)
	d := c.expect(func(b frames.Body) bool {
		disp, ok := b.(*frames.Disposition)
		return ok && disp.Role == frames.RoleReceiver && disp.First <= id && (disp.Last == nil && disp.First == id || disp.Last != nil && *disp.Last >= id)
	}).(*frames.Disposition)
	return d.State
}

// declare starts a transaction on the coordinator link.
func (c *rawClient) declare(coord uint32) []byte {
	c.t.Helper()
	payload, err := frames.MarshalMessage(&frames.Message{Value: &frames.Declare{}, HasValue: true})
	if err != nil {
		c.t.Fatal(err)
	}
	st := c.transfer(coord, payload, nil)
	declared, ok := st.(*frames.Declared)
	if !ok {
		c.t.Fatalf("declare answered %T %+v, want declared", st, st)
	}
	return declared.TxnID
}

// discharge ends a transaction, committing it unless fail is set, and
// returns the outcome.
func (c *rawClient) discharge(coord uint32, id []byte, fail bool) frames.DeliveryState {
	c.t.Helper()
	payload, err := frames.MarshalMessage(&frames.Message{Value: &frames.Discharge{TxnID: id, Fail: fail}, HasValue: true})
	if err != nil {
		c.t.Fatal(err)
	}
	return c.transfer(coord, payload, nil)
}

// received is a delivery the client got.
type received struct {
	id      uint32
	body    string
	subject string
}

// receive waits for one delivery on a receiver link.
func (c *rawClient) receive() received {
	c.t.Helper()
	var tr *frames.Transfer
	if len(c.pending) > 0 {
		tr, c.pending = c.pending[0], c.pending[1:]
	} else {
		tr = c.expect(func(b frames.Body) bool { _, ok := b.(*frames.Transfer); return ok }).(*frames.Transfer)
	}
	msg, err := frames.UnmarshalMessage(tr.Payload)
	if err != nil {
		c.t.Fatalf("decoding the delivery: %v", err)
	}
	c.recvDelivered++
	c.recvCredit--
	r := received{id: *tr.DeliveryID}
	if len(msg.Data) > 0 {
		r.body = string(msg.Data[0])
	}
	if msg.Properties != nil {
		r.subject = msg.Properties.Subject
	}
	return r
}

// dispose settles a received delivery with state and returns the state of
// the server's answer.
func (c *rawClient) dispose(id uint32, state frames.DeliveryState) frames.DeliveryState {
	c.t.Helper()
	c.send(0, &frames.Disposition{Role: frames.RoleReceiver, First: id, State: state})
	d := c.expect(func(b frames.Body) bool {
		disp, ok := b.(*frames.Disposition)
		return ok && disp.Role == frames.RoleSender && disp.First == id
	}).(*frames.Disposition)
	return d.State
}

// message builds an encoded message with a text body.
func rawMessage(t *testing.T, body, subject string) []byte {
	t.Helper()
	payload, err := frames.MarshalMessage(&frames.Message{
		Properties: &frames.Properties{MessageID: body, Subject: subject},
		Data:       [][]byte{[]byte(body)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
