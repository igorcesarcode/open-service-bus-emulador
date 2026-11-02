// Command goservice is a small Go service for the end-to-end tests. It uses
// the official azservicebus SDK the way a real service would.
//
// Usage:
//
//	goservice produce -count n
//	goservice work -name w1 [-idle 3s]
//	goservice respond [-idle 3s]
//	goservice request -count n
//	goservice publish-events -count n
//	goservice consume-events [-idle 3s]
//	goservice admin-check
//	goservice send-payments -count n
//	goservice collect-payments [-idle 3s]
//	goservice publish-alerts -count n
//	goservice consume-alerts [-idle 3s]
//
// It connects to OSB_CONNECTION_STRING and writes spec.Event lines to
// stdout. With OSB_TRANSPORT=websocket it connects over WebSocket, trusting
// the system roots (SSL_CERT_FILE on Linux). test/node/e2e/service.mjs implements the same commands.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/coder/websocket"

	"github.com/igorcesarcode/open-service-bus-emulador/test/e2e/spec"
)

const lang = "go"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "goservice:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("missing command: produce, work, respond, request, publish-events or consume-events")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	count := fs.Int("count", 24, "number of orders or requests")
	name := fs.String("name", "worker", "worker name reported in results")
	idle := fs.Duration("idle", 3*time.Second, "exit after this long without work")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client, err := newClient(os.Getenv("OSB_CONNECTION_STRING"), os.Getenv("OSB_TRANSPORT"))
	if err != nil {
		return err
	}
	defer client.Close(context.Background())

	switch args[0] {
	case "produce":
		return produce(ctx, client, *count)
	case "work":
		return (&worker{client: client, name: *name, idle: *idle}).run(ctx)
	case "respond":
		return respond(ctx, client, *idle)
	case "request":
		return request(ctx, client, *count)
	case "publish-events":
		return publishEvents(ctx, client, *count)
	case "consume-events":
		return consumeEvents(ctx, client, *idle)
	case "admin-check":
		return adminCheck(ctx)
	case "send-payments":
		return sendPayments(ctx, client, *count)
	case "collect-payments":
		return collectPayments(ctx, client, *idle)
	case "publish-alerts":
		return publishAlerts(ctx, client, *count)
	case "consume-alerts":
		return consumeAlerts(ctx, client, *idle)
	}
	return fmt.Errorf("unknown command %q", args[0])
}

// newClient connects over AMQP, or over WebSocket when transport is
// "websocket".
func newClient(connStr, transport string) (*azservicebus.Client, error) {
	var opts azservicebus.ClientOptions
	switch transport {
	case "", "amqp":
	case "websocket":
		opts.NewWebSocketConn = func(ctx context.Context, args azservicebus.NewWebSocketConnArgs) (net.Conn, error) {
			ws, _, err := websocket.Dial(ctx, args.Host, &websocket.DialOptions{Subprotocols: []string{"AMQPWSB10"}})
			if err != nil {
				return nil, err
			}
			return websocket.NetConn(context.Background(), ws, websocket.MessageBinary), nil
		}
	default:
		return nil, fmt.Errorf("unknown OSB_TRANSPORT %q: want amqp or websocket", transport)
	}
	return azservicebus.NewClientFromConnectionString(connStr, &opts)
}

func emit(e spec.Event) {
	b, _ := json.Marshal(e)
	fmt.Println(string(b))
}

func produce(ctx context.Context, client *azservicebus.Client, n int) error {
	sender, err := client.NewSender(spec.OrdersTopic, nil)
	if err != nil {
		return err
	}
	defer sender.Close(ctx)
	for _, o := range spec.Orders(n) {
		body, err := json.Marshal(spec.Body{ID: o.ID, Customer: o.Customer, Seq: o.Seq, Quantity: o.Quantity, Amount: o.Amount})
		if err != nil {
			return err
		}
		msg := &azservicebus.Message{
			MessageID:   &o.ID,
			SessionID:   &o.Customer,
			Subject:     new("order.created"),
			ContentType: new("application/json"),
			Body:        body,
			ApplicationProperties: map[string]any{
				"customer":  o.Customer,
				"quantity":  o.Quantity,
				"amount":    o.Amount,
				"express":   o.Express,
				"kind":      o.Kind,
				"createdAt": o.CreatedAt,
			},
		}
		if err := sender.SendMessage(ctx, msg, nil); err != nil {
			return fmt.Errorf("sending %s: %w", o.ID, err)
		}
		emit(spec.Event{Event: "sent", ID: o.ID})
	}
	return nil
}

// worker processes the orders of one customer session at a time, in order.
type worker struct {
	client  *azservicebus.Client
	name    string
	idle    time.Duration
	results *azservicebus.Sender
}

func (w *worker) run(ctx context.Context) error {
	var err error
	if w.results, err = w.client.NewSender(spec.ResultsQueue, nil); err != nil {
		return err
	}
	defer w.results.Close(context.Background())
	for {
		actx, cancel := context.WithTimeout(ctx, w.idle)
		r, err := w.client.AcceptNextSessionForSubscription(actx, spec.OrdersTopic, spec.FulfillmentSub, nil)
		cancel()
		var sbErr *azservicebus.Error
		switch {
		case errors.Is(err, context.DeadlineExceeded) || errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeTimeout:
			return nil // No session left to work on.
		case err != nil:
			return fmt.Errorf("accepting a session: %w", err)
		}
		err = w.session(ctx, r)
		r.Close(context.Background())
		if err != nil {
			return fmt.Errorf("session %s: %w", r.SessionID(), err)
		}
	}
}

// session handles the messages of a session until it is empty. It takes
// one message at a time: a retried message must be processed before the
// ones after it.
func (w *worker) session(ctx context.Context, r *azservicebus.SessionReceiver) error {
	for {
		rctx, cancel := context.WithTimeout(ctx, time.Second)
		msgs, err := r.ReceiveMessages(rctx, 1, nil)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if len(msgs) == 0 {
			return nil
		}
		if err := w.handle(ctx, r, msgs[0]); err != nil {
			return err
		}
	}
}

func (w *worker) handle(ctx context.Context, r *azservicebus.SessionReceiver, m *azservicebus.ReceivedMessage) error {
	kind, _ := m.ApplicationProperties["kind"].(string)
	if kind == spec.KindPoison || kind == spec.KindFlaky && m.DeliveryCount == 1 {
		emit(spec.Event{Event: "abandoned", ID: m.MessageID, Attempt: int(m.DeliveryCount)})
		return r.AbandonMessage(ctx, m, nil)
	}

	var body spec.Body
	if err := json.Unmarshal(m.Body, &body); err != nil {
		return fmt.Errorf("decoding %s: %w", m.MessageID, err)
	}
	props, err := resultProps(m.ApplicationProperties)
	if err != nil {
		return fmt.Errorf("decoding %s: %w", m.MessageID, err)
	}
	res, err := json.Marshal(spec.Result{
		ID:       m.MessageID,
		Customer: deref(m.SessionID),
		Seq:      body.Seq,
		Attempt:  int(m.DeliveryCount),
		Worker:   w.name,
		Lang:     lang,
		Body:     body,
		Props:    props,
		Subject:  deref(m.Subject),
	})
	if err != nil {
		return err
	}
	err = w.results.SendMessage(ctx, &azservicebus.Message{
		CorrelationID:         &m.MessageID,
		Subject:               new("order.processed"),
		ContentType:           new("application/json"),
		Body:                  res,
		ApplicationProperties: map[string]any{"worker": w.name, "lang": lang},
	}, nil)
	if err != nil {
		return err
	}
	if err := r.CompleteMessage(ctx, m, nil); err != nil {
		return err
	}
	emit(spec.Event{Event: "processed", ID: m.MessageID, Attempt: int(m.DeliveryCount)})
	return nil
}

// resultProps reads the typed application properties of an order.
func resultProps(p map[string]any) (spec.ResultProps, error) {
	var out spec.ResultProps
	var ok bool
	if out.Customer, ok = p["customer"].(string); !ok {
		return out, fmt.Errorf("customer is %T, want string", p["customer"])
	}
	if out.Kind, ok = p["kind"].(string); !ok {
		return out, fmt.Errorf("kind is %T, want string", p["kind"])
	}
	if out.Express, ok = p["express"].(bool); !ok {
		return out, fmt.Errorf("express is %T, want bool", p["express"])
	}
	if out.Amount, ok = p["amount"].(float64); !ok {
		return out, fmt.Errorf("amount is %T, want float64", p["amount"])
	}
	if out.Quantity, ok = spec.Int64(p["quantity"]); !ok {
		return out, fmt.Errorf("quantity is %T, want an integer", p["quantity"])
	}
	created, ok := p["createdAt"].(time.Time)
	if !ok {
		return out, fmt.Errorf("createdAt is %T, want time.Time", p["createdAt"])
	}
	out.CreatedAt = spec.FormatTime(created)
	return out, nil
}

func respond(ctx context.Context, client *azservicebus.Client, idle time.Duration) error {
	r, err := client.NewReceiverForQueue(spec.PricingQueue, nil)
	if err != nil {
		return err
	}
	defer r.Close(context.Background())
	senders := map[string]*azservicebus.Sender{}
	defer func() {
		for _, s := range senders {
			s.Close(context.Background())
		}
	}()
	for {
		rctx, cancel := context.WithTimeout(ctx, idle)
		msgs, err := r.ReceiveMessages(rctx, 10, nil)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if len(msgs) == 0 {
			return nil
		}
		for _, m := range msgs {
			to := deref(m.ReplyTo)
			s := senders[to]
			if s == nil {
				if s, err = client.NewSender(to, nil); err != nil {
					return err
				}
				senders[to] = s
			}
			reply, err := priceReply(m)
			if err != nil {
				return err
			}
			if err := s.SendMessage(ctx, reply, nil); err != nil {
				return err
			}
			if err := r.CompleteMessage(ctx, m, nil); err != nil {
				return err
			}
			emit(spec.Event{Event: "replied", ID: m.MessageID})
		}
	}
}

func priceReply(m *azservicebus.ReceivedMessage) (*azservicebus.Message, error) {
	reply := &azservicebus.Message{
		CorrelationID: &m.MessageID,
		SessionID:     m.ReplyToSessionID,
		Subject:       new("price.quoted"),
	}
	if deref(m.ContentType) != "application/json" {
		body := slices.Clone(m.Body)
		slices.Reverse(body)
		reply.Body = body
		reply.ContentType = new("application/octet-stream")
		return reply, nil
	}
	var req spec.PriceRequest
	if err := json.Unmarshal(m.Body, &req); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", m.MessageID, err)
	}
	body, err := json.Marshal(spec.PriceReply{SKU: req.SKU, Qty: req.Qty, Total: float64(req.Qty) * spec.UnitPrice, Responder: lang})
	if err != nil {
		return nil, err
	}
	reply.Body = body
	reply.ContentType = new("application/json")
	return reply, nil
}

func request(ctx context.Context, client *azservicebus.Client, n int) error {
	sessionID := lang + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	sender, err := client.NewSender(spec.PricingQueue, nil)
	if err != nil {
		return err
	}
	defer sender.Close(context.Background())
	for i := range n {
		msg := &azservicebus.Message{
			MessageID:        new(fmt.Sprintf("req-%d", i)),
			ReplyTo:          new(spec.PricingRepliesQueue),
			ReplyToSessionID: &sessionID,
		}
		if spec.IsBinaryRequest(i) {
			msg.Body = spec.BinaryRequest(i)
			msg.ContentType = new("application/octet-stream")
		} else {
			if msg.Body, err = json.Marshal(spec.PriceRequest{SKU: fmt.Sprintf("sku-%d", i), Qty: i + 1}); err != nil {
				return err
			}
			msg.ContentType = new("application/json")
		}
		if err := sender.SendMessage(ctx, msg, nil); err != nil {
			return err
		}
	}

	r, err := client.AcceptSessionForQueue(ctx, spec.PricingRepliesQueue, sessionID, nil)
	if err != nil {
		return err
	}
	defer r.Close(context.Background())
	for got := 0; got < n; {
		msgs, err := r.ReceiveMessages(ctx, n-got, nil)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			e := spec.Event{
				Event:         "reply",
				CorrelationID: deref(m.CorrelationID),
				SessionID:     deref(m.SessionID),
				ContentType:   deref(m.ContentType),
				Subject:       deref(m.Subject),
			}
			if e.ContentType == "application/json" {
				e.Body = &spec.PriceReply{}
				if err := json.Unmarshal(m.Body, e.Body); err != nil {
					return fmt.Errorf("decoding reply %s: %w", e.CorrelationID, err)
				}
			} else {
				e.BodyHex = hex.EncodeToString(m.Body)
			}
			if err := r.CompleteMessage(ctx, m, nil); err != nil {
				return err
			}
			emit(e)
			got++
		}
	}
	return nil
}

func publishEvents(ctx context.Context, client *azservicebus.Client, n int) error {
	sender, err := client.NewSender(spec.EventsTopic, nil)
	if err != nil {
		return err
	}
	defer sender.Close(context.Background())
	for _, e := range spec.RoutedEvents(n) {
		err := sender.SendMessage(ctx, &azservicebus.Message{
			MessageID: &e.ID,
			Subject:   &e.Subject,
			Body:      []byte(e.ID),
			ApplicationProperties: map[string]any{
				"event":      e.Event,
				"isApproved": e.IsApproved,
				"quantity":   e.Quantity,
				"region":     e.Region,
			},
		}, nil)
		if err != nil {
			return fmt.Errorf("sending %s: %w", e.ID, err)
		}
	}
	jobs, err := client.NewSender(spec.JobsQueue, nil)
	if err != nil {
		return err
	}
	defer jobs.Close(context.Background())
	for i := range spec.JobCount {
		if err := jobs.SendMessage(ctx, &azservicebus.Message{MessageID: new(fmt.Sprintf("job-%d", i)), Body: []byte("job")}, nil); err != nil {
			return err
		}
	}
	return nil
}

// consumeEvents drains the routed subscriptions and the escalations queue,
// reporting what arrived where, then rejects the jobs.
func consumeEvents(ctx context.Context, client *azservicebus.Client, idle time.Duration) error {
	rad := &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete}
	drain := func(entity string, r *azservicebus.Receiver) error {
		defer r.Close(context.Background())
		for {
			rctx, cancel := context.WithTimeout(ctx, idle)
			msgs, err := r.ReceiveMessages(rctx, 10, nil)
			cancel()
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if len(msgs) == 0 {
				return nil
			}
			for _, m := range msgs {
				e := spec.Event{Event: "routed", ID: m.MessageID, Entity: entity}
				e.RuleName, _ = m.ApplicationProperties["RuleName"].(string)
				if p, ok := m.ApplicationProperties["priced"].(bool); ok {
					e.Priced = &p
				}
				if t, ok := spec.Int64(m.ApplicationProperties["total"]); ok {
					e.Total = &t
				}
				emit(e)
			}
		}
	}
	for _, sub := range spec.RoutedSubscriptions {
		r, err := client.NewReceiverForSubscription(spec.EventsTopic, sub, rad)
		if err != nil {
			return err
		}
		if err := drain(sub, r); err != nil {
			return fmt.Errorf("draining %s: %w", sub, err)
		}
	}
	r, err := client.NewReceiverForQueue(spec.EscalationsQueue, rad)
	if err != nil {
		return err
	}
	if err := drain(spec.EscalationsQueue, r); err != nil {
		return err
	}

	jobs, err := client.NewReceiverForQueue(spec.JobsQueue, nil)
	if err != nil {
		return err
	}
	defer jobs.Close(context.Background())
	for got := 0; got < spec.JobCount; {
		msgs, err := jobs.ReceiveMessages(ctx, spec.JobCount-got, nil)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			reason := spec.JobRejectedPrefix + lang
			if err := jobs.DeadLetterMessage(ctx, m, &azservicebus.DeadLetterOptions{Reason: &reason}); err != nil {
				return err
			}
			emit(spec.Event{Event: "rejected", ID: m.MessageID, Reason: reason})
			got++
		}
	}
	return nil
}

// adminCheck uses the administration client with its default transport,
// as a service would: the emulator's certificate must be trusted.
func adminCheck(ctx context.Context) error {
	ac, err := admin.NewClientFromConnectionString(os.Getenv("OSB_CONNECTION_STRING"), nil)
	if err != nil {
		return err
	}
	q, err := ac.GetQueue(ctx, spec.ResultsQueue, nil)
	if err != nil {
		return err
	}
	if q == nil {
		return fmt.Errorf("queue %s not found", spec.ResultsQueue)
	}
	emit(spec.Event{Event: "admin", ID: q.QueueName})
	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// sendPayments sends n payments one by one, then all of them again in a
// batch, as a service retrying after timeouts would. The queue keeps one
// of each.
func sendPayments(ctx context.Context, client *azservicebus.Client, n int) error {
	sender, err := client.NewSender(spec.PaymentsQueue, nil)
	if err != nil {
		return err
	}
	defer sender.Close(context.Background())
	payment := func(i int) *azservicebus.Message {
		return &azservicebus.Message{MessageID: new(spec.PaymentID(i)), Body: []byte(lang)}
	}
	for i := range n {
		if err := sender.SendMessage(ctx, payment(i), nil); err != nil {
			return err
		}
		emit(spec.Event{Event: "sent", ID: spec.PaymentID(i)})
	}
	batch, err := sender.NewMessageBatch(ctx, nil)
	if err != nil {
		return err
	}
	for i := range n {
		if err := batch.AddMessage(payment(i), nil); err != nil {
			return err
		}
	}
	if err := sender.SendMessageBatch(ctx, batch, nil); err != nil {
		return err
	}
	for i := range n {
		emit(spec.Event{Event: "sent", ID: spec.PaymentID(i)})
	}
	return nil
}

// collectPayments completes payments until none arrives for idle.
func collectPayments(ctx context.Context, client *azservicebus.Client, idle time.Duration) error {
	r, err := client.NewReceiverForQueue(spec.PaymentsQueue, nil)
	if err != nil {
		return err
	}
	defer r.Close(context.Background())
	return drainReceiver(ctx, r, idle, func(m *azservicebus.ReceivedMessage) error {
		if err := r.CompleteMessage(ctx, m, nil); err != nil {
			return err
		}
		emit(spec.Event{Event: "received", ID: m.MessageID, Entity: spec.PaymentsQueue})
		return nil
	})
}

// drainReceiver hands every message of r to handle until none arrives for
// idle.
func drainReceiver(ctx context.Context, r *azservicebus.Receiver, idle time.Duration, handle func(*azservicebus.ReceivedMessage) error) error {
	for {
		rctx, cancel := context.WithTimeout(ctx, idle)
		msgs, err := r.ReceiveMessages(rctx, 10, nil)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if len(msgs) == 0 {
			return nil
		}
		for _, m := range msgs {
			if err := handle(m); err != nil {
				return err
			}
		}
	}
}

func publishAlerts(ctx context.Context, client *azservicebus.Client, n int) error {
	sender, err := client.NewSender(spec.AlertsTopic, nil)
	if err != nil {
		return err
	}
	defer sender.Close(context.Background())
	for i := range n {
		id, channel := spec.Alert(i)
		msg := &azservicebus.Message{MessageID: &id, Body: []byte(id), ApplicationProperties: map[string]any{"channel": channel}}
		if err := sender.SendMessage(ctx, msg, nil); err != nil {
			return err
		}
	}
	return nil
}

// consumeAlerts drains every alerts subscription, reporting what arrived
// where.
func consumeAlerts(ctx context.Context, client *azservicebus.Client, idle time.Duration) error {
	for _, sub := range spec.AlertSubscriptions {
		r, err := client.NewReceiverForSubscription(spec.AlertsTopic, sub, &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
		if err != nil {
			return err
		}
		err = drainReceiver(ctx, r, idle, func(m *azservicebus.ReceivedMessage) error {
			emit(spec.Event{Event: "routed", ID: m.MessageID, Entity: sub})
			return nil
		})
		r.Close(context.Background())
		if err != nil {
			return fmt.Errorf("%s: %w", sub, err)
		}
	}
	return nil
}
