package integration_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func sqlRule(name, expr, action string) broker.Rule {
	return broker.Rule{Name: name, Filter: broker.RuleFilter{Kind: broker.FilterSQL, SQL: expr}, Action: action}
}

func subscription(name string, rules []broker.Rule, edit func(*broker.Properties)) server.Subscription {
	p := broker.DefaultProperties(broker.KindSubscription)
	if edit != nil {
		edit(&p)
	}
	return server.Subscription{Name: name, Properties: p, Rules: rules}
}

func newSubReceiver(t *testing.T, client *azservicebus.Client, topic, sub string, opts *azservicebus.ReceiverOptions) *azservicebus.Receiver {
	t.Helper()
	r, err := client.NewReceiverForSubscription(topic, sub, opts)
	if err != nil {
		t.Fatalf("NewReceiverForSubscription(%s, %s) error: %v", topic, sub, err)
	}
	t.Cleanup(func() { r.Close(context.Background()) })
	return r
}

var radOptions = &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete}

func sortedBodies(msgs []*azservicebus.ReceivedMessage) []string {
	out := bodiesOf(msgs)
	sort.Strings(out)
	return out
}

func TestSubscriptionFilters(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Topics: []server.Topic{{
		Name: "events",
		Subscriptions: []server.Subscription{
			subscription("all", nil, nil),
			subscription("approved", []broker.Rule{sqlRule("approved", "event in ('completed', 'approved') AND isApproved = TRUE", "")}, nil),
			subscription("big", []broker.Rule{sqlRule("big", "quantity > 10 AND sys.Label LIKE 'order.%'", "")}, nil),
			subscription("eu", []broker.Rule{{Name: "eu", Filter: broker.RuleFilter{Kind: broker.FilterCorrelation, Correlation: &filter.CorrelationFilter{
				Subject: "order.created", Properties: map[string]any{"region": "eu"}}}}}, nil),
		},
	}}})
	client := newClient(t, srv)
	msg := func(body, subject string, props map[string]any) *azservicebus.Message {
		return &azservicebus.Message{Body: []byte(body), Subject: &subject, ApplicationProperties: props}
	}
	send(t, newSender(t, client, "events"),
		msg("m1", "order.created", map[string]any{"event": "completed", "isApproved": true, "quantity": int64(20), "region": "eu"}),
		msg("m2", "order.updated", map[string]any{"event": "approved", "isApproved": false, "quantity": int32(50), "region": "eu"}),
		msg("m3", "other", map[string]any{"event": "completed", "isApproved": true, "quantity": 5.5}),
	)

	for sub, want := range map[string][]string{
		"all":      {"m1", "m2", "m3"},
		"approved": {"m1", "m3"},
		"big":      {"m1", "m2"},
		"eu":       {"m1"},
	} {
		r := newSubReceiver(t, client, "events", sub, radOptions)
		if diff := cmp.Diff(want, sortedBodies(receive(t, r, len(want)))); diff != "" {
			t.Errorf("subscription %s mismatch (-want +got):\n%s", sub, diff)
		}
		receiveNone(t, r, 200*time.Millisecond)
	}
}

func TestRuleActions(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Topics: []server.Topic{{
		Name: "orders",
		Subscriptions: []server.Subscription{subscription("s", []broker.Rule{
			broker.TrueRule("all"),
			sqlRule("red", "color = 'red'", "SET quantity = quantity / 2; SET sys.To = 'warehouse'; REMOVE color"),
		}, nil)},
	}}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), &azservicebus.Message{
		Body:                  []byte("order"),
		ApplicationProperties: map[string]any{"color": "red", "quantity": int64(10)},
	})

	msgs := receive(t, newSubReceiver(t, client, "orders", "s", radOptions), 2)
	type copyInfo struct {
		To    string
		Props map[string]any
	}
	var got []copyInfo
	for _, m := range msgs {
		got = append(got, copyInfo{To: deref(m.To), Props: m.ApplicationProperties})
	}
	sort.Slice(got, func(i, j int) bool { return got[i].To < got[j].To })
	want := []copyInfo{
		{Props: map[string]any{"color": "red", "quantity": int64(10)}},
		{To: "warehouse", Props: map[string]any{"quantity": int64(5), "RuleName": "red"}},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("copies mismatch (-want +got):\n%s", diff)
	}
	if *msgs[0].SequenceNumber == *msgs[1].SequenceNumber {
		t.Errorf("both copies have sequence number %d, want distinct", *msgs[0].SequenceNumber)
	}
}

func TestFilterEvaluationErrorDeadLetters(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Topics: []server.Topic{{
		Name:          "t",
		Subscriptions: []server.Subscription{subscription("s", []broker.Rule{sqlRule("bad", "sys.NoSuchProperty = 1", "")}, nil)},
	}}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "t"), bodies("x")...)
	dlq := newSubReceiver(t, client, "t", "s", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	m := receive(t, dlq, 1)[0]
	if got := deref(m.DeadLetterReason); got != broker.ReasonFilterEvaluation {
		t.Errorf("DeadLetterReason = %q, want %s", got, broker.ReasonFilterEvaluation)
	}
}

func TestAutoForwarding(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{
		Queues: []server.Queue{
			{Name: "inbox"},
			{Name: "poison"},
			queue("front", func(p *broker.Properties) { p.ForwardTo = "events" }),
			queue("work", func(p *broker.Properties) {
				p.MaxDeliveryCount = 1
				p.ForwardDeadLetteredMessagesTo = "poison"
			}),
		},
		Topics: []server.Topic{{
			Name: "events",
			Subscriptions: []server.Subscription{
				subscription("relay", []broker.Rule{sqlRule("red", "color = 'red'", "")}, func(p *broker.Properties) { p.ForwardTo = "inbox" }),
				subscription("audit", nil, nil),
			},
		}},
	})
	client := newClient(t, srv)
	send(t, newSender(t, client, "front"),
		&azservicebus.Message{Body: []byte("red"), ApplicationProperties: map[string]any{"color": "red"}},
		&azservicebus.Message{Body: []byte("blue"), ApplicationProperties: map[string]any{"color": "blue"}},
	)
	if diff := cmp.Diff([]string{"red"}, bodiesOf(receive(t, newReceiver(t, client, "inbox", radOptions), 1))); diff != "" {
		t.Errorf("inbox mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"blue", "red"}, sortedBodies(receive(t, newSubReceiver(t, client, "events", "audit", radOptions), 2))); diff != "" {
		t.Errorf("audit mismatch (-want +got):\n%s", diff)
	}

	// A forwarding entity has no receivers.
	r, err := client.NewReceiverForQueue("front", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = r.ReceiveMessages(ctx, 1, nil)
	var sbErr *azservicebus.Error
	if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeTimeout {
		t.Errorf("ReceiveMessages(front) error = %v, want a refused receiver", err)
	}

	// Dead letters of work go to poison.
	send(t, newSender(t, client, "work"), bodies("bad")...)
	wr := newReceiver(t, client, "work", nil)
	m := receive(t, wr, 1)[0]
	if err := wr.DeadLetterMessage(testContext(t), m, &azservicebus.DeadLetterOptions{Reason: new("Invalid")}); err != nil {
		t.Fatalf("DeadLetterMessage() error: %v", err)
	}
	p := receive(t, newReceiver(t, client, "poison", radOptions), 1)[0]
	if string(p.Body) != "bad" || deref(p.DeadLetterReason) != "Invalid" {
		t.Errorf("poison got body %q reason %q, want bad/Invalid", p.Body, deref(p.DeadLetterReason))
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// docFilterSubscriptions are examples of the Service Bus pages on SQL
// filters, one per subscription.
func docFilterSubscriptions() []server.Subscription {
	return []server.Subscription{
		subscription("prefix", []broker.Rule{sqlRule("r", `user.Code LIKE 'order-%'`, "")}, nil),
		subscription("escape", []broker.Rule{sqlRule("r", `user.Code LIKE '%!%%' ESCAPE '!'`, "")}, nil),
		subscription("delimited", []broker.Rule{sqlRule("r", `[Order Type] = 'priority'`, "")}, nil),
		subscription("quote", []broker.Rule{sqlRule("r", `user.CustomerName = 'O''Brien'`, "")}, nil),
		subscription("numeric", []broker.Rule{sqlRule("r", `Quantity >= 10 AND Price > 2.08`, "")}, nil),
	}
}

func TestDocumentedFiltersSDK(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Topics: []server.Topic{{Name: "docs", Subscriptions: docFilterSubscriptions()}}})
	client := newClient(t, srv)
	msg := func(body string, props map[string]any) *azservicebus.Message {
		return &azservicebus.Message{Body: []byte(body), ApplicationProperties: props}
	}
	send(t, newSender(t, client, "docs"),
		msg("m1", map[string]any{"Code": "order-1", "Order Type": "priority", "CustomerName": "O'Brien", "Quantity": int64(15), "Price": 3.0}),
		msg("m2", map[string]any{"Code": "100%", "Order Type": "standard", "Quantity": int32(9), "Price": 1.0}),
		msg("m3", map[string]any{"Code": "preorder-1", "Quantity": int32(20), "Price": 2.5}),
	)
	for sub, want := range map[string][]string{
		"prefix":    {"m1"},
		"escape":    {"m2"},
		"delimited": {"m1"},
		"quote":     {"m1"},
		"numeric":   {"m1", "m3"},
	} {
		r := newSubReceiver(t, client, "docs", sub, radOptions)
		if diff := cmp.Diff(want, sortedBodies(receive(t, r, len(want)))); diff != "" {
			t.Errorf("subscription %s mismatch (-want +got):\n%s", sub, diff)
		}
		receiveNone(t, r, 200*time.Millisecond)
	}
}
