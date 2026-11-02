package broker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

type testMsg struct {
	body    string
	subject string
	props   encoding.Map
	session string
	// id is the MessageId; the body is used when it is empty.
	id string
}

func (m testMsg) incoming(t *testing.T) IncomingMessage {
	t.Helper()
	id := m.id
	if id == "" {
		id = m.body
	}
	msg := &frames.Message{
		Properties:            &frames.Properties{MessageID: id, Subject: m.subject, GroupID: m.session},
		ApplicationProperties: m.props,
		Data:                  [][]byte{[]byte(m.body)},
	}
	p, err := frames.MarshalMessage(msg)
	if err != nil {
		t.Fatalf("MarshalMessage() error: %v", err)
	}
	return IncomingMessage{Payload: p, MessageID: id, SessionID: m.session}
}

func send(t *testing.T, b *Broker, address string, msgs ...testMsg) []int64 {
	t.Helper()
	var in []IncomingMessage
	for _, m := range msgs {
		in = append(in, m.incoming(t))
	}
	seqs, err := b.Send(context.Background(), address, in)
	if err != nil {
		t.Fatalf("Send(%s) error: %v", address, err)
	}
	return seqs
}

// drainBodies receives and completes every message of address and returns
// their bodies, sorted.
func drainBodies(t *testing.T, b *Broker, address string) []string {
	t.Helper()
	ds := drain(t, b, address)
	var out []string
	for _, d := range ds {
		out = append(out, string(decode(t, d.Payload).Data[0]))
	}
	sort.Strings(out)
	return out
}

func drain(t *testing.T, b *Broker, address string) []*Delivery {
	t.Helper()
	ctx := context.Background()
	addr, err := b.Resolve(address)
	if err != nil {
		t.Fatalf("Resolve(%s) error: %v", address, err)
	}
	ds, err := b.Receive(ctx, addr, 100, ReceiveAndDelete)
	if err != nil {
		t.Fatalf("Receive(%s) error: %v", address, err)
	}
	for _, d := range ds {
		if err := b.Complete(ctx, addr, d.LockToken); err != nil {
			t.Fatalf("Complete() error: %v", err)
		}
	}
	return ds
}

func decode(t *testing.T, payload []byte) *frames.Message {
	t.Helper()
	m, err := frames.UnmarshalMessage(payload)
	if err != nil {
		t.Fatalf("UnmarshalMessage() error: %v", err)
	}
	return m
}

func sqlRule(name, expr, action string) Rule {
	return Rule{Name: name, Filter: RuleFilter{Kind: FilterSQL, SQL: expr}, Action: action}
}

func newTopic(t *testing.T, b *Broker, topic string, subs map[string][]Rule) {
	t.Helper()
	ctx := context.Background()
	if _, err := b.CreateTopic(ctx, topic, DefaultProperties(KindTopic)); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}
	for name, rules := range subs {
		if _, err := b.CreateSubscription(ctx, topic, name, DefaultProperties(KindSubscription)); err != nil {
			t.Fatalf("CreateSubscription(%s) error: %v", name, err)
		}
		if rules != nil {
			if err := b.SetRules(ctx, SubscriptionPath(topic, name), rules); err != nil {
				t.Fatalf("SetRules(%s) error: %v", name, err)
			}
		}
	}
}

func TestRulesRouteMessages(t *testing.T) {
	b, _ := newTestBroker(t)
	newTopic(t, b, "events", map[string][]Rule{
		"all":      nil,
		"approved": {sqlRule("approved", "event IN ('completed', 'approved') AND isApproved = TRUE", "")},
		"big":      {sqlRule("big", "quantity > 10", "")},
		"created": {{Name: "subject", Filter: RuleFilter{Kind: FilterCorrelation, Correlation: &filter.CorrelationFilter{
			Subject: "order.created", Properties: map[string]any{"region": "eu"}}}}},
		"none":  {},
		"false": {{Name: "never", Filter: RuleFilter{Kind: FilterFalse}}},
		"either": {
			sqlRule("a", "event = 'completed'", ""),
			sqlRule("b", "quantity > 10", ""),
		},
	})
	send(t, b, "events",
		testMsg{body: "m1", subject: "order.created", props: encoding.Map{{Key: "event", Value: "completed"}, {Key: "isApproved", Value: true}, {Key: "quantity", Value: uint32(20)}, {Key: "region", Value: "eu"}}},
		testMsg{body: "m2", subject: "order.created", props: encoding.Map{{Key: "event", Value: "completed"}, {Key: "isApproved", Value: "true"}, {Key: "region", Value: "us"}}},
		testMsg{body: "m3", props: encoding.Map{{Key: "quantity", Value: int64(5)}}},
	)
	want := map[string][]string{
		"all":      {"m1", "m2", "m3"},
		"approved": {"m1"},
		"big":      {"m1"},
		"created":  {"m1"},
		"none":     nil,
		"false":    nil,
		"either":   {"m1", "m2"}, // One copy, although m1 matches both rules.
	}
	for sub, w := range want {
		if diff := cmp.Diff(w, drainBodies(t, b, SubscriptionPath("events", sub))); diff != "" {
			t.Errorf("subscription %s mismatch (-want +got):\n%s", sub, diff)
		}
	}
}

func TestActionRulesCopyMessages(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	newTopic(t, b, "orders", map[string][]Rule{
		"s": {
			TrueRule("plain"),
			sqlRule("half", "color = 'red'", "SET quantity = quantity / 2; SET sys.Label = 'halved'; REMOVE color"),
			sqlRule("tag", "1=1", "SET tagged = TRUE"),
		},
	})
	seqs := send(t, b, "orders", testMsg{body: "m", subject: "s", props: encoding.Map{{Key: "color", Value: "red"}, {Key: "quantity", Value: int64(10)}}})

	addr, _ := b.Resolve(SubscriptionPath("orders", "s"))
	ds, err := b.Receive(ctx, addr, 10, PeekLock)
	if err != nil || len(ds) != 3 {
		t.Fatalf("Receive() = %d deliveries, %v; want 3 copies", len(ds), err)
	}
	type copyInfo struct {
		Rule, Subject string
		Props         map[string]any
	}
	var got []copyInfo
	seen := map[int64]bool{}
	for _, d := range ds {
		if seen[d.SequenceNumber] {
			t.Errorf("sequence number %d used by two copies", d.SequenceNumber)
		}
		seen[d.SequenceNumber] = true
		m := decode(t, d.Payload)
		props := map[string]any{}
		for _, e := range m.ApplicationProperties {
			props[e.Key.(string)] = e.Value
		}
		rule, _ := props["RuleName"].(string)
		delete(props, "RuleName")
		got = append(got, copyInfo{Rule: rule, Subject: m.Properties.Subject, Props: props})
	}
	sort.Slice(got, func(i, j int) bool { return got[i].Rule < got[j].Rule })
	want := []copyInfo{
		{Rule: "", Subject: "s", Props: map[string]any{"color": "red", "quantity": int64(10)}},
		{Rule: "half", Subject: "halved", Props: map[string]any{"quantity": int64(5)}},
		{Rule: "tag", Subject: "s", Props: map[string]any{"color": "red", "quantity": int64(10), "tagged": true}},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("copies mismatch (-want +got):\n%s", diff)
	}
	if !seen[seqs[0]] {
		t.Errorf("no copy has the sequence number %d returned by Send", seqs[0])
	}

	// Copies are distinct messages: defer one and get it back by its
	// sequence number.
	var half *Delivery
	for _, d := range ds {
		if m := decode(t, d.Payload); m.Properties.Subject == "halved" {
			half = d
		}
	}
	if err := b.Defer(ctx, addr, half.LockToken, nil); err != nil {
		t.Fatalf("Defer() error: %v", err)
	}
	got2, err := b.ReceiveDeferred(ctx, addr, []int64{half.SequenceNumber}, PeekLock, "")
	if err != nil || len(got2) != 1 || decode(t, got2[0].Payload).Properties.Subject != "halved" {
		t.Errorf("ReceiveDeferred(%d) = %d messages, %v; want the halved copy", half.SequenceNumber, len(got2), err)
	}
}

func TestRuleErrorsDeadLetter(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	newTopic(t, b, "t", map[string][]Rule{
		"filter": {sqlRule("bad", "sys.Nope = 1", "")},
		"action": {sqlRule("bad", "1=1", "SET sys.SequenceNumber = 1")},
	})
	props := DefaultProperties(KindSubscription)
	props.DeadLetteringOnFilterEvaluationExceptions = false
	if _, err := b.CreateSubscription(ctx, "t", "quiet", props); err != nil {
		t.Fatal(err)
	}
	if err := b.SetRules(ctx, SubscriptionPath("t", "quiet"), []Rule{sqlRule("bad", "sys.Nope = 1", "")}); err != nil {
		t.Fatal(err)
	}
	send(t, b, "t", testMsg{body: "m"})

	for sub, reason := range map[string]string{"filter": ReasonFilterEvaluation, "action": ReasonRuleAction} {
		path := SubscriptionPath("t", sub)
		if got := drainBodies(t, b, path); got != nil {
			t.Errorf("%s: active messages %v, want none", sub, got)
		}
		ds := drain(t, b, path+"/$DeadLetterQueue")
		if len(ds) != 1 {
			t.Fatalf("%s: %d dead letters, want 1", sub, len(ds))
		}
		if got, _ := decode(t, ds[0].Payload).ApplicationProperties.Get("DeadLetterReason"); got != reason {
			t.Errorf("%s: DeadLetterReason = %v, want %s", sub, got, reason)
		}
	}
	if got := drain(t, b, SubscriptionPath("t", "quiet")+"/$DeadLetterQueue"); len(got) != 0 {
		t.Errorf("quiet: %d dead letters, want none without DeadLetteringOnFilterEvaluationExceptions", len(got))
	}
}

func TestRuleManagement(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	newTopic(t, b, "t", map[string][]Rule{"s": nil})
	path := SubscriptionPath("t", "s")
	names := func() []string {
		rules, err := b.Rules(path)
		if err != nil {
			t.Fatalf("Rules() error: %v", err)
		}
		var out []string
		for _, r := range rules {
			out = append(out, r.Name)
		}
		return out
	}
	if diff := cmp.Diff([]string{DefaultRuleName}, names()); diff != "" {
		t.Errorf("new subscription rules mismatch (-want +got):\n%s", diff)
	}
	if err := b.AddRule(ctx, path, sqlRule("red", "color = 'red'", "")); err != nil {
		t.Fatalf("AddRule() error: %v", err)
	}
	if err := b.AddRule(ctx, path, sqlRule("RED", "1=1", "")); !errors.Is(err, ErrEntityExists) {
		t.Errorf("AddRule(duplicate) error = %v, want ErrEntityExists", err)
	}
	if err := b.AddRule(ctx, path, sqlRule("bad", "color =", "")); !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("AddRule(bad syntax) error = %v, want ErrInvalidOperation", err)
	}
	if err := b.DeleteRule(ctx, path, DefaultRuleName); err != nil {
		t.Fatalf("DeleteRule() error: %v", err)
	}
	if err := b.DeleteRule(ctx, path, "missing"); !errors.Is(err, ErrRuleNotFound) {
		t.Errorf("DeleteRule(missing) error = %v, want ErrRuleNotFound", err)
	}
	if diff := cmp.Diff([]string{"red"}, names()); diff != "" {
		t.Errorf("rules mismatch (-want +got):\n%s", diff)
	}
	send(t, b, "t", testMsg{body: "blue", props: encoding.Map{{Key: "color", Value: "blue"}}}, testMsg{body: "red", props: encoding.Map{{Key: "color", Value: "red"}}})
	if diff := cmp.Diff([]string{"red"}, drainBodies(t, b, path)); diff != "" {
		t.Errorf("routed messages mismatch (-want +got):\n%s", diff)
	}
}

func TestRulesSurviveRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "osb.db")
	open := func() (*Broker, func()) {
		st, err := store.Open(ctx, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		b, err := New(ctx, st, Options{})
		if err != nil {
			t.Fatal(err)
		}
		return b, func() { b.Close(); st.Close() }
	}
	b, closeB := open()
	newTopic(t, b, "t", map[string][]Rule{"s": {sqlRule("red", "color = 'red'", "SET seen = TRUE")}})
	closeB()

	b, closeB = open()
	defer closeB()
	send(t, b, "t", testMsg{body: "blue", props: encoding.Map{{Key: "color", Value: "blue"}}}, testMsg{body: "red", props: encoding.Map{{Key: "color", Value: "red"}}})
	ds := drain(t, b, SubscriptionPath("t", "s"))
	if len(ds) != 1 {
		t.Fatalf("got %d messages after restart, want the red one", len(ds))
	}
	if v, _ := decode(t, ds[0].Payload).ApplicationProperties.Get("seen"); v != true {
		t.Errorf("seen = %v, want the action applied", v)
	}
}

func newQueue(t *testing.T, b *Broker, name string, edit func(*Properties)) {
	t.Helper()
	props := DefaultProperties(KindQueue)
	if edit != nil {
		edit(&props)
	}
	if _, err := b.CreateQueue(context.Background(), name, props); err != nil {
		t.Fatalf("CreateQueue(%s) error: %v", name, err)
	}
}

func TestForwarding(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "inbox", nil)
	newQueue(t, b, "front", func(p *Properties) { p.ForwardTo = "events" })
	newTopic(t, b, "events", map[string][]Rule{
		"audit": nil,
		"red":   {sqlRule("red", "color = 'red'", "")},
	})
	sub := DefaultProperties(KindSubscription)
	sub.ForwardTo = "inbox"
	if _, err := b.CreateSubscription(ctx, "events", "relay", sub); err != nil {
		t.Fatal(err)
	}

	send(t, b, "front", testMsg{body: "red", props: encoding.Map{{Key: "color", Value: "red"}}}, testMsg{body: "blue"})
	for path, want := range map[string][]string{
		"front":                      nil,
		"events/subscriptions/audit": {"blue", "red"},
		"events/subscriptions/red":   {"red"},
		"inbox":                      {"blue", "red"},
		"front/$DeadLetterQueue":     nil,
		"events/subscriptions/relay/$DeadLetterQueue": nil,
	} {
		if path == "front" {
			addr, _ := b.Resolve(path)
			if _, err := b.Receive(ctx, addr, 1, PeekLock); !errors.Is(err, ErrInvalidOperation) {
				t.Errorf("Receive(%s) error = %v, want ErrInvalidOperation for a forwarding queue", path, err)
			}
			continue
		}
		if diff := cmp.Diff(want, drainBodies(t, b, path)); diff != "" {
			t.Errorf("%s mismatch (-want +got):\n%s", path, diff)
		}
	}
}

func TestForwardingFailuresDeadLetter(t *testing.T) {
	b, _ := newTestBroker(t)
	newQueue(t, b, "loop-a", func(p *Properties) { p.ForwardTo = "loop-b" })
	newQueue(t, b, "loop-b", func(p *Properties) { p.ForwardTo = "loop-a" })
	newQueue(t, b, "nowhere", func(p *Properties) { p.ForwardTo = "missing" })
	newQueue(t, b, "sessions", func(p *Properties) { p.RequiresSession = true })
	newQueue(t, b, "to-sessions", func(p *Properties) { p.ForwardTo = "sessions" })
	newQueue(t, b, "off", func(p *Properties) { p.Status = StatusDisabled })
	newQueue(t, b, "to-off", func(p *Properties) { p.ForwardTo = "off" })

	send(t, b, "loop-a", testMsg{body: "looping"})
	send(t, b, "nowhere", testMsg{body: "lost"})
	send(t, b, "to-sessions", testMsg{body: "no-session"}, testMsg{body: "with-session", session: "s1"})
	send(t, b, "to-off", testMsg{body: "disabled"})

	// The reasons the documentation lists: too many hops and a missing
	// session id have their own; the rest use MessageForwardingFailed.
	reasons := map[string]string{
		"loop-a/$DeadLetterQueue":      ReasonMaxTransferHopExceeded,
		"to-sessions/$DeadLetterQueue": ReasonSessionIDNull,
	}
	for path, want := range map[string][]string{
		// After four hops (a→b→a→b→a), the fifth forward exceeds the
		// limit and fails on loop-a.
		"loop-a/$DeadLetterQueue":      {"looping"},
		"loop-b/$DeadLetterQueue":      nil,
		"nowhere/$DeadLetterQueue":     {"lost"},
		"to-sessions/$DeadLetterQueue": {"no-session"},
		"to-off/$DeadLetterQueue":      {"disabled"},
	} {
		ds := drain(t, b, path)
		var got []string
		for _, d := range ds {
			m := decode(t, d.Payload)
			got = append(got, string(m.Data[0]))
			wantReason := reasons[path]
			if wantReason == "" {
				wantReason = ReasonForwardFailed
			}
			if r, _ := m.ApplicationProperties.Get("DeadLetterReason"); r != wantReason {
				t.Errorf("%s: DeadLetterReason = %v, want %s", path, r, wantReason)
			}
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%s mismatch (-want +got):\n%s", path, diff)
		}
	}
	ctx := context.Background()
	addr, _ := b.Resolve("sessions")
	lock, err := b.AcceptSession(ctx, addr, "s1")
	if err != nil {
		t.Fatalf("AcceptSession() error: %v", err)
	}
	if ds, err := b.ReceiveSession(ctx, lock, 10, ReceiveAndDelete); err != nil || len(ds) != 1 {
		t.Errorf("ReceiveSession() = %d, %v; want the forwarded session message", len(ds), err)
	}
}

func TestForwardDeadLetteredMessages(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "poison", nil)
	newQueue(t, b, "work", func(p *Properties) {
		p.MaxDeliveryCount = 1
		p.ForwardDeadLetteredMessagesTo = "poison"
	})
	send(t, b, "work", testMsg{body: "a"}, testMsg{body: "b"})
	addr, _ := b.Resolve("work")
	ds, err := b.Receive(ctx, addr, 2, PeekLock)
	if err != nil || len(ds) != 2 {
		t.Fatalf("Receive() = %d, %v", len(ds), err)
	}
	// One explicit dead-letter, one past MaxDeliveryCount.
	if err := b.DeadLetter(ctx, addr, ds[0].LockToken, "Custom", "explicit", nil); err != nil {
		t.Fatalf("DeadLetter() error: %v", err)
	}
	if err := b.Abandon(ctx, addr, ds[1].LockToken, nil); err != nil {
		t.Fatalf("Abandon() error: %v", err)
	}
	if got, _ := b.Receive(ctx, addr, 1, PeekLock); len(got) != 0 {
		t.Fatalf("Receive() after max deliveries got %d, want none", len(got))
	}

	got := drain(t, b, "poison")
	reasons := map[string]any{}
	for _, d := range got {
		m := decode(t, d.Payload)
		reasons[string(m.Data[0])], _ = m.ApplicationProperties.Get("DeadLetterReason")
	}
	if diff := cmp.Diff(map[string]any{"a": "Custom", "b": ReasonMaxDeliveryCount}, reasons); diff != "" {
		t.Errorf("forwarded dead letters mismatch (-want +got):\n%s", diff)
	}
	if left := drain(t, b, "work/$DeadLetterQueue"); len(left) != 0 {
		t.Errorf("%d messages left in work's dead-letter queue, want them forwarded", len(left))
	}
}

func TestSessionAndForwardAreExclusive(t *testing.T) {
	b, _ := newTestBroker(t)
	props := DefaultProperties(KindQueue)
	props.RequiresSession = true
	props.ForwardTo = "x"
	if _, err := b.CreateQueue(context.Background(), "q", props); !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("CreateQueue(sessions and ForwardTo) error = %v, want ErrInvalidOperation", err)
	}
}

// TestNoDeadlockBetweenRulesAndRouting changes rules while messages are
// routed and dead-lettered, which reads the routing snapshot inside store
// transactions.
func TestNoDeadlockBetweenRulesAndRouting(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	newQueue(t, b, "poison", nil)
	newTopic(t, b, "t", nil)
	props := DefaultProperties(KindSubscription)
	props.MaxDeliveryCount = 1
	props.ForwardDeadLetteredMessagesTo = "poison"
	if _, err := b.CreateSubscription(ctx, "t", "s", props); err != nil {
		t.Fatal(err)
	}
	path := SubscriptionPath("t", "s")
	addr, _ := b.Resolve(path)

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 50 {
			name := fmt.Sprintf("r%d", i)
			if err := b.AddRule(ctx, path, sqlRule(name, "n >= 0", "SET rule = 'x'")); err != nil {
				t.Errorf("AddRule() error: %v", err)
			}
			if err := b.DeleteRule(ctx, path, name); err != nil {
				t.Errorf("DeleteRule() error: %v", err)
			}
		}
	})
	wg.Go(func() {
		for i := range 50 {
			if _, err := b.Send(ctx, "t", []IncomingMessage{testMsg{body: fmt.Sprint(i), props: encoding.Map{{Key: "n", Value: int64(i)}}}.incoming(t)}); err != nil {
				t.Errorf("Send() error: %v", err)
			}
		}
	})
	wg.Go(func() {
		for range 50 {
			ds, err := b.Receive(ctx, addr, 5, PeekLock)
			if err != nil {
				t.Errorf("Receive() error: %v", err)
				return
			}
			for _, d := range ds {
				if err := b.Abandon(ctx, addr, d.LockToken, nil); err != nil {
					t.Errorf("Abandon() error: %v", err)
				}
			}
		}
	})
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("deadlock: operations did not finish")
	}
}

// TestForwardExpiredMessages covers dead-letter forwarding from the timer
// loop, which enqueues inside its own transaction.
func TestForwardExpiredMessages(t *testing.T) {
	b, clock := newTestBroker(t)
	newQueue(t, b, "expired", nil)
	newQueue(t, b, "short", func(p *Properties) {
		p.DefaultMessageTimeToLive = time.Second
		p.DeadLetteringOnMessageExpiration = true
		p.ForwardDeadLetteredMessagesTo = "expired"
	})
	send(t, b, "short", testMsg{body: "old"})
	clock.Advance(2 * time.Second)
	b.wakeTimers()

	deadline := time.Now().Add(5 * time.Second)
	for {
		ds := drain(t, b, "expired")
		if len(ds) == 1 {
			if r, _ := decode(t, ds[0].Payload).ApplicationProperties.Get("DeadLetterReason"); r != ReasonTTLExpired {
				t.Errorf("DeadLetterReason = %v, want %s", r, ReasonTTLExpired)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the expired message was not forwarded")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if left := drain(t, b, "short/$DeadLetterQueue"); len(left) != 0 {
		t.Errorf("%d messages left in the source dead-letter queue, want none", len(left))
	}
}

func TestForwardPath(t *testing.T) {
	for in, want := range map[string]string{
		"inbox":                     "inbox",
		"/inbox":                    "inbox",
		"sb://localhost:5672/inbox": "inbox",
		"https://ns.servicebus.windows.net/t/sub": "t/sub",
		"sb://host": "",
	} {
		if got := ForwardPath(in); got != want {
			t.Errorf("ForwardPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestUpdateRuleAndParameters(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "inbox", nil)
	newTopic(t, b, "t", map[string][]Rule{"s": {{Name: "color", Filter: RuleFilter{Kind: FilterSQL, SQL: "color = @c", Parameters: filter.Values{"@c": "red"}}}}})
	path := SubscriptionPath("t", "s")
	send(t, b, "t", testMsg{body: "red", props: encoding.Map{{Key: "color", Value: "red"}}}, testMsg{body: "blue", props: encoding.Map{{Key: "color", Value: "blue"}}})
	if diff := cmp.Diff([]string{"red"}, drainBodies(t, b, path)); diff != "" {
		t.Errorf("before update mismatch (-want +got):\n%s", diff)
	}
	err := b.UpdateRule(ctx, path, Rule{Name: "COLOR", Filter: RuleFilter{Kind: FilterSQL, SQL: "color = @c", Parameters: filter.Values{"@c": "blue"}},
		Action: "SET tag = @t", ActionParameters: filter.Values{"@t": int64(1)}})
	if err != nil {
		t.Fatalf("UpdateRule() error: %v", err)
	}
	if err := b.UpdateRule(ctx, path, TrueRule("missing")); !errors.Is(err, ErrRuleNotFound) {
		t.Errorf("UpdateRule(missing) error = %v, want ErrRuleNotFound", err)
	}
	rules, _ := b.Rules(path)
	if len(rules) != 1 || rules[0].Name != "color" {
		t.Fatalf("Rules() = %+v, want the renamed-free updated rule", rules)
	}
	send(t, b, "t", testMsg{body: "red", props: encoding.Map{{Key: "color", Value: "red"}}}, testMsg{body: "blue", props: encoding.Map{{Key: "color", Value: "blue"}}})
	ds := drain(t, b, path)
	if len(ds) != 1 {
		t.Fatalf("got %d messages after update, want blue only", len(ds))
	}
	if v, _ := decode(t, ds[0].Payload).ApplicationProperties.Get("tag"); v != int64(1) {
		t.Errorf("tag = %v, want the action parameter 1", v)
	}

	// ForwardTo may be an absolute URL, as the JavaScript SDK sends it.
	props := DefaultProperties(KindQueue)
	props.ForwardTo = "sb://localhost:5672/inbox"
	if _, err := b.CreateQueue(ctx, "front", props); err != nil {
		t.Fatal(err)
	}
	send(t, b, "front", testMsg{body: "x"})
	if diff := cmp.Diff([]string{"x"}, drainBodies(t, b, "inbox")); diff != "" {
		t.Errorf("forward by URL mismatch (-want +got):\n%s", diff)
	}
}

func TestTestRules(t *testing.T) {
	rules := []Rule{
		TrueRule("all"),
		{Name: "big", Filter: RuleFilter{Kind: FilterSQL, SQL: "quantity > 10 AND sys.Label LIKE 'order.%'"},
			Action: "SET region = 'eu'; REMOVE debug"},
		{Name: "eu", Filter: RuleFilter{Kind: FilterCorrelation, Correlation: &filter.CorrelationFilter{Subject: "order.created"}}},
		{Name: "none", Filter: RuleFilter{Kind: FilterFalse}},
		{Name: "bad", Filter: RuleFilter{Kind: FilterSQL, SQL: "quantity >"}},
	}
	msg := SampleMessage{
		System: map[string]any{"Label": "order.created"},
		User:   map[string]any{"Quantity": int64(20), "debug": true},
	}
	got := TestRules(rules, msg)
	want := []bool{true, true, true, false, false}
	for i, w := range want {
		if got[i].Matched != w {
			t.Errorf("rule %s matched = %v, want %v", got[i].Name, got[i].Matched, w)
		}
	}
	if after := got[1].After; after.User["region"] != "eu" || len(after.User) != 2 {
		t.Errorf("action result = %v, want region set and debug removed", after.User)
	}
	if _, ok := msg.User["debug"]; !ok {
		t.Error("TestRules changed the input message")
	}
	if got[4].Error == "" || got[3].Error != "" {
		t.Errorf("errors = %q (bad) and %q (none)", got[4].Error, got[3].Error)
	}
	// A type error is reported, not matched.
	res := TestRules([]Rule{{Name: "t", Filter: RuleFilter{Kind: FilterSQL, SQL: "sys.Label > 3"}}}, msg)
	if res[0].Matched {
		t.Errorf("mismatched types matched: %+v", res[0])
	}
}

func TestSendToForwardingQueueReturnsDestinationSequence(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "dest", nil)
	newQueue(t, b, "front", func(p *Properties) { p.ForwardTo = "dest" })
	// Two messages go straight to dest first, so the numbers differ from a
	// fresh queue's.
	send(t, b, "dest", testMsg{body: "a"}, testMsg{body: "b"})
	seqs := send(t, b, "front", testMsg{body: "c"})
	if seqs[0] != 3 {
		t.Fatalf("Send(front) sequence = %d, want 3, the number on dest", seqs[0])
	}
	// A scheduled send to the forwarding queue can be cancelled by that
	// number, naming the source entity.
	later := testMsg{body: "later"}.incoming(t)
	later.ScheduledEnqueueTime = clock.Now().Add(time.Hour)
	scheduled, err := b.Send(ctx, "front", []IncomingMessage{later})
	if err != nil || scheduled[0] == 0 {
		t.Fatalf("scheduled Send = %v, %v; want a sequence number", scheduled, err)
	}
	front, _ := b.Resolve("front")
	if err := b.CancelScheduled(ctx, front, scheduled); err != nil {
		t.Fatalf("CancelScheduled() error: %v", err)
	}
	clock.Advance(2 * time.Hour)
	if got := drainBodies(t, b, "dest"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("dest after the cancellation = %v, want a, b, c without the cancelled one", got)
	}
}

func TestCancelScheduledCancelsActionCopies(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	newTopic(t, b, "t", map[string][]Rule{
		"plain": nil,
		"acting": {
			sqlRule("tag-a", "1 = 1", "SET tag = 'a'"),
			sqlRule("tag-b", "1 = 1", "SET tag = 'b'"),
		},
	})
	later := testMsg{body: "later"}.incoming(t)
	later.ScheduledEnqueueTime = clock.Now().Add(time.Hour)
	seqs, err := b.Send(ctx, "t", []IncomingMessage{later})
	if err != nil {
		t.Fatal(err)
	}
	// Plain copy, plus two action copies with sequence numbers of their own.
	acting, _ := b.Resolve("t/subscriptions/acting")
	peek, err := b.Peek(ctx, acting, 0, 10, "")
	if err != nil || len(peek) != 2 {
		t.Fatalf("Peek(acting) = %d, %v; want the two action copies", len(peek), err)
	}
	topic, _ := b.Resolve("t")
	if err := b.CancelScheduled(ctx, topic, seqs); err != nil {
		t.Fatalf("CancelScheduled() error: %v", err)
	}
	clock.Advance(2 * time.Hour)
	for _, path := range []string{"t/subscriptions/plain", "t/subscriptions/acting"} {
		if got := drainBodies(t, b, path); len(got) != 0 {
			t.Errorf("%s still holds %v after the cancellation", path, got)
		}
	}
}

func TestTransferDeadLetterQueue(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "q", nil)
	newTopic(t, b, "t", map[string][]Rule{"s": nil})
	for _, path := range []string{"q", "t/subscriptions/s"} {
		seqs, err := b.TransferDeadLetter(ctx, path, []IncomingMessage{testMsg{body: "stuck"}.incoming(t)}, "TransferFailed", "destination is disabled")
		if err != nil || len(seqs) != 1 || seqs[0] == 0 {
			t.Fatalf("TransferDeadLetter(%s) = %v, %v", path, seqs, err)
		}
		counts, err := b.Counts(ctx, path)
		if err != nil || counts.TransferDeadLetter != 1 || counts.DeadLetter != 0 || counts.Active != 0 {
			t.Errorf("counts of %s = %+v, %v; want one transfer dead-letter message", path, counts, err)
		}
		ds := drain(t, b, path+"/$Transfer/$DeadLetterQueue")
		if len(ds) != 1 {
			t.Fatalf("receive from %s TDLQ = %d messages, want 1", path, len(ds))
		}
		m := decode(t, ds[0].Payload)
		if r, _ := m.ApplicationProperties.Get("DeadLetterReason"); r != "TransferFailed" {
			t.Errorf("reason = %v", r)
		}
		if got := drain(t, b, path+"/$DeadLetterQueue"); len(got) != 0 {
			t.Errorf("the regular DLQ of %s holds %d messages", path, len(got))
		}
	}
	if _, err := b.TransferDeadLetter(ctx, "t", nil, "x", "y"); err == nil {
		t.Error("TransferDeadLetter on a topic succeeded")
	}
}
