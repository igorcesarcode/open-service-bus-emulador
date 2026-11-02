package e2e_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/test/e2e/spec"
)

const orderCount = 24

// TestOrderPipeline runs the order pipeline with every combination of
// producer and worker languages. The producer publishes orders to a topic;
// two competing workers take customer sessions from a subscription, retry
// flaky orders, let poison orders go to the dead-letter queue and publish
// results to a queue. A second subscription audits every order.
func TestOrderPipeline(t *testing.T) {
	tests := []struct {
		producer string
		workers  []string
	}{
		{"go", []string{"go", "go"}},
		{"js", []string{"js", "js"}},
		{"go", []string{"js", "js"}},
		{"js", []string{"go", "go"}},
		{"go", []string{"go", "js"}},
		{"js", []string{"go", "js"}},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_to_%s", tt.producer, strings.Join(tt.workers, "+")), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := timeoutContext(2 * time.Minute)
			defer cancel()
			conn := startEmulator(t)

			sent := runService(ctx, t, conn, tt.producer, "produce", "-count", fmt.Sprint(orderCount))
			if len(sent) != orderCount {
				t.Fatalf("producer sent %d orders, want %d", len(sent), orderCount)
			}
			var workers []*service
			for i, lang := range tt.workers {
				workers = append(workers, startService(ctx, t, conn, lang, "work", "-name", fmt.Sprintf("w%d-%s", i, lang), "-idle", "3s"))
			}
			waitAll(t, workers...)

			client := newClient(t, conn)
			checkResults(ctx, t, client, tt.workers)
			checkDeadLetters(ctx, t, client)
			checkAudit(ctx, t, client)
		})
	}
}

func receiveAndDelete(t *testing.T, newReceiver func(*azservicebus.ReceiverOptions) (*azservicebus.Receiver, error)) *azservicebus.Receiver {
	t.Helper()
	r, err := newReceiver(&azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	if err != nil {
		t.Fatalf("creating receiver: %v", err)
	}
	t.Cleanup(func() { r.Close(context.Background()) })
	return r
}

// checkResults checks every order except the poison ones was processed
// once, in order within its customer, with its body and properties intact.
func checkResults(ctx context.Context, t *testing.T, client *azservicebus.Client, langs []string) {
	t.Helper()
	orders := map[string]spec.Order{}
	want := 0
	for _, o := range spec.Orders(orderCount) {
		orders[o.ID] = o
		if o.Kind != spec.KindPoison {
			want++
		}
	}
	r := receiveAndDelete(t, func(o *azservicebus.ReceiverOptions) (*azservicebus.Receiver, error) {
		return client.NewReceiverForQueue(spec.ResultsQueue, o)
	})
	lastSeq := map[string]int{}
	seen := map[string]bool{}
	for _, m := range drain(ctx, t, r, want) {
		var res spec.Result
		if err := json.Unmarshal(m.Body, &res); err != nil {
			t.Errorf("result %s: invalid body %q: %v", m.MessageID, m.Body, err)
			continue
		}
		o, ok := orders[res.ID]
		if !ok || o.Kind == spec.KindPoison {
			t.Errorf("result for unexpected order %q", res.ID)
			continue
		}
		if seen[o.ID] {
			t.Errorf("order %s processed twice", o.ID)
		}
		seen[o.ID] = true
		if last, ok := lastSeq[o.Customer]; ok && res.Seq <= last {
			t.Errorf("customer %s: order seq %d processed after seq %d", o.Customer, res.Seq, last)
		}
		lastSeq[o.Customer] = res.Seq

		if got := deref(m.CorrelationID); got != o.ID {
			t.Errorf("result %s: CorrelationID = %q, want the order id", o.ID, got)
		}
		if got := deref(m.Subject); got != "order.processed" {
			t.Errorf("result %s: Subject = %q, want order.processed", o.ID, got)
		}
		if got := m.ApplicationProperties["lang"]; got != res.Lang || !slices.Contains(langs, res.Lang) {
			t.Errorf("result %s: lang property %v, body %q, want one of %v", o.ID, got, res.Lang, langs)
		}
		wantAttempt := 1
		if o.Kind == spec.KindFlaky {
			wantAttempt = 2
		}
		want := spec.Result{
			ID:       o.ID,
			Customer: o.Customer,
			Seq:      o.Seq,
			Attempt:  wantAttempt,
			Worker:   res.Worker,
			Lang:     res.Lang,
			Body:     spec.Body{ID: o.ID, Customer: o.Customer, Seq: o.Seq, Quantity: o.Quantity, Amount: o.Amount},
			Props: spec.ResultProps{
				Customer:  o.Customer,
				Quantity:  o.Quantity,
				Amount:    o.Amount,
				Express:   o.Express,
				Kind:      o.Kind,
				CreatedAt: spec.FormatTime(o.CreatedAt),
			},
			Subject: "order.created",
		}
		if diff := cmp.Diff(want, res); diff != "" {
			t.Errorf("result %s mismatch (-want +got):\n%s", o.ID, diff)
		}
	}
}

// checkDeadLetters checks the poison orders, and only them, reached the
// fulfillment dead-letter queue after MaxDeliveryCount deliveries.
func checkDeadLetters(ctx context.Context, t *testing.T, client *azservicebus.Client) {
	t.Helper()
	var want []string
	for _, o := range spec.Orders(orderCount) {
		if o.Kind == spec.KindPoison {
			want = append(want, o.ID)
		}
	}
	r := receiveAndDelete(t, func(o *azservicebus.ReceiverOptions) (*azservicebus.Receiver, error) {
		o.SubQueue = azservicebus.SubQueueDeadLetter
		return client.NewReceiverForSubscription(spec.OrdersTopic, spec.FulfillmentSub, o)
	})
	var got []string
	for _, m := range drain(ctx, t, r, len(want)) {
		got = append(got, m.MessageID)
		if reason := deref(m.DeadLetterReason); reason != "MaxDeliveryCountExceeded" {
			t.Errorf("dead letter %s: reason %q, want MaxDeliveryCountExceeded", m.MessageID, reason)
		}
		if m.DeliveryCount < spec.MaxDeliveryCount {
			t.Errorf("dead letter %s: DeliveryCount = %d, want at least %d", m.MessageID, m.DeliveryCount, spec.MaxDeliveryCount)
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("dead-lettered orders mismatch (-want +got):\n%s", diff)
	}
}

// checkAudit checks, from Go, how the producer encoded every order: system
// properties, the JSON body and the types of the application properties.
func checkAudit(ctx context.Context, t *testing.T, client *azservicebus.Client) {
	t.Helper()
	orders := map[string]spec.Order{}
	for _, o := range spec.Orders(orderCount) {
		orders[o.ID] = o
	}
	r := receiveAndDelete(t, func(o *azservicebus.ReceiverOptions) (*azservicebus.Receiver, error) {
		return client.NewReceiverForSubscription(spec.OrdersTopic, spec.AuditSub, o)
	})
	for _, m := range drain(ctx, t, r, orderCount) {
		o, ok := orders[m.MessageID]
		if !ok {
			t.Errorf("audit: unexpected message %q", m.MessageID)
			continue
		}
		if got := deref(m.SessionID); got != o.Customer {
			t.Errorf("audit %s: SessionID = %q, want %q", o.ID, got, o.Customer)
		}
		if got := deref(m.ContentType); got != "application/json" {
			t.Errorf("audit %s: ContentType = %q, want application/json", o.ID, got)
		}
		if got := deref(m.Subject); got != "order.created" {
			t.Errorf("audit %s: Subject = %q, want order.created", o.ID, got)
		}
		var body spec.Body
		if err := json.Unmarshal(m.Body, &body); err != nil {
			t.Errorf("audit %s: body %q is not JSON: %v", o.ID, m.Body, err)
		} else if want := (spec.Body{ID: o.ID, Customer: o.Customer, Seq: o.Seq, Quantity: o.Quantity, Amount: o.Amount}); body != want {
			t.Errorf("audit %s: body = %+v, want %+v", o.ID, body, want)
		}
		p := m.ApplicationProperties
		checkProp(t, o.ID, p, "customer", o.Customer)
		checkProp(t, o.ID, p, "kind", o.Kind)
		checkProp(t, o.ID, p, "express", o.Express)
		checkProp(t, o.ID, p, "amount", o.Amount)
		checkProp(t, o.ID, p, "createdAt", o.CreatedAt)
		if q, ok := spec.Int64(p["quantity"]); !ok || q != o.Quantity {
			t.Errorf("audit %s: quantity = %T(%v), want the integer %d", o.ID, p["quantity"], p["quantity"], o.Quantity)
		}
	}
}

func checkProp[T comparable](t *testing.T, id string, props map[string]any, key string, want T) {
	t.Helper()
	got, ok := props[key].(T)
	if !ok {
		t.Errorf("audit %s: %s is %T(%v), want %T", id, key, props[key], props[key], want)
		return
	}
	if tm, ok := any(got).(time.Time); ok {
		if !tm.Equal(any(want).(time.Time)) {
			t.Errorf("audit %s: %s = %v, want %v", id, key, got, want)
		}
		return
	}
	if got != want {
		t.Errorf("audit %s: %s = %v, want %v", id, key, got, want)
	}
}

const requestCount = 6

// TestRequestReply sends pricing requests from one language to a service
// in another. Replies go to a session queue, to the session the requester
// named in ReplyToSessionId.
func TestRequestReply(t *testing.T) {
	for _, requester := range []string{"go", "js"} {
		for _, responder := range []string{"go", "js"} {
			t.Run(fmt.Sprintf("%s_to_%s", requester, responder), func(t *testing.T) {
				t.Parallel()
				ctx, cancel := timeoutContext(time.Minute)
				defer cancel()
				conn := startEmulator(t)

				resp := startService(ctx, t, conn, responder, "respond", "-idle", "3s")
				replies := runService(ctx, t, conn, requester, "request", "-count", fmt.Sprint(requestCount))
				if got := len(resp.wait(t)); got != requestCount {
					t.Errorf("responder answered %d requests, want %d", got, requestCount)
				}
				checkReplies(t, replies, requester, responder)
			})
		}
	}
}

func checkReplies(t *testing.T, replies []spec.Event, requester, responder string) {
	t.Helper()
	if len(replies) != requestCount {
		t.Fatalf("requester got %d replies, want %d: %+v", len(replies), requestCount, replies)
	}
	seen := map[string]bool{}
	for _, r := range replies {
		var i int
		if _, err := fmt.Sscanf(r.CorrelationID, "req-%d", &i); err != nil || i < 0 || i >= requestCount || seen[r.CorrelationID] {
			t.Errorf("reply with unexpected CorrelationID %q", r.CorrelationID)
			continue
		}
		seen[r.CorrelationID] = true
		if !strings.HasPrefix(r.SessionID, requester+"-") || r.SessionID != replies[0].SessionID {
			t.Errorf("reply %s: SessionID = %q, want the requester's session", r.CorrelationID, r.SessionID)
		}
		want := spec.Event{Event: "reply", CorrelationID: r.CorrelationID, SessionID: r.SessionID, Subject: "price.quoted"}
		if spec.IsBinaryRequest(i) {
			body := spec.BinaryRequest(i)
			slices.Reverse(body)
			want.ContentType = "application/octet-stream"
			want.BodyHex = hex.EncodeToString(body)
		} else {
			want.ContentType = "application/json"
			want.Body = &spec.PriceReply{SKU: fmt.Sprintf("sku-%d", i), Qty: i + 1, Total: float64(i+1) * spec.UnitPrice, Responder: responder}
		}
		if diff := cmp.Diff(want, r); diff != "" {
			t.Errorf("reply %s mismatch (-want +got):\n%s", r.CorrelationID, diff)
		}
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

const eventCount = 12

// TestEventRouting publishes events to a topic whose subscriptions have
// rules from Config.json: SQL filters shaped like real-world ones, a
// correlation filter, an action rule and a subscription that forwards to a
// queue. A consumer in each language reports where every event arrived.
// It also dead-letters jobs on a queue that forwards dead letters.
func TestEventRouting(t *testing.T) {
	for _, producer := range []string{"go", "js"} {
		for _, consumer := range []string{"go", "js"} {
			t.Run(fmt.Sprintf("%s_to_%s", producer, consumer), func(t *testing.T) {
				t.Parallel()
				ctx, cancel := timeoutContext(time.Minute)
				defer cancel()
				conn := startEmulator(t)

				runService(ctx, t, conn, producer, "publish-events", "-count", fmt.Sprint(eventCount))
				events := runService(ctx, t, conn, consumer, "consume-events", "-idle", "1s")

				want := map[string][]string{}
				for _, e := range spec.RoutedEvents(eventCount) {
					for _, entity := range e.Routes() {
						want[entity] = append(want[entity], e.ID)
					}
				}
				got := map[string][]string{}
				var rejected []spec.Event
				for _, e := range events {
					switch e.Event {
					case "routed":
						got[e.Entity] = append(got[e.Entity], e.ID)
						checkRouted(t, e)
					case "rejected":
						rejected = append(rejected, e)
					}
				}
				for _, ids := range got {
					slices.Sort(ids)
				}
				for _, ids := range want {
					slices.Sort(ids)
				}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("routing mismatch (-want +got):\n%s", diff)
				}
				if len(rejected) != spec.JobCount {
					t.Errorf("consumer rejected %d jobs, want %d", len(rejected), spec.JobCount)
				}
				checkPoison(ctx, t, newClient(t, conn), spec.JobRejectedPrefix+consumer)
			})
		}
	}
}

// checkRouted checks the action of the price rule, which multiplies a
// property that the JavaScript SDK sends as uint.
func checkRouted(t *testing.T, e spec.Event) {
	t.Helper()
	if e.Entity != "priced" {
		if e.RuleName != "" || e.Priced != nil {
			t.Errorf("%s in %s: has RuleName %q and priced %v, want no action", e.ID, e.Entity, e.RuleName, e.Priced)
		}
		return
	}
	var i int64
	fmt.Sscanf(e.ID, "event-%d", &i)
	want := spec.RoutedEvents(eventCount)[i].Quantity * 2
	if e.RuleName != spec.PricedRule || e.Priced == nil || !*e.Priced || e.Total == nil || *e.Total != want {
		t.Errorf("%s in priced: RuleName %q, priced %v, total %v; want %s, true, %d", e.ID, e.RuleName, e.Priced, e.Total, spec.PricedRule, want)
	}
}

// checkPoison checks the jobs dead-lettered by the consumer were forwarded
// to the poison queue with their reason.
func checkPoison(ctx context.Context, t *testing.T, client *azservicebus.Client, reason string) {
	t.Helper()
	r := receiveAndDelete(t, func(o *azservicebus.ReceiverOptions) (*azservicebus.Receiver, error) {
		return client.NewReceiverForQueue(spec.JobsPoisonQueue, o)
	})
	for _, m := range drain(ctx, t, r, spec.JobCount) {
		if got := deref(m.DeadLetterReason); got != reason {
			t.Errorf("poison %s: DeadLetterReason = %q, want %q", m.MessageID, got, reason)
		}
	}
	dlq := receiveAndDelete(t, func(o *azservicebus.ReceiverOptions) (*azservicebus.Receiver, error) {
		o.SubQueue = azservicebus.SubQueueDeadLetter
		return client.NewReceiverForQueue(spec.JobsQueue, o)
	})
	if left, err := dlq.PeekMessages(ctx, 10, nil); err != nil || len(left) > 0 {
		t.Errorf("jobs dead-letter queue has %d messages, %v; want them forwarded", len(left), err)
	}
}

// TestAdminTrustsExportedCertificate runs the Go administration client
// with its default transport against the binary. It trusts the emulator
// through the certificate written by -tls-cert-out, as a service would.
// SSL_CERT_FILE is how Go on Linux reads extra roots; it is not parallel
// because it sets the environment of the service process.
func TestAdminTrustsExportedCertificate(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SSL_CERT_FILE is only read on Linux")
	}
	pemFile := filepath.Join(t.TempDir(), "osb.pem")
	conn := startEmulator(t, "-tls-cert-out", pemFile)
	t.Setenv("SSL_CERT_FILE", pemFile)
	ctx, cancel := timeoutContext(30 * time.Second)
	defer cancel()
	events := runService(ctx, t, conn, "go", "admin-check")
	if len(events) != 1 || events[0].ID != spec.ResultsQueue {
		t.Errorf("admin-check events = %+v, want the %s queue", events, spec.ResultsQueue)
	}
}
