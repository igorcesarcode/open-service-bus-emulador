// Package spec defines the contract shared by the end-to-end mini-services
// and the tests that check them. test/node/e2e/spec.mjs mirrors it for the
// JavaScript services; both must stay in sync.
package spec

import (
	"fmt"
	"math"
	"slices"
	"time"
)

// Entity names, matching testdata/Config.json.
const (
	OrdersTopic         = "orders"
	FulfillmentSub      = "fulfillment"
	AuditSub            = "audit"
	ResultsQueue        = "results"
	PricingQueue        = "pricing"
	PricingRepliesQueue = "pricing-replies"
)

// MaxDeliveryCount is the fulfillment subscription's limit, after which a
// poison order is dead-lettered.
const MaxDeliveryCount = 3

// Customers is the number of customers, and so of sessions.
const Customers = 4

// Order kinds tell workers how to behave.
const (
	KindNormal = "normal"
	// KindFlaky fails on the first delivery and succeeds on the retry.
	KindFlaky = "flaky"
	// KindPoison always fails and ends in the dead-letter queue.
	KindPoison = "poison"
)

// Order is one order the producer publishes. Its fields travel in the body
// (as JSON) and again as application properties, to check typed values.
type Order struct {
	ID        string    `json:"id"`
	Customer  string    `json:"customer"`
	Seq       int       `json:"seq"`
	Quantity  int64     `json:"quantity"`
	Amount    float64   `json:"amount"`
	Express   bool      `json:"express"`
	Kind      string    `json:"kind"`
	CreatedAt time.Time `json:"-"`
}

// Body is the JSON body of an order message.
type Body struct {
	ID       string  `json:"id"`
	Customer string  `json:"customer"`
	Seq      int     `json:"seq"`
	Quantity int64   `json:"quantity"`
	Amount   float64 `json:"amount"`
}

var createdBase = time.Date(2026, 1, 2, 3, 4, 5, 678_000_000, time.UTC)

// Orders returns the first n orders. Order i belongs to customer i%Customers
// and is that customer's order number i/Customers.
func Orders(n int) []Order {
	out := make([]Order, n)
	for i := range out {
		kind := KindNormal
		switch {
		case i%11 == 5:
			kind = KindPoison
		case i%6 == 1:
			kind = KindFlaky
		}
		out[i] = Order{
			ID:        fmt.Sprintf("order-%d", i),
			Customer:  fmt.Sprintf("customer-%d", i%Customers),
			Seq:       i / Customers,
			Quantity:  int64(i + 1),
			Amount:    float64(i)*10 + 0.25,
			Express:   i%2 == 0,
			Kind:      kind,
			CreatedAt: createdBase.Add(time.Duration(i) * time.Second),
		}
	}
	return out
}

// Result is the JSON body a worker publishes to ResultsQueue for each
// processed order.
type Result struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
	Seq      int    `json:"seq"`
	// Attempt is the delivery count of the processed delivery.
	Attempt int    `json:"attempt"`
	Worker  string `json:"worker"`
	Lang    string `json:"lang"`
	// Body is the order body as the worker decoded it.
	Body Body `json:"body"`
	// Props are the order's application properties as the worker decoded
	// them, with createdAt in FormatTime form.
	Props   ResultProps `json:"props"`
	Subject string      `json:"subject"`
}

// ResultProps are the order's application properties.
type ResultProps struct {
	Customer  string  `json:"customer"`
	Quantity  int64   `json:"quantity"`
	Amount    float64 `json:"amount"`
	Express   bool    `json:"express"`
	Kind      string  `json:"kind"`
	CreatedAt string  `json:"createdAt"`
}

// FormatTime formats t like JavaScript's Date.toISOString.
func FormatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// Int64 converts an integer application property of any AMQP integer type.
// The JavaScript SDK encodes small non-negative numbers as uint, while the
// Go SDK encodes int64 values as long.
func Int64(v any) (int64, bool) {
	switch v := v.(type) {
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), v <= math.MaxInt64
	}
	return 0, false
}

// UnitPrice is what the pricing service charges per unit.
const UnitPrice = 2.5

// PriceRequest is the JSON body of a pricing request. Every third request
// is binary instead; see IsBinaryRequest.
type PriceRequest struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

// PriceReply is the JSON reply to a PriceRequest. Binary requests are
// answered with their bytes reversed.
type PriceReply struct {
	SKU       string  `json:"sku"`
	Qty       int     `json:"qty"`
	Total     float64 `json:"total"`
	Responder string  `json:"responder"`
}

// IsBinaryRequest reports whether request i has a binary body.
func IsBinaryRequest(i int) bool { return i%3 == 2 }

// BinaryRequest is the body of binary request i. It starts with 0xff so it
// is never valid JSON or UTF-8.
func BinaryRequest(i int) []byte { return []byte{0xff, byte(i), 0x00, 0x10, 0x80} }

// Event routing scenario entities, matching testdata/Config.json.
const (
	EventsTopic       = "events"
	EscalationsQueue  = "escalations"
	JobsQueue         = "jobs"
	JobsPoisonQueue   = "jobs-poison"
	PricedRule        = "price"
	JobCount          = 2
	JobRejectedPrefix = "rejected-by-"
)

// RoutedSubscriptions are the subscriptions of EventsTopic that consumers
// drain; the relay subscription forwards to EscalationsQueue instead.
var RoutedSubscriptions = []string{"approved", "bulk", "eu-orders", "priced"}

// RoutedEvent is one event published to EventsTopic. Its fields travel as
// application properties and the subject.
type RoutedEvent struct {
	ID         string
	Subject    string
	Event      string
	IsApproved bool
	Quantity   int64
	Region     string
}

// RoutedEvents returns the first n events.
func RoutedEvents(n int) []RoutedEvent {
	kinds := []string{"completed", "approved", "created", "escalated"}
	out := make([]RoutedEvent, n)
	for i := range out {
		subject := "order.updated"
		if i%3 == 0 {
			subject = "order.created"
		}
		region := "us"
		if i%2 == 0 {
			region = "eu"
		}
		out[i] = RoutedEvent{
			ID:         fmt.Sprintf("event-%d", i),
			Subject:    subject,
			Event:      kinds[i%4],
			IsApproved: i%3 != 0,
			Quantity:   int64(i * 3),
			Region:     region,
		}
	}
	return out
}

// Routes returns where the rules of testdata/Config.json send e: the
// subscriptions in RoutedSubscriptions and EscalationsQueue.
func (e RoutedEvent) Routes() []string {
	var out []string
	if (e.Event == "completed" || e.Event == "approved") && e.IsApproved {
		out = append(out, "approved")
	}
	if e.Quantity > 10 {
		out = append(out, "bulk")
	}
	if e.Subject == "order.created" && e.Region == "eu" {
		out = append(out, "eu-orders")
	}
	out = append(out, "priced")
	if e.Event == "escalated" {
		out = append(out, EscalationsQueue)
	}
	return out
}

// Event is a line a service writes to stdout to report what it did.
type Event struct {
	Event string `json:"event"`
	ID    string `json:"id,omitempty"`
	// Reply fields.
	CorrelationID string `json:"correlationId,omitempty"`
	SessionID     string `json:"sessionId,omitempty"`
	ContentType   string `json:"contentType,omitempty"`
	Subject       string `json:"subject,omitempty"`
	// Reply bodies: JSON for JSON replies, hex for binary ones.
	Body    *PriceReply `json:"body,omitempty"`
	BodyHex string      `json:"bodyHex,omitempty"`
	Attempt int         `json:"attempt,omitempty"`
	// Routing fields: where the message was received and what rule
	// actions added.
	Entity   string `json:"entity,omitempty"`
	RuleName string `json:"ruleName,omitempty"`
	Priced   *bool  `json:"priced,omitempty"`
	Total    *int64 `json:"total,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// Duplicate detection scenario: PaymentsQueue, declared in
// testdata/Config.json, drops messages whose MessageId it has seen.
const PaymentsQueue = "payments"

// PaymentID is the MessageId of payment i. Every producer uses the same
// ids, as a service retrying after a timeout would.
func PaymentID(i int) string { return fmt.Sprintf("payment-%d", i) }

// Terraform scenario entities, declared by testdata/terraform/main.tf.
const (
	AlertsTopic = "alerts"
	// EverythingSub has no rules of its own.
	EverythingSub = "everything"
)

// AlertChannels are the channels of the alerts; each has a subscription
// whose rule accepts only its channel. The test passes them to Terraform
// with -tf-var.
var AlertChannels = []string{"sms", "email"}

// AlertSubscriptions are the subscriptions of AlertsTopic.
var AlertSubscriptions = append(slices.Clone(AlertChannels), EverythingSub)

// Alert is alert i: its channel cycles through AlertChannels and a channel
// without subscription.
func Alert(i int) (id, channel string) {
	channels := append(slices.Clone(AlertChannels), "push")
	return fmt.Sprintf("alert-%d", i), channels[i%len(channels)]
}

// AlertRoutes returns the subscriptions alert channel reaches. As in
// Azure, subscriptions keep $Default (1=1) next to declared rules unless
// the emulator runs with -tf-drop-default-rule.
func AlertRoutes(channel string, dropDefault bool) []string {
	if !dropDefault {
		return AlertSubscriptions
	}
	var out []string
	for _, c := range AlertChannels {
		if c == channel {
			out = append(out, c)
		}
	}
	return append(out, EverythingSub)
}
