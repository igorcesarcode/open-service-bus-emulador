package integration_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// newAdminClient connects the Go administration client, which always uses
// HTTPS on the connection string's port. The emulator's certificate is
// self-signed.
func newAdminClient(t *testing.T, srv *server.Server) *admin.Client {
	t.Helper()
	return newAdminClientFor(t, srv.ConnectionString())
}

func newAdminClientFor(t *testing.T, connStr string) *admin.Client {
	t.Helper()
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	if noEmulatorFlag() {
		var addr string
		connStr, addr = withoutEmulatorFlag(connStr)
		tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}
	}
	transport := &http.Client{Transport: tr}
	client, err := admin.NewClientFromConnectionString(connStr, &admin.ClientOptions{
		ClientOptions: azcore.ClientOptions{Transport: transport},
	})
	if err != nil {
		t.Fatalf("admin.NewClientFromConnectionString() error: %v", err)
	}
	return client
}

func responseStatus(err error) int {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		return re.StatusCode
	}
	return 0
}

func TestAdminQueues(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{})
	ac := newAdminClient(t, srv)
	ctx := testContext(t)

	ns, err := ac.GetNamespaceProperties(ctx, nil)
	if err != nil || ns.Name == "" || ns.SKU == "" {
		t.Fatalf("GetNamespaceProperties() = %+v, %v", ns, err)
	}

	// Values beyond the official emulator's limits.
	props := admin.QueueProperties{
		LockDuration:             new("PT10M"),
		MaxDeliveryCount:         new(int32(500)),
		DefaultMessageTimeToLive: new("P30D"),
		RequiresSession:          new(false),
		UserMetadata:             new("meta <&>"),
	}
	created, err := ac.CreateQueue(ctx, "orders", &admin.CreateQueueOptions{Properties: &props})
	if err != nil {
		t.Fatalf("CreateQueue() error: %v", err)
	}
	if got := *created.LockDuration; got != "PT10M" {
		t.Errorf("created LockDuration = %q, want PT10M", got)
	}
	if _, err := ac.CreateQueue(ctx, "orders", nil); responseStatus(err) != http.StatusConflict {
		t.Errorf("CreateQueue(existing) error = %v, want 409", err)
	}

	got, err := ac.GetQueue(ctx, "orders", nil)
	if err != nil || got == nil {
		t.Fatalf("GetQueue() = %v, %v", got, err)
	}
	if *got.MaxDeliveryCount != 500 || *got.DefaultMessageTimeToLive != "P30D" || *got.UserMetadata != "meta <&>" || *got.Status != admin.EntityStatusActive {
		t.Errorf("GetQueue() properties = %+v", got.QueueProperties)
	}
	if missing, err := ac.GetQueue(ctx, "missing", nil); err != nil || missing != nil {
		t.Errorf("GetQueue(missing) = %v, %v; want nil, nil", missing, err)
	}

	// An update replaces the description, as in Service Bus: what it leaves
	// out goes back to its default.
	updated, err := ac.UpdateQueue(ctx, "orders", admin.QueueProperties{MaxDeliveryCount: new(int32(3))}, nil)
	if err != nil {
		t.Fatalf("UpdateQueue() error: %v", err)
	}
	if *updated.MaxDeliveryCount != 3 || *updated.LockDuration != "PT1M" || *updated.DefaultMessageTimeToLive == "P30D" {
		t.Errorf("UpdateQueue() = MaxDeliveryCount %d LockDuration %s TTL %s, want 3, the default lock and the default TTL",
			*updated.MaxDeliveryCount, *updated.LockDuration, *updated.DefaultMessageTimeToLive)
	}
	// Get, change and update keeps the rest.
	cur, err := ac.GetQueue(ctx, "orders", nil)
	if err != nil || cur == nil {
		t.Fatalf("GetQueue() = %v, %v", cur, err)
	}
	cur.LockDuration = new("PT2M")
	if updated, err = ac.UpdateQueue(ctx, "orders", cur.QueueProperties, nil); err != nil {
		t.Fatalf("UpdateQueue(full) error: %v", err)
	}
	if *updated.LockDuration != "PT2M" || *updated.MaxDeliveryCount != 3 {
		t.Errorf("UpdateQueue(full) = %+v", updated.QueueProperties)
	}
	// RequiresSession, RequiresDuplicateDetection and EnablePartitioning
	// cannot change; leaving them out keeps them.
	for want, edit := range map[string]func(p *admin.QueueProperties){
		"The value for RequiresSession property of an existing Queue can't be changed.":                  func(p *admin.QueueProperties) { p.RequiresSession = new(true) },
		"The value for the 'requiresDuplicateDetection' property of an existing Queue can't be changed.": func(p *admin.QueueProperties) { p.RequiresDuplicateDetection = new(!*cur.RequiresDuplicateDetection) },
		"Partitioning can't be changed for Queue/Topic.":                                                 func(p *admin.QueueProperties) { p.EnablePartitioning = new(true) },
	} {
		p := cur.QueueProperties
		edit(&p)
		_, err := ac.UpdateQueue(ctx, "orders", p, nil)
		if responseStatus(err) != http.StatusBadRequest || !strings.Contains(unescapeXML(err), "Sub code=40000. "+want) {
			t.Errorf("UpdateQueue() error = %v, want 400 with %q", err, want)
		}
	}
	if kept, err := ac.GetQueue(ctx, "orders", nil); err != nil || *kept.RequiresDuplicateDetection != *cur.RequiresDuplicateDetection {
		t.Errorf("after the refused updates: %+v, %v", kept, err)
	}
	if _, err := ac.UpdateQueue(ctx, "orders", admin.QueueProperties{MaxDeliveryCount: new(int32(4))}, nil); err != nil {
		t.Errorf("UpdateQueue() leaving the immutable properties out error: %v", err)
	}
	if kept, _ := ac.GetQueue(ctx, "orders", nil); *kept.RequiresDuplicateDetection != *cur.RequiresDuplicateDetection {
		t.Errorf("RequiresDuplicateDetection = %v after an update that left it out, want %v", *kept.RequiresDuplicateDetection, *cur.RequiresDuplicateDetection)
	}

	// Runtime properties count messages by state.
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")
	send(t, sender, bodies("a", "b", "c")...)
	if _, err := sender.ScheduleMessages(ctx, []*azservicebus.Message{{Body: []byte("later")}}, time.Now().Add(time.Hour), nil); err != nil {
		t.Fatal(err)
	}
	r := newReceiver(t, client, "orders", nil)
	m := receive(t, r, 1)[0]
	if err := r.DeadLetterMessage(ctx, m, nil); err != nil {
		t.Fatal(err)
	}
	rt, err := ac.GetQueueRuntimeProperties(ctx, "orders", nil)
	if err != nil || rt == nil {
		t.Fatalf("GetQueueRuntimeProperties() = %v, %v", rt, err)
	}
	type counts struct{ Total, Active, DeadLetter, Scheduled int64 }
	gotCounts := counts{rt.TotalMessageCount, int64(rt.ActiveMessageCount), int64(rt.DeadLetterMessageCount), int64(rt.ScheduledMessageCount)}
	if diff := cmp.Diff(counts{4, 2, 1, 1}, gotCounts); diff != "" {
		t.Errorf("runtime counts mismatch (-want +got):\n%s", diff)
	}
	if rt.SizeInBytes == 0 || rt.CreatedAt.IsZero() {
		t.Errorf("runtime SizeInBytes %d CreatedAt %v, want set", rt.SizeInBytes, rt.CreatedAt)
	}

	// Listing pages through every queue.
	for _, name := range []string{"a1", "a2", "a3"} {
		if _, err := ac.CreateQueue(ctx, name, nil); err != nil {
			t.Fatal(err)
		}
	}
	var names []string
	pager := ac.NewListQueuesPager(&admin.ListQueuesOptions{MaxPageSize: 2})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("NextPage() error: %v", err)
		}
		for _, q := range page.Queues {
			names = append(names, q.QueueName)
		}
	}
	if diff := cmp.Diff([]string{"a1", "a2", "a3", "orders"}, names); diff != "" {
		t.Errorf("listed queues mismatch (-want +got):\n%s", diff)
	}
	rtPager := ac.NewListQueuesRuntimePropertiesPager(nil)
	listed := 0
	for rtPager.More() {
		page, err := rtPager.NextPage(ctx)
		if err != nil {
			t.Fatalf("NextPage() error: %v", err)
		}
		listed += len(page.QueueRuntimeProperties)
	}
	if listed != 4 {
		t.Errorf("listed %d queue runtime properties, want 4", listed)
	}

	if _, err := ac.DeleteQueue(ctx, "orders", nil); err != nil {
		t.Fatalf("DeleteQueue() error: %v", err)
	}
	if _, err := ac.DeleteQueue(ctx, "orders", nil); responseStatus(err) != http.StatusNotFound {
		t.Errorf("DeleteQueue(deleted) error = %v, want 404", err)
	}
}

func TestAdminTopicsAndSubscriptions(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{})
	ac := newAdminClient(t, srv)
	ctx := testContext(t)

	if _, err := ac.CreateTopic(ctx, "events", &admin.CreateTopicOptions{Properties: &admin.TopicProperties{DefaultMessageTimeToLive: new("P7D")}}); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}
	if _, err := ac.CreateSubscription(ctx, "events", "all", nil); err != nil {
		t.Fatalf("CreateSubscription() error: %v", err)
	}
	_, err := ac.CreateSubscription(ctx, "events", "red", &admin.CreateSubscriptionOptions{Properties: &admin.SubscriptionProperties{
		MaxDeliveryCount: new(int32(2)),
		DefaultRule: &admin.RuleProperties{
			Name:   "red",
			Filter: &admin.SQLFilter{Expression: "color = @c", Parameters: map[string]any{"@c": "red"}},
			Action: &admin.SQLAction{Expression: "SET routed = TRUE"},
		},
	}})
	if err != nil {
		t.Fatalf("CreateSubscription(with default rule) error: %v", err)
	}
	if _, err := ac.CreateSubscription(ctx, "missing", "s", nil); err == nil {
		t.Error("CreateSubscription(missing topic) succeeded, want error")
	}

	sub, err := ac.GetSubscription(ctx, "events", "red", nil)
	if err != nil || sub == nil || *sub.MaxDeliveryCount != 2 {
		t.Fatalf("GetSubscription() = %+v, %v", sub, err)
	}
	if _, err := ac.UpdateSubscription(ctx, "events", "red", admin.SubscriptionProperties{LockDuration: new("PT2M")}, nil); err != nil {
		t.Fatalf("UpdateSubscription() error: %v", err)
	}

	// The rules route messages sent over AMQP.
	client := newClient(t, srv)
	send(t, newSender(t, client, "events"),
		&azservicebus.Message{Body: []byte("r"), ApplicationProperties: map[string]any{"color": "red"}},
		&azservicebus.Message{Body: []byte("b"), ApplicationProperties: map[string]any{"color": "blue"}},
	)
	got := receive(t, newSubReceiver(t, client, "events", "red", radOptions), 1)
	if string(got[0].Body) != "r" || got[0].ApplicationProperties["routed"] != true {
		t.Errorf("red got %q with props %v, want r routed by the default rule", got[0].Body, got[0].ApplicationProperties)
	}

	rt, err := ac.GetSubscriptionRuntimeProperties(ctx, "events", "all", nil)
	if err != nil || rt == nil || rt.ActiveMessageCount != 2 {
		t.Errorf("GetSubscriptionRuntimeProperties(all) = %+v, %v; want 2 active", rt, err)
	}
	trt, err := ac.GetTopicRuntimeProperties(ctx, "events", nil)
	if err != nil || trt == nil || trt.SubscriptionCount != 2 || trt.SQLFilterCount != 2 {
		t.Errorf("GetTopicRuntimeProperties() = %+v, %v; want 2 subscriptions and 2 SQL filters", trt, err)
	}

	var subs []string
	pager := ac.NewListSubscriptionsPager("events", &admin.ListSubscriptionsOptions{MaxPageSize: 1})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("NextPage() error: %v", err)
		}
		for _, s := range page.Subscriptions {
			subs = append(subs, s.SubscriptionName)
		}
	}
	if diff := cmp.Diff([]string{"all", "red"}, subs); diff != "" {
		t.Errorf("listed subscriptions mismatch (-want +got):\n%s", diff)
	}

	var topics []string
	tp := ac.NewListTopicsPager(nil)
	for tp.More() {
		page, err := tp.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range page.Topics {
			topics = append(topics, x.TopicName)
		}
	}
	if diff := cmp.Diff([]string{"events"}, topics); diff != "" {
		t.Errorf("listed topics mismatch (-want +got):\n%s", diff)
	}

	if _, err := ac.DeleteSubscription(ctx, "events", "all", nil); err != nil {
		t.Fatalf("DeleteSubscription() error: %v", err)
	}
	if _, err := ac.DeleteTopic(ctx, "events", nil); err != nil {
		t.Fatalf("DeleteTopic() error: %v", err)
	}
	if s, err := ac.GetSubscription(ctx, "events", "red", nil); err != nil || s != nil {
		t.Errorf("GetSubscription() after deleting the topic = %v, %v; want nil, nil", s, err)
	}
}

func TestAdminRules(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Topics: []server.Topic{{Name: "t", Subscriptions: []server.Subscription{{Name: "s"}}}}})
	ac := newAdminClient(t, srv)
	ctx := testContext(t)

	ruleNames := func() []string {
		var out []string
		pager := ac.NewListRulesPager("t", "s", nil)
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				t.Fatalf("NextPage() error: %v", err)
			}
			for _, r := range page.Rules {
				out = append(out, r.Name)
			}
		}
		sort.Strings(out)
		return out
	}
	if diff := cmp.Diff([]string{"$Default"}, ruleNames()); diff != "" {
		t.Errorf("initial rules mismatch (-want +got):\n%s", diff)
	}
	def, err := ac.GetRule(ctx, "t", "s", "$Default", nil)
	if err != nil || def == nil {
		t.Fatalf("GetRule($Default) = %v, %v", def, err)
	}
	if _, ok := def.Filter.(*admin.TrueFilter); !ok || def.Action != nil {
		t.Errorf("$Default = %T / %T, want TrueFilter without action", def.Filter, def.Action)
	}

	_, err = ac.CreateRule(ctx, "t", "s", &admin.CreateRuleOptions{
		Name:   new("big"),
		Filter: &admin.SQLFilter{Expression: "quantity > @n", Parameters: map[string]any{"@n": 10}},
		Action: &admin.SQLAction{Expression: "SET size = 'big'"},
	})
	if err != nil {
		t.Fatalf("CreateRule(sql) error: %v", err)
	}
	_, err = ac.CreateRule(ctx, "t", "s", &admin.CreateRuleOptions{
		Name:   new("eu"),
		Filter: &admin.CorrelationFilter{Subject: new("order"), ApplicationProperties: map[string]any{"region": "eu", "n": 7}},
	})
	if err != nil {
		t.Fatalf("CreateRule(correlation) error: %v", err)
	}
	if _, err := ac.CreateRule(ctx, "t", "s", &admin.CreateRuleOptions{Name: new("eu"), Filter: &admin.TrueFilter{}}); responseStatus(err) != http.StatusConflict {
		t.Errorf("CreateRule(duplicate) error = %v, want 409", err)
	}

	eu, err := ac.GetRule(ctx, "t", "s", "eu", nil)
	if err != nil || eu == nil {
		t.Fatalf("GetRule(eu) = %v, %v", eu, err)
	}
	cf, ok := eu.Filter.(*admin.CorrelationFilter)
	if !ok || *cf.Subject != "order" {
		t.Fatalf("eu filter = %#v, want the correlation filter", eu.Filter)
	}
	if diff := cmp.Diff(map[string]any{"region": "eu", "n": int64(7)}, cf.ApplicationProperties); diff != "" {
		t.Errorf("eu properties mismatch (-want +got):\n%s", diff)
	}
	big, err := ac.GetRule(ctx, "t", "s", "big", nil)
	if err != nil || big == nil {
		t.Fatalf("GetRule(big) = %v, %v", big, err)
	}
	if f, ok := big.Filter.(*admin.SQLFilter); !ok || f.Expression != "quantity > @n" || f.Parameters["@n"] != int64(10) {
		t.Errorf("big filter = %#v", big.Filter)
	}

	// Without a name, the Go SDK sends the PUT to .../Rules/ and names the
	// rule $Default in the body.
	if _, err := ac.DeleteRule(ctx, "t", "s", "$Default", nil); err != nil {
		t.Fatalf("DeleteRule() error: %v", err)
	}
	if _, err := ac.CreateRule(ctx, "t", "s", &admin.CreateRuleOptions{Filter: &admin.FalseFilter{}}); err != nil {
		t.Fatalf("CreateRule(no name) error: %v", err)
	}
	if r, err := ac.GetRule(ctx, "t", "s", "$Default", nil); err != nil || r == nil {
		t.Fatalf("GetRule($Default) after an unnamed create = %v, %v", r, err)
	} else if _, ok := r.Filter.(*admin.FalseFilter); !ok {
		t.Errorf("unnamed rule filter = %T, want FalseFilter", r.Filter)
	}
	if _, err := ac.DeleteRule(ctx, "t", "s", "$Default", nil); err != nil {
		t.Fatalf("DeleteRule() error: %v", err)
	}
	if _, err := ac.UpdateRule(ctx, "t", "s", admin.RuleProperties{Name: "big", Filter: &admin.SQLFilter{Expression: "quantity > 100"}}); err != nil {
		t.Fatalf("UpdateRule() error: %v", err)
	}
	if diff := cmp.Diff([]string{"big", "eu"}, ruleNames()); diff != "" {
		t.Errorf("rules mismatch (-want +got):\n%s", diff)
	}
	if r, err := ac.GetRule(ctx, "t", "s", "missing", nil); err != nil || r != nil {
		t.Errorf("GetRule(missing) = %v, %v; want nil, nil", r, err)
	}

	// The rules route messages sent over AMQP.
	client := newClient(t, srv)
	msg := func(body, subject string, props map[string]any) *azservicebus.Message {
		return &azservicebus.Message{Body: []byte(body), Subject: &subject, ApplicationProperties: props}
	}
	send(t, newSender(t, client, "t"),
		msg("eu", "order", map[string]any{"region": "eu", "n": 7.0}),
		msg("medium", "x", map[string]any{"quantity": int64(50)}),
		msg("huge", "x", map[string]any{"quantity": int64(500)}),
	)
	got := receive(t, newSubReceiver(t, client, "t", "s", radOptions), 2)
	if diff := cmp.Diff([]string{"eu", "huge"}, sortedBodies(got)); diff != "" {
		t.Errorf("routed messages mismatch (-want +got):\n%s", diff)
	}
}

// TestAdminOverPlainHTTP checks the port also answers management requests
// without TLS, as the JavaScript SDK sends them in emulator mode.
func TestAdminOverPlainHTTP(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("q")})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+srv.Addr().String()+"/q?api-version=2024-05", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /q status = %d, want 200", resp.StatusCode)
	}
}

func TestAdminUpdateReplacesTopicsAndSubscriptions(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{})
	ac := newAdminClient(t, srv)
	ctx := testContext(t)

	if _, err := ac.CreateTopic(ctx, "events", &admin.CreateTopicOptions{Properties: &admin.TopicProperties{
		DefaultMessageTimeToLive: new("P2D"), RequiresDuplicateDetection: new(true), UserMetadata: new("m")}}); err != nil {
		t.Fatal(err)
	}
	updated, err := ac.UpdateTopic(ctx, "events", admin.TopicProperties{MaxSizeInMegabytes: new(int32(2048))}, nil)
	if err != nil {
		t.Fatalf("UpdateTopic() error: %v", err)
	}
	if *updated.MaxSizeInMegabytes != 2048 || *updated.DefaultMessageTimeToLive == "P2D" || (updated.UserMetadata != nil && *updated.UserMetadata != "") {
		t.Errorf("UpdateTopic() = %+v, want the omitted properties back at their defaults", updated.TopicProperties)
	}
	if !*updated.RequiresDuplicateDetection {
		t.Error("RequiresDuplicateDetection changed by an update that left it out")
	}
	_, err = ac.UpdateTopic(ctx, "events", admin.TopicProperties{RequiresDuplicateDetection: new(false)}, nil)
	if responseStatus(err) != http.StatusBadRequest || !strings.Contains(unescapeXML(err), "of an existing Topic can't be changed.") {
		t.Errorf("UpdateTopic(RequiresDuplicateDetection) error = %v, want 400", err)
	}

	if _, err := ac.CreateSubscription(ctx, "events", "audit", &admin.CreateSubscriptionOptions{Properties: &admin.SubscriptionProperties{
		MaxDeliveryCount: new(int32(7)), RequiresSession: new(true), LockDuration: new("PT3M")}}); err != nil {
		t.Fatal(err)
	}
	sub, err := ac.UpdateSubscription(ctx, "events", "audit", admin.SubscriptionProperties{MaxDeliveryCount: new(int32(2))}, nil)
	if err != nil {
		t.Fatalf("UpdateSubscription() error: %v", err)
	}
	if *sub.MaxDeliveryCount != 2 || *sub.LockDuration != "PT1M" || !*sub.RequiresSession {
		t.Errorf("UpdateSubscription() = %+v, want 2, the default lock and the kept session requirement", sub.SubscriptionProperties)
	}
	_, err = ac.UpdateSubscription(ctx, "events", "audit", admin.SubscriptionProperties{RequiresSession: new(false)}, nil)
	if responseStatus(err) != http.StatusBadRequest || !strings.Contains(unescapeXML(err), "RequiresSession property of an existing Subscription can't be changed.") {
		t.Errorf("UpdateSubscription(RequiresSession) error = %v, want 400", err)
	}
}

// unescapeXML undoes the escaping of apostrophes in the body of an error.
func unescapeXML(err error) string {
	if err == nil {
		return ""
	}
	return strings.ReplaceAll(err.Error(), "&apos;", "'")
}
