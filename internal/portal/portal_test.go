package portal_test

import (
	"context"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

func newPortal(t *testing.T) (*broker.Broker, *httptest.Server) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, store.Memory)
	if err != nil {
		t.Fatal(err)
	}
	b, err := broker.New(ctx, st, broker.Options{})
	if err != nil {
		t.Fatal(err)
	}
	h := portal.NewHandler(portal.Options{
		Broker: b,
		Info: func() views.Info {
			return views.Info{Name: "test-ns", Version: "v9", ConnectionStr: "Endpoint=sb://localhost:5672;X=1"}
		},
		Certificate: func() []byte { return []byte("PEM") },
		Started:     time.Now().Add(-90 * time.Second),
		Location:    time.UTC,
	})
	mux := http.NewServeMux()
	mux.Handle(portal.Prefix, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		b.Close()
		st.Close()
	})
	return b, srv
}

func get(t *testing.T, srv *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func post(t *testing.T, srv *httptest.Server, path string, form url.Values, hdr map[string]string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", srv.URL+path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func wantContains(t *testing.T, what, body string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Errorf("%s lacks %q:\n%s", what, w, body)
		}
	}
}

func TestOverview(t *testing.T) {
	b, srv := newPortal(t)
	ctx := context.Background()
	b.CreateQueue(ctx, "orders", broker.DefaultProperties(broker.KindQueue))
	b.CreateTopic(ctx, "events", broker.DefaultProperties(broker.KindTopic))
	if _, err := b.Send(ctx, "orders", nil); err != nil {
		t.Fatal(err)
	}

	status, body := get(t, srv, "/$portal/")
	if status != 200 {
		t.Fatalf("overview status = %d", status)
	}
	wantContains(t, "overview", body,
		"<h1>test-ns</h1>", "Essentials", "v9", "1 queues, 1 topics", "1m 30s",
		"Endpoint=sb://localhost:5672;X=1", "/$portal/certificate.pem",
		"Incoming Requests (Sum)", "Throttled Requests (Sum)", "Incoming Messages (Sum)", "Active Messages (Last)",
		"/$portal/assets/htmx.min.js", `<svg viewBox="0 0 800 260"`)

	// The range selector rewrites the charts.
	status, body = get(t, srv, "/$portal/overview/live?range=7d")
	if status != 200 {
		t.Fatalf("live status = %d", status)
	}
	wantContains(t, "live 7d", body, `value="7d" checked`, "Requests")
	if strings.Contains(body, "Essentials") {
		t.Error("the live fragment repeats the Essentials block")
	}
	// An unknown range falls back to the first.
	if _, body = get(t, srv, "/$portal/overview/live?range=nope"); !strings.Contains(body, `value="1h" checked`) {
		t.Error("unknown range did not fall back to 1 hour")
	}

	if status, body := get(t, srv, "/$portal/certificate.pem"); status != 200 || body != "PEM" {
		t.Errorf("certificate = %d %q", status, body)
	}
	if status, body := get(t, srv, "/$portal/assets/portal.css"); status != 200 || !strings.Contains(body, "--accent") {
		t.Errorf("css = %d", status)
	}
	if status, _ := get(t, srv, "/$portal/assets/htmx.min.js"); status != 200 {
		t.Errorf("htmx status = %d", status)
	}
}

func TestEntityListsAndSearch(t *testing.T) {
	b, srv := newPortal(t)
	ctx := context.Background()
	for _, n := range []string{"orders", "payments", "ORDERS-dlq-test"} {
		b.CreateQueue(ctx, n, broker.DefaultProperties(broker.KindQueue))
	}
	b.CreateTopic(ctx, "events", broker.DefaultProperties(broker.KindTopic))
	b.CreateSubscription(ctx, "events", "audit", broker.DefaultProperties(broker.KindSubscription))

	_, body := get(t, srv, "/$portal/queues")
	wantContains(t, "queues", body, "orders", "payments", "Dead-letter", "+ Queue", `placeholder="Search queues by name"`)
	_, rows := get(t, srv, "/$portal/queues/rows?q=ORDERS")
	wantContains(t, "filtered rows", rows, "orders", "ORDERS-dlq-test")
	if strings.Contains(rows, "payments") || strings.Contains(rows, "<html") {
		t.Errorf("search returned the wrong rows or a whole page:\n%s", rows)
	}
	if _, rows = get(t, srv, "/$portal/queues/rows?q=zzz"); !strings.Contains(rows, "No queues") {
		t.Errorf("empty result = %s", rows)
	}
	_, body = get(t, srv, "/$portal/topics")
	wantContains(t, "topics", body, "events", "Subscriptions", "+ Topic")
}

func TestCreateAndDelete(t *testing.T) {
	b, srv := newPortal(t)
	form := url.Values{
		"name": {"made-in-portal"}, "lockDuration": {"PT5M"}, "maxDeliveryCount": {"500"}, "maxSizeInMegabytes": {"99999"},
		"defaultMessageTimeToLive": {"P90D"}, "requiresSession": {"true"}, "status": {"SendDisabled"},
		"forwardTo": {""},
	}
	// A form post needs a same-origin Origin or an HTMX request.
	if status, _ := post(t, srv, "/$portal/queues", form, nil); status != http.StatusForbidden {
		t.Errorf("cross-site post status = %d, want 403", status)
	}
	status, _ := post(t, srv, "/$portal/queues", form, map[string]string{"Origin": srv.URL})
	if status != http.StatusSeeOther {
		t.Fatalf("create status = %d, want 303", status)
	}
	e, err := b.Entity("made-in-portal")
	if err != nil {
		t.Fatal(err)
	}
	p := e.Properties
	if p.LockDuration != 5*time.Minute || p.MaxDeliveryCount != 500 || p.MaxSizeInMegabytes != 99999 ||
		p.DefaultMessageTimeToLive != 90*24*time.Hour || !p.RequiresSession || p.Status != broker.StatusSendDisabled {
		t.Errorf("properties = %+v", p)
	}

	// The creation is in the activity log under the portal actor.
	entries, _ := b.Activity(context.Background(), broker.ActivityQuery{Actor: portal.Actor})
	if len(entries) != 1 || entries[0].Operation != "CreateQueue" {
		t.Errorf("activity = %v", entries)
	}
	_, body := get(t, srv, "/$portal/activity")
	wantContains(t, "activity page", body, "CreateQueue", "made-in-portal", "portal", "Succeeded")
	_, rows := get(t, srv, "/$portal/activity/rows?status=Failed")
	if !strings.Contains(rows, "No operations match") {
		t.Errorf("failed filter = %s", rows)
	}

	// Bad input re-renders the form with what was typed.
	bad := url.Values{"name": {"x"}, "lockDuration": {"five minutes"}}
	status, body = post(t, srv, "/$portal/queues", bad, map[string]string{"HX-Request": "true"})
	if status != http.StatusUnprocessableEntity {
		t.Errorf("bad create status = %d", status)
	}
	wantContains(t, "error form", body, "Lock duration", "five minutes", `role="alert"`)
	// A duplicate name is reported.
	status, body = post(t, srv, "/$portal/queues", form, map[string]string{"HX-Request": "true"})
	if status != http.StatusUnprocessableEntity || !strings.Contains(body, "already exists") {
		t.Errorf("duplicate = %d %s", status, body)
	}
	// A topic form has no lock duration or forwarding.
	_, body = get(t, srv, "/$portal/topics/new")
	if strings.Contains(body, "Lock duration") || !strings.Contains(body, "Support ordering") {
		t.Errorf("topic form = %s", body)
	}
	status, _ = post(t, srv, "/$portal/topics", url.Values{"name": {"made-topic"}, "supportOrdering": {"true"}}, map[string]string{"HX-Request": "true"})
	if status != http.StatusSeeOther {
		t.Errorf("topic create = %d", status)
	}

	status, _ = post(t, srv, "/$portal/entities/delete", url.Values{"path": {"made-in-portal"}}, map[string]string{"HX-Request": "true"})
	if status != 200 || b.Exists("made-in-portal") {
		t.Errorf("delete status = %d, exists = %v", status, b.Exists("made-in-portal"))
	}
	entries, _ = b.Activity(context.Background(), broker.ActivityQuery{Operation: "DeleteQueue"})
	if len(entries) != 1 || entries[0].Actor != portal.Actor {
		t.Errorf("delete activity = %v", entries)
	}
}

// TestGeneratedFilesAreCurrent checks that the committed *_templ.go files
// match their .templ sources.
func TestGeneratedFilesAreCurrent(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go not found in PATH")
	}
	tmp := t.TempDir()
	srcs, _ := filepath.Glob("views/*.templ")
	for _, s := range srcs {
		data, err := os.ReadFile(s)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(tmp, filepath.Base(s)), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("go", "tool", "templ", "generate", "-path", tmp)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("templ is not available: %v\n%s", err, out)
	}
	for _, s := range srcs {
		name := strings.TrimSuffix(filepath.Base(s), ".templ") + "_templ.go"
		want, err := os.ReadFile(filepath.Join(tmp, name))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join("views", name))
		if err != nil {
			t.Fatalf("%s is missing; run go tool templ generate ./internal/portal/views", name)
		}
		// templ records source paths relative to where it ran.
		if string(want) != strings.ReplaceAll(string(got), "`internal/portal/views/", "`") {
			t.Errorf("%s is out of date; run go tool templ generate ./internal/portal/views", name)
		}
	}
}

func hx(extra map[string]string) map[string]string {
	m := map[string]string{"HX-Request": "true"}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func TestEntityPages(t *testing.T) {
	b, srv := newPortal(t)
	ctx := context.Background()
	q := broker.DefaultProperties(broker.KindQueue)
	q.DefaultMessageTimeToLive = 14 * 24 * time.Hour
	q.MaxDeliveryCount = 3
	b.CreateQueue(ctx, "orders", q)
	b.CreateTopic(ctx, "events", broker.DefaultProperties(broker.KindTopic))
	b.CreateSubscription(ctx, "events", "audit", broker.DefaultProperties(broker.KindSubscription))
	b.CreateSubscription(ctx, "events", "billing", broker.DefaultProperties(broker.KindSubscription))

	status, body := get(t, srv, "/$portal/entity?path=orders")
	if status != 200 {
		t.Fatalf("queue page = %d", status)
	}
	wantContains(t, "queue page", body, "Current size", "Max size", "14 days", "NEVER", "Free space", "100.0",
		"Active", "Dead-letter", "Transfer dead-letter", "Max delivery count", "<dd>3</dd>", "Incoming Messages (Sum)", "Properties")
	if strings.Contains(body, "Subscriptions</h3>") {
		t.Error("a queue page lists subscriptions")
	}

	_, body = get(t, srv, "/$portal/entity?path=events")
	wantContains(t, "topic page", body, "+ Subscription", "Subscriptions</h3>", "audit", "billing", "Client Scoped")
	_, rows := get(t, srv, "/$portal/entity/subscriptions?path=events&q=bill")
	if !strings.Contains(rows, "billing") || strings.Contains(rows, "audit") {
		t.Errorf("filtered subscriptions = %s", rows)
	}
	_, body = get(t, srv, "/$portal/entity?path=events/subscriptions/audit")
	wantContains(t, "subscription page", body, ">Rules<", "events</a>", "Lock duration")
	if status, _ := get(t, srv, "/$portal/entity?path=nothing"); status != http.StatusNotFound {
		t.Errorf("unknown entity = %d, want 404", status)
	}
	_, live := get(t, srv, "/$portal/entity/live?path=orders&range=6h")
	wantContains(t, "live", live, `value="6h" checked`)
}

func TestEditProperties(t *testing.T) {
	b, srv := newPortal(t)
	ctx := context.Background()
	p := broker.DefaultProperties(broker.KindQueue)
	p.RequiresSession = true
	p.EnableBatchedOperations = true
	b.CreateQueue(ctx, "orders", p)

	_, body := get(t, srv, "/$portal/entity/edit?path=orders")
	wantContains(t, "edit form", body, "Properties: orders", `name="name" value="orders" readonly`, "cannot change after creation", "Save")

	form := url.Values{
		"path": {"orders"}, "lockDuration": {"PT2M"}, "maxDeliveryCount": {"7"}, "status": {"ReceiveDisabled"},
		"requiresSession": {""}, // disabled in the page; a forged value must not apply
		"forwardTo":       {""},
	}
	status, _ := post(t, srv, "/$portal/entity/edit", form, map[string]string{"Origin": srv.URL})
	if status != http.StatusSeeOther {
		t.Fatalf("edit status = %d", status)
	}
	e, _ := b.Entity("orders")
	got := e.Properties
	if got.LockDuration != 2*time.Minute || got.MaxDeliveryCount != 7 || got.Status != broker.StatusReceiveDisabled {
		t.Errorf("properties = %+v", got)
	}
	if !got.RequiresSession {
		t.Error("an immutable property changed")
	}
	if got.EnableBatchedOperations {
		t.Error("an unchecked box was not applied")
	}
	if got.MaxSizeInMegabytes != p.MaxSizeInMegabytes {
		t.Error("a field left blank did not keep its value")
	}
	entries, _ := b.Activity(ctx, broker.ActivityQuery{Operation: "UpdateQueue"})
	if len(entries) != 1 || entries[0].Actor != portal.Actor {
		t.Errorf("activity = %v", entries)
	}
	form.Set("lockDuration", "soon")
	if status, body := post(t, srv, "/$portal/entity/edit", form, hx(nil)); status != http.StatusUnprocessableEntity || !strings.Contains(body, "soon") {
		t.Errorf("bad edit = %d %s", status, body)
	}
}

func TestCreateSubscriptionAndDelete(t *testing.T) {
	b, srv := newPortal(t)
	b.CreateTopic(context.Background(), "events", broker.DefaultProperties(broker.KindTopic))
	_, body := get(t, srv, "/$portal/subscriptions/new?topic=events")
	wantContains(t, "subscription form", body, "Create subscription", `name="topic" value="events"`, "Dead-letter on filter evaluation exceptions", "Lock duration")
	if strings.Contains(body, "Max size (MB)") {
		t.Error("a subscription form has a max size")
	}
	status, _ := post(t, srv, "/$portal/subscriptions", url.Values{"name": {"audit"}, "topic": {"events"}, "maxDeliveryCount": {"4"}}, hx(nil))
	if status != http.StatusSeeOther {
		t.Fatalf("create = %d", status)
	}
	e, err := b.Entity("events/subscriptions/audit")
	if err != nil || e.Properties.MaxDeliveryCount != 4 {
		t.Fatalf("subscription = %+v, %v", e, err)
	}
	req, _ := http.NewRequest("POST", srv.URL+"/$portal/entities/delete", strings.NewReader(url.Values{"path": {"events/subscriptions/audit"}, "redirect": {"1"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if loc := resp.Header.Get("HX-Redirect"); loc != "/$portal/entity?path=events" {
		t.Errorf("HX-Redirect = %q, want the topic page", loc)
	}
}

func TestRules(t *testing.T) {
	b, srv := newPortal(t)
	ctx := context.Background()
	b.CreateTopic(ctx, "events", broker.DefaultProperties(broker.KindTopic))
	b.CreateSubscription(ctx, "events", "audit", broker.DefaultProperties(broker.KindSubscription))
	const sub = "events/subscriptions/audit"

	_, body := get(t, srv, "/$portal/rules?path="+url.QueryEscape(sub))
	wantContains(t, "rules page", body, "$Default", "True filter", "1=1", "Test the filters", "sys.Label = order.created")

	// Create an SQL rule with an action; the validator accepts and rejects.
	form := url.Values{
		"path": {sub}, "name": {"big"}, "kind": {"Sql"}, "sql": {"quantity > 10 AND sys.Label LIKE 'order.%'"},
		"action": {"SET region = 'eu'"},
	}
	_, msg := post(t, srv, "/$portal/rules/validate", form, hx(nil))
	wantContains(t, "validation", msg, "The rule is valid")
	bad := url.Values{"path": {sub}, "kind": {"Sql"}, "sql": {"quantity >"}}
	_, msg = post(t, srv, "/$portal/rules/validate", bad, hx(nil))
	if strings.Contains(msg, "is valid") || !strings.Contains(msg, "error") {
		t.Errorf("validation of a bad filter = %s", msg)
	}
	if status, _ := post(t, srv, "/$portal/rules/save", form, hx(nil)); status != http.StatusSeeOther {
		t.Fatalf("save = %d", status)
	}
	// A correlation rule with a typed custom property.
	corr := url.Values{"path": {sub}, "name": {"eu"}, "kind": {"Correlation"}, "subject": {"order.created"}, "corrprops": {"region = eu\nvip = true"}}
	if status, _ := post(t, srv, "/$portal/rules/save", corr, hx(nil)); status != http.StatusSeeOther {
		t.Fatalf("save correlation = %d", status)
	}
	rules, _ := b.Rules(sub)
	var eu broker.Rule
	for _, r := range rules {
		if r.Name == "eu" {
			eu = r
		}
	}
	if eu.Filter.Correlation == nil || eu.Filter.Correlation.Subject != "order.created" || eu.Filter.Correlation.Properties["vip"] != true {
		t.Errorf("correlation rule = %+v", eu.Filter.Correlation)
	}
	// A duplicate name is an error on the form.
	if status, body := post(t, srv, "/$portal/rules/save", form, hx(nil)); status != http.StatusUnprocessableEntity || !strings.Contains(body, "already exists") {
		t.Errorf("duplicate rule = %d %s", status, body)
	}

	// Edit keeps the name and replaces the filter.
	_, body = get(t, srv, "/$portal/rules/edit?path="+url.QueryEscape(sub)+"&name=big")
	wantContains(t, "rule form", body, "Edit rule big", "quantity &gt; 10", "SET region")
	form.Set("edit", "1")
	form.Set("sql", "quantity > 100")
	if status, _ := post(t, srv, "/$portal/rules/save", form, hx(nil)); status != http.StatusSeeOther {
		t.Fatalf("edit = %d", status)
	}
	if rules, _ = b.Rules(sub); rules[1].Filter.SQL != "quantity > 100" {
		t.Errorf("rules after edit = %+v", rules)
	}

	// The tester shows what each rule does.
	sample := "sys.Label = order.created\nquantity = 200\nregion = us"
	status, res := post(t, srv, "/$portal/rules/test", url.Values{"path": {sub}, "sample": {sample}}, hx(nil))
	if status != 200 {
		t.Fatalf("test = %d %s", status, res)
	}
	wantContains(t, "test results", res, "Delivered", "~ region: us -&gt; eu")
	if strings.Contains(res, "<html") {
		t.Error("the test result is a whole page")
	}
	if status, body := post(t, srv, "/$portal/rules/test", url.Values{"path": {sub}, "sample": {"no equals sign"}}, hx(nil)); status != http.StatusUnprocessableEntity || !strings.Contains(body, "line 1") {
		t.Errorf("bad sample = %d %s", status, body)
	}
	if status, _ := post(t, srv, "/$portal/rules/test", url.Values{"path": {sub}, "sample": {"sys.Nope = 1"}}, hx(nil)); status != http.StatusUnprocessableEntity {
		t.Errorf("unknown system property = %d", status)
	}

	// Delete, and every change is in the activity log.
	if status, _ := post(t, srv, "/$portal/rules/delete", url.Values{"path": {sub}, "name": {"$Default"}}, hx(nil)); status != 200 {
		t.Fatalf("delete = %d", status)
	}
	entries, _ := b.Activity(ctx, broker.ActivityQuery{Actor: portal.Actor})
	ops := map[string]int{}
	for _, e := range entries {
		ops[e.Operation]++
	}
	if ops["CreateRule"] != 3 /* two saved and one refused */ || ops["UpdateRule"] != 1 || ops["DeleteRule"] != 1 {
		t.Errorf("portal operations = %v", ops)
	}
}

func peekWith(t *testing.T, srv *httptest.Server, query string) string {
	t.Helper()
	status, body := get(t, srv, "/$portal/explorer/view?"+query)
	if status != 200 {
		t.Fatalf("view %s = %d %s", query, status, body)
	}
	return body
}

func TestExplorerPeek(t *testing.T) {
	b, srv := newPortal(t)
	ctx := context.Background()
	b.CreateQueue(ctx, "orders", broker.DefaultProperties(broker.KindQueue))

	// Send through the portal: JSON body, typed properties, and a repeat.
	form := url.Values{
		"path": {"orders"}, "contentType": {"application/json"}, "body": {`{"total": 42}`}, "messageId": {"m-1"},
		"subject": {"order.created"}, "ttl": {"PT1H"}, "repeat": {"3"}, "interval": {"0"},
		"pkey": {"tenant", "retries", "vip", "id", ""}, "ptype": {"string", "int", "boolean", "uuid", "string"},
		"pvalue": {"acme", "3", "true", "6f9619ff-8b86-d011-b42d-00c04fc964ff", ""},
	}
	status, body := post(t, srv, "/$portal/explorer/send", form, hx(nil))
	if status != 200 {
		t.Fatalf("send = %d %s", status, body)
	}
	wantContains(t, "send result", body, "Sent 3 messages to orders")

	view := peekWith(t, srv, "path=orders&op=start")
	wantContains(t, "peek", view, "Showing 3 of 3 messages", "Queue (3)", "Dead-letter (0)", "m-1", "order.created", "Active", `{&#34;total&#34;: 42}`)
	// Peeking again changes nothing: peek does not lock.
	view = peekWith(t, srv, "path=orders&op=start")
	wantContains(t, "second peek", view, "Showing 3 of 3 messages")
	// Peek next continues after the last shown message.
	view = peekWith(t, srv, "path=orders&op=next&next=4&count=10")
	wantContains(t, "peek next", view, "Showing 0 of 3 messages", "No messages")
	view = peekWith(t, srv, "path=orders&op=opts&from=2&count=1")
	wantContains(t, "peek options", view, "Showing 1 of 3 messages")

	// The detail shows broker and custom properties with their types.
	_, det := get(t, srv, "/$portal/explorer/detail?path=orders&seq=1")
	wantContains(t, "detail", det, "Message Body", "Message Properties", "deliveryCount", "contentType", "application/json",
		"messageId", "sequenceNumber", "enqueuedTimeUtc", "expiresAtUtc", "timeToLive", "PT1H",
		"tenant", "acme", "string", "retries", "int", "vip", "boolean", "uuid", "6f9619ff-8b86-d011-b42d-00c04fc964ff", "state")
	wantContains(t, "pretty json", det, "&#34;total&#34;: 42")

	// A bad send is refused with the form, and nothing is sent.
	bad := url.Values{"path": {"orders"}, "contentType": {"application/json"}, "body": {"{not json"}}
	if status, body := post(t, srv, "/$portal/explorer/send", bad, hx(nil)); status != http.StatusUnprocessableEntity || !strings.Contains(body, "not valid JSON") {
		t.Errorf("bad json = %d %s", status, body)
	}
	bad = url.Values{"path": {"orders"}, "contentType": {"text/plain"}, "body": {"x"}, "pkey": {"n"}, "ptype": {"int"}, "pvalue": {"many"}}
	if status, body := post(t, srv, "/$portal/explorer/send", bad, hx(nil)); status != http.StatusUnprocessableEntity || !strings.Contains(body, "Custom property n") {
		t.Errorf("bad property = %d %s", status, body)
	}
	bad = url.Values{"path": {"orders"}, "contentType": {"text/plain"}, "body": {"x"}, "repeat": {"100"}, "interval": {"1000"}}
	if status, _ := post(t, srv, "/$portal/explorer/send", bad, hx(nil)); status != http.StatusUnprocessableEntity {
		t.Errorf("long repeat = %d, want 422", status)
	}
	if n := countOf(t, b, "orders"); n != 3 {
		t.Errorf("messages after refused sends = %d, want 3", n)
	}

	// Scheduled messages show with their state, and the panel has the form.
	sched := url.Values{"path": {"orders"}, "contentType": {"text/plain"}, "body": {"later"}, "scheduled": {"2999-01-01T00:00"}}
	if status, body := post(t, srv, "/$portal/explorer/send", sched, hx(nil)); status != 200 {
		t.Fatalf("schedule = %d %s", status, body)
	}
	view = peekWith(t, srv, "path=orders&op=start")
	wantContains(t, "scheduled", view, "Scheduled", "later")
	_, panel := get(t, srv, "/$portal/explorer/send?path=orders")
	wantContains(t, "panel", panel, "Send messages", "Upload .json file", "Repeat send", "Custom Properties", "Broker Properties", "application/xml")

	// Export and body download.
	req, _ := http.NewRequest("GET", srv.URL+"/$portal/explorer/export?path=orders&format=csv&seq=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	csvBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.HasPrefix(resp.Header.Get("Content-Disposition"), "attachment") || !strings.Contains(string(csvBody), "sequenceNumber,messageId") || !strings.Contains(string(csvBody), "m-1") {
		t.Errorf("csv = %v %s", resp.Header, csvBody)
	}
	_, js := get(t, srv, "/$portal/explorer/export?path=orders&format=json&count=2")
	wantContains(t, "json export", js, `"sequenceNumber": 1`, `"applicationProperties"`, `"tenant": "acme"`)
	if status, body := get(t, srv, "/$portal/explorer/body?path=orders&seq=1"); status != 200 || !strings.Contains(body, `"total": 42`) {
		t.Errorf("body download = %d %q", status, body)
	}
	if status, _ := get(t, srv, "/$portal/explorer/body?path=orders&seq=1&seq=2"); status != http.StatusBadRequest {
		t.Errorf("two bodies = %d, want 400", status)
	}
}

func countOf(t *testing.T, b *broker.Broker, path string) int64 {
	t.Helper()
	c, err := b.Counts(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return c.Active + c.Scheduled + c.Deferred
}

func TestExplorerReceiveAndSettle(t *testing.T) {
	b, srv := newPortal(t)
	ctx := context.Background()
	p := broker.DefaultProperties(broker.KindQueue)
	p.MaxDeliveryCount = 5
	b.CreateQueue(ctx, "work", p)
	for _, body := range []string{"a", "b", "c", "d"} {
		post(t, srv, "/$portal/explorer/send", url.Values{"path": {"work"}, "contentType": {"text/plain"}, "body": {body}}, hx(nil))
	}
	// Receive is a change: a GET is refused.
	if status, _ := get(t, srv, "/$portal/explorer/view?path=work&mode=receive&op=receive"); status != http.StatusMethodNotAllowed {
		t.Errorf("GET receive = %d, want 405", status)
	}
	status, body := post(t, srv, "/$portal/explorer/view", url.Values{"path": {"work"}, "mode": {"receive"}, "op": {"receive"}, "count": {"3"}}, hx(nil))
	if status != 200 {
		t.Fatalf("receive = %d %s", status, body)
	}
	wantContains(t, "received", body, "Received 3 messages", "Locked until", `name="token"`, "Dead-letter reason")
	tokens := regexpAll(`name="token" value="([0-9a-f-]+)"`, body)
	helds := regexpAll(`name="held" value="([^"]+)"`, body)
	if len(tokens) != 3 || len(helds) != 3 {
		t.Fatalf("tokens = %v, held = %v", tokens, helds)
	}
	// The received messages are locked: a second receive gets the fourth.
	_, body2 := post(t, srv, "/$portal/explorer/view", url.Values{"path": {"work"}, "mode": {"receive"}, "op": {"receive"}, "count": {"10"}, "held": helds}, hx(nil))
	if got := len(regexpAll(`name="token" value="`, body2)); got != 4 {
		t.Errorf("rows after a second receive = %d, want 3 held + 1 new", got)
	}

	settle := func(action string, toks []string, held []string, extra url.Values) (int, string) {
		f := url.Values{"path": {"work"}, "mode": {"receive"}, "action": {action}, "token": toks, "held": held}
		for k, v := range extra {
			f[k] = v
		}
		return post(t, srv, "/$portal/explorer/settle", f, hx(nil))
	}
	status, body = settle("complete", tokens[:1], helds, nil)
	wantContains(t, "complete", body, "Complete: 1 messages")
	if got := len(regexpAll(`name="token" value="`, body)); got != 2 {
		t.Errorf("rows after completing one = %d, want 2", got)
	}
	if c, _ := b.Counts(ctx, "work"); c.Active != 3 {
		t.Errorf("active after complete = %d, want 3", c.Active)
	}
	// Dead-letter with a reason and a description.
	_, body = settle("deadletter", tokens[1:2], helds[1:], url.Values{"reason": {"Poison"}, "description": {"cannot parse"}})
	wantContains(t, "dead-letter", body, "Deadletter: 1 messages", "Dead-letter (1)")
	c, _ := b.Counts(ctx, "work")
	if c.DeadLetter != 1 {
		t.Fatalf("dead-letter count = %d", c.DeadLetter)
	}
	// The DLQ tab shows the reason, and a DLQ offers no dead-lettering.
	view := peekWith(t, srv, "path=work&queue=dlq&op=start")
	wantContains(t, "dlq", view, "Showing 1 of 1 messages")
	_, det := get(t, srv, "/$portal/explorer/detail?path=work&queue=dlq&seq=2")
	wantContains(t, "dlq detail", det, "deadLetterReason", "Poison", "deadLetterErrorDescription", "cannot parse")
	_, dlqRecv := post(t, srv, "/$portal/explorer/view", url.Values{"path": {"work"}, "queue": {"dlq"}, "mode": {"receive"}, "op": {"receive"}}, hx(nil))
	if strings.Contains(dlqRecv, "Dead-letter reason") {
		t.Error("a dead-letter queue offers dead-lettering")
	}

	// Abandon and defer; an unselected action and a stale token report errors.
	_, body = settle("abandon", tokens[2:3], helds[2:], nil)
	wantContains(t, "abandon", body, "Abandon: 1 messages")
	_, body = settle("complete", nil, nil, nil)
	wantContains(t, "empty selection", body, "Select the messages")
	_, body = settle("complete", tokens[:1], nil, nil)
	wantContains(t, "stale", body, "1 messages failed")
	// Receive and delete removes the messages for good.
	_, body = post(t, srv, "/$portal/explorer/view", url.Values{"path": {"work"}, "mode": {"receive"}, "op": {"receive"}, "settle": {"delete"}, "count": {"10"}}, hx(nil))
	wantContains(t, "receive and delete", body, "Received and deleted")
	// The fourth message is still locked by the second receive, so only
	// two were left to take.
	if n := countOf(t, b, "work"); n != 1 {
		t.Errorf("messages after receive and delete = %d, want the locked one", n)
	}
}

func TestExplorerResendAndSubscription(t *testing.T) {
	b, srv := newPortal(t)
	ctx := context.Background()
	b.CreateQueue(ctx, "work", broker.DefaultProperties(broker.KindQueue))
	b.CreateTopic(ctx, "events", broker.DefaultProperties(broker.KindTopic))
	b.CreateSubscription(ctx, "events", "audit", broker.DefaultProperties(broker.KindSubscription))

	post(t, srv, "/$portal/explorer/send", url.Values{"path": {"work"}, "contentType": {"text/plain"}, "body": {"poison"}, "pkey": {"k"}, "ptype": {"string"}, "pvalue": {"v"}}, hx(nil))
	addr, _ := b.Resolve("work")
	ds, _ := b.Receive(ctx, addr, 1, broker.PeekLock)
	if err := b.DeadLetter(ctx, addr, ds[0].LockToken, "Bad", "bad", nil); err != nil {
		t.Fatal(err)
	}
	// Re-send from the DLQ puts a copy back, without the old annotations.
	status, body := post(t, srv, "/$portal/explorer/resend", url.Values{"path": {"work"}, "queue": {"dlq"}, "seq": {"1"}}, hx(nil))
	if status != 200 {
		t.Fatalf("resend = %d %s", status, body)
	}
	wantContains(t, "resend", body, "Sent 1 messages to work")
	view := peekWith(t, srv, "path=work&op=start")
	wantContains(t, "resent", view, "Showing 1 of 1 messages", "poison")
	_, det := get(t, srv, "/$portal/explorer/detail?path=work&seq=2")
	wantContains(t, "resent detail", det, "<td class=\"name\">k</td>")
	if strings.Contains(det, "deadLetterSource") {
		t.Error("the re-sent message keeps the dead-letter source")
	}
	_, body = post(t, srv, "/$portal/explorer/resend", url.Values{"path": {"work"}, "queue": {"dlq"}}, hx(nil))
	wantContains(t, "nothing selected", body, "Select the messages")

	// A subscription: sending goes to the topic, peeking shows its copy.
	_, panel := get(t, srv, "/$portal/explorer/send?path=events/subscriptions/audit")
	wantContains(t, "subscription panel", panel, "To events")
	post(t, srv, "/$portal/explorer/send", url.Values{"path": {"events/subscriptions/audit"}, "contentType": {"text/plain"}, "body": {"hello"}}, hx(nil))
	view = peekWith(t, srv, "path=events/subscriptions/audit&op=start")
	wantContains(t, "subscription peek", view, "Subscription (1)", "hello")
	// The topic page of the Explorer lists subscriptions instead.
	_, topic := get(t, srv, "/$portal/explorer?path=events")
	wantContains(t, "topic explorer", topic, "Pick a subscription", "audit")
	if status, _ := get(t, srv, "/$portal/explorer/view?path=events"); status != http.StatusBadRequest {
		t.Errorf("view of a topic = %d, want 400", status)
	}
	_, page := get(t, srv, "/$portal/explorer?path=work")
	wantContains(t, "page", page, "Peek Mode", "Receive Mode", "Send messages", "Peek from start", "Peek next messages",
		"Peek with options", "Re-send selected messages", "Download selected message body", "Export messages", "Transfer dead-letter (0)")
}

func regexpAll(pattern, s string) []string {
	re := regexp.MustCompile(pattern)
	var out []string
	for _, m := range re.FindAllStringSubmatch(s, -1) {
		if len(m) > 1 {
			out = append(out, html.UnescapeString(m[1]))
		} else {
			out = append(out, m[0])
		}
	}
	return out
}

func TestMetricsPage(t *testing.T) {
	b, srv := newPortal(t)
	ctx := context.Background()
	b.CreateQueue(ctx, "orders", broker.DefaultProperties(broker.KindQueue))
	b.CreateTopic(ctx, "events", broker.DefaultProperties(broker.KindTopic))
	b.CreateSubscription(ctx, "events", "audit", broker.DefaultProperties(broker.KindSubscription))
	for i := 0; i < 4; i++ {
		post(t, srv, "/$portal/explorer/send", url.Values{"path": {"orders"}, "contentType": {"text/plain"}, "body": {"x"}}, hx(nil))
	}

	status, body := get(t, srv, "/$portal/metrics")
	if status != 200 {
		t.Fatalf("metrics page = %d", status)
	}
	wantContains(t, "page", body, "Namespace", "queue: orders", "topic: events", "subscription: events/Subscriptions/audit",
		"Incoming Requests", "Active Connections", "Size (bytes)", `value="IncomingMessages" checked`,
		"Incoming Messages (Sum)", "Active Messages (Last)", "/$api/metrics?entity=", "Metrics</a>")
	if strings.Contains(body, `value="UserErrors" checked`) {
		t.Error("a metric nobody chose is selected by default")
	}

	// One chart per chosen metric, for the chosen entity and interval.
	_, view := get(t, srv, "/$portal/metrics/view?entity=orders&m=IncomingMessages&m=Size&range=6h")
	wantContains(t, "view", view, "Incoming Messages (Sum)", "Size (bytes) (Last)", "entity=orders", "step=5m0s")
	if strings.Contains(view, "Outgoing") || strings.Contains(view, "<html") {
		t.Errorf("view has unchosen metrics or a whole page:\n%s", view)
	}
	if got := regexpAll(`<strong>(\d+)</strong>`, view); len(got) < 1 || got[0] != "4" {
		t.Errorf("legend totals = %v, want the 4 sent messages first", got)
	}
	_, none := get(t, srv, "/$portal/metrics/view?entity=orders")
	wantContains(t, "no metric", none, "Select at least one metric")
	// An unknown entity falls back to the namespace instead of failing.
	if status, _ := get(t, srv, "/$portal/metrics/view?entity=gone&m=IncomingMessages"); status != 200 {
		t.Errorf("unknown entity = %d, want 200", status)
	}
}

func newAuthPortal(t *testing.T, mode auth.Mode) (*broker.Broker, *auth.Authority, *httptest.Server) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, store.Memory)
	if err != nil {
		t.Fatal(err)
	}
	b, err := broker.New(ctx, st, broker.Options{})
	if err != nil {
		t.Fatal(err)
	}
	a, err := auth.New(ctx, st, auth.Options{Mode: mode})
	if err != nil {
		t.Fatal(err)
	}
	h := portal.NewHandler(portal.Options{
		Broker: b, Auth: a, Location: time.UTC, Started: time.Now(),
		Info: func() views.Info {
			return views.Info{Name: "ns", ConnectionStr: "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true"}
		},
	})
	mux := http.NewServeMux()
	mux.Handle(portal.Prefix, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		b.Close()
		st.Close()
	})
	return b, a, srv
}

func TestPoliciesAndAccessPages(t *testing.T) {
	b, a, srv := newAuthPortal(t, auth.ModeNone)
	ctx := context.Background()
	b.CreateQueue(ctx, "orders", broker.DefaultProperties(broker.KindQueue))

	_, body := get(t, srv, "/$portal/policies")
	wantContains(t, "policies off", body, "Authentication is off", "No policies", "queue: orders", "Shared access policies")

	status, _ := post(t, srv, "/$portal/policies", url.Values{"scope": {"orders"}, "name": {"sender"}, "rights": {"Send"}}, hx(nil))
	if status != http.StatusSeeOther {
		t.Fatalf("create policy = %d", status)
	}
	p, ok := a.Policy("orders", "sender")
	if !ok || p.Rights != auth.Send || p.PrimaryKey == "" || p.PrimaryKey == p.SecondaryKey {
		t.Fatalf("policy = %+v, %v", p, ok)
	}
	_, body = get(t, srv, "/$portal/policies")
	wantContains(t, "policies", body, "sender", "orders", "Send", p.PrimaryKey, "SharedAccessKeyName=sender", "New primary")
	if status, body := post(t, srv, "/$portal/policies", url.Values{"scope": {""}, "name": {"x"}}, hx(nil)); status != http.StatusUnprocessableEntity || !strings.Contains(body, "at least one right") {
		t.Errorf("policy without rights = %d %s", status, body)
	}

	_, rows := post(t, srv, "/$portal/policies/regenerate", url.Values{"scope": {"orders"}, "name": {"sender"}, "which": {"primary"}}, hx(nil))
	q, _ := a.Policy("orders", "sender")
	if q.PrimaryKey == p.PrimaryKey || q.SecondaryKey != p.SecondaryKey || !strings.Contains(rows, q.PrimaryKey) {
		t.Errorf("regenerate: old %+v new %+v", p, q)
	}
	post(t, srv, "/$portal/policies/delete", url.Values{"scope": {"orders"}, "name": {"sender"}}, hx(nil))
	if _, ok := a.Policy("orders", "sender"); ok {
		t.Error("the policy was not deleted")
	}

	// Role assignments.
	_, body = get(t, srv, "/$portal/access")
	wantContains(t, "access", body, "Access control (IAM)", "-auth full", "Azure Service Bus Data Sender", "No role assignments")
	post(t, srv, "/$portal/access", url.Values{"principal": {"oid-9"}, "role": {"sender"}, "scope": {"orders"}}, hx(nil))
	as := a.Assignments()
	if len(as) != 1 || as[0].Principal != "oid-9" || as[0].Role != auth.RoleSender || as[0].Scope != "orders" {
		t.Fatalf("assignments = %+v", as)
	}
	_, body = get(t, srv, "/$portal/access")
	wantContains(t, "assignments", body, "oid-9", "Azure Service Bus Data Sender", "orders")
	post(t, srv, "/$portal/access/delete", url.Values{"id": {strconv.FormatInt(as[0].ID, 10)}}, hx(nil))
	if len(a.Assignments()) != 0 {
		t.Error("the assignment was not removed")
	}
	// Changes are in the activity log and never carry a key.
	entries, _ := b.Activity(ctx, broker.ActivityQuery{Actor: portal.Actor})
	ops := map[string]int{}
	for _, e := range entries {
		ops[e.Operation]++
		if strings.Contains(e.Detail, q.PrimaryKey) {
			t.Error("a key reached the activity log")
		}
	}
	for _, op := range []string{"CreatePolicy", "RegeneratePolicyKey", "DeletePolicy", "CreateRoleAssignment", "DeleteRoleAssignment"} {
		if ops[op] == 0 {
			t.Errorf("activity lacks %s: %v", op, ops)
		}
	}
}

func TestPortalWithoutAuthority(t *testing.T) {
	_, srv := newPortal(t)
	if status, _ := get(t, srv, "/$portal/policies"); status != http.StatusNotFound {
		t.Errorf("policies without an authority = %d, want 404", status)
	}
}

func TestExplorerPurge(t *testing.T) {
	b, srv := newPortal(t)
	ctx := context.Background()
	b.CreateQueue(ctx, "work", broker.DefaultProperties(broker.KindQueue))
	for i := 0; i < 5; i++ {
		post(t, srv, "/$portal/explorer/send", url.Values{"path": {"work"}, "contentType": {"text/plain"}, "body": {"m"}}, hx(nil))
	}
	post(t, srv, "/$portal/explorer/send", url.Values{"path": {"work"}, "contentType": {"text/plain"}, "body": {"later"}, "scheduled": {"2999-01-01T00:00"}}, hx(nil))
	addr, _ := b.Resolve("work")
	locked, err := b.Receive(ctx, addr, 1, broker.PeekLock)
	if err != nil || len(locked) != 1 {
		t.Fatal(err)
	}

	_, page := get(t, srv, "/$portal/explorer?path=work")
	wantContains(t, "page", page, "Purge", "hx-confirm")
	status, body := post(t, srv, "/$portal/explorer/purge", url.Values{"path": {"work"}}, hx(nil))
	if status != 200 {
		t.Fatalf("purge = %d %s", status, body)
	}
	wantContains(t, "purge result", body, "Purged 4 messages from work")
	c, _ := b.Counts(ctx, "work")
	if c.Active != 1 || c.Scheduled != 1 {
		t.Errorf("counts after the purge = %+v; the locked and the scheduled message stay", c)
	}
	entries, _ := b.Activity(ctx, broker.ActivityQuery{Operation: "DeleteMessages"})
	if len(entries) == 0 || entries[0].Actor != portal.Actor {
		t.Errorf("activity = %v", entries)
	}
	// The dead-letter tab purges its own queue.
	if err := b.DeadLetter(ctx, addr, locked[0].LockToken, "r", "d", nil); err != nil {
		t.Fatal(err)
	}
	_, body = post(t, srv, "/$portal/explorer/purge", url.Values{"path": {"work"}, "queue": {"dlq"}}, hx(nil))
	wantContains(t, "dlq purge", body, "Purged 1 messages from work/$DeadLetterQueue")
}
