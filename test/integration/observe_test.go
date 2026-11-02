package integration_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func httpGet(t *testing.T, srv *server.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + srv.HTTPAddr().String() + path)
	if err != nil {
		t.Fatalf("GET %s error: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return resp.StatusCode, string(body)
}

// metricSum adds up one metric of an entity from /$api/metrics.
func metricSum(t *testing.T, srv *server.Server, entity, name string) int64 {
	t.Helper()
	status, body := httpGet(t, srv, "/$api/metrics?entity="+entity)
	if status != http.StatusOK {
		t.Fatalf("GET /$api/metrics?entity=%s = %d: %s", entity, status, body)
	}
	var out struct {
		Metrics map[string][]struct{ Value int64 }
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decoding metrics: %v\n%s", err, body)
	}
	points, ok := out.Metrics[name]
	if !ok || len(points) == 0 {
		t.Fatalf("metric %s missing in %s", name, body)
	}
	var n int64
	for _, p := range points {
		n += p.Value
	}
	return n
}

func TestMetricsAndActivitySDK(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders"), Prometheus: true})
	client := newClient(t, srv)
	ac := newAdminClient(t, srv)
	ctx := testContext(t)

	sender := newSender(t, client, "orders")
	send(t, sender, bodies("a", "b", "c")...)
	r := newReceiver(t, client, "orders", nil)
	got := receive(t, r, 3)
	for _, m := range got {
		if err := r.CompleteMessage(ctx, m, nil); err != nil {
			t.Fatalf("CompleteMessage() error: %v", err)
		}
	}
	send(t, sender, bodies("d")...)

	for name, want := range map[string]int64{"IncomingMessages": 4, "OutgoingMessages": 3} {
		if got := metricSum(t, srv, "orders", name); got != want {
			t.Errorf("orders %s = %d, want %d", name, got, want)
		}
		if got := metricSum(t, srv, "", name); got != want {
			t.Errorf("namespace %s = %d, want %d", name, got, want)
		}
	}
	if got := metricSum(t, srv, "orders", "SuccessfulRequests"); got == 0 {
		t.Error("SuccessfulRequests = 0, want > 0")
	}
	if got := metricSum(t, srv, "", "ConnectionsOpened"); got == 0 {
		t.Error("ConnectionsOpened = 0, want > 0")
	}
	if status, _ := httpGet(t, srv, "/$api/metrics?entity=nothing"); status != http.StatusNotFound {
		t.Errorf("metrics of an unknown entity = %d, want 404", status)
	}

	// Prometheus text.
	status, text := httpGet(t, srv, "/$metrics")
	if status != http.StatusOK {
		t.Fatalf("GET /$metrics = %d", status)
	}
	for _, want := range []string{
		`servicebus_incoming_messages_total{entity="orders"} 4`,
		`servicebus_outgoing_messages_total{entity="orders"} 3`,
		"servicebus_incoming_messages_total 4",
		`servicebus_active_messages{entity="orders"} 1`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Errorf("/$metrics lacks %q:\n%s", want, text)
		}
	}

	// Administrative operations of both clients reach the activity log.
	if _, err := ac.CreateQueue(ctx, "created-by-admin", nil); err != nil {
		t.Fatalf("CreateQueue() error: %v", err)
	}
	if _, err := ac.CreateQueue(ctx, "created-by-admin", nil); err == nil {
		t.Fatal("second CreateQueue() succeeded")
	}
	if _, err := ac.DeleteQueue(ctx, "created-by-admin", nil); err != nil {
		t.Fatalf("DeleteQueue() error: %v", err)
	}
	type entry struct{ Actor, Operation, Entity, Status string }
	var entries []entry
	_, body := httpGet(t, srv, "/$api/activity?entity=created-by-admin")
	if err := json.Unmarshal([]byte(body), &entries); err != nil {
		t.Fatalf("decoding activity: %v\n%s", err, body)
	}
	want := []entry{ // newest first
		{"atom", "DeleteQueue", "created-by-admin", "Succeeded"},
		{"atom", "CreateQueue", "created-by-admin", "Failed"},
		{"atom", "CreateQueue", "created-by-admin", "Succeeded"},
	}
	if len(entries) != len(want) {
		t.Fatalf("activity = %v, want %v", entries, want)
	}
	for i := range want {
		if entries[i] != want[i] {
			t.Errorf("activity[%d] = %v, want %v", i, entries[i], want[i])
		}
	}
	// Declared entities carry the config actor.
	_, body = httpGet(t, srv, "/$api/activity?actor=config&operation=CreateQueue")
	if !strings.Contains(body, `"entity":"orders"`) {
		t.Errorf("config activity lacks orders: %s", body)
	}
	if status, _ := httpGet(t, srv, "/$api/activity?limit=0"); status != http.StatusBadRequest {
		t.Errorf("limit=0 status = %d, want 400", status)
	}
}

func TestMetricsEndpointsOffByDefault(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{})
	// Without -prometheus the path is not served; the admin API answers.
	if status, _ := httpGet(t, srv, "/$metrics"); status == http.StatusOK {
		t.Errorf("/$metrics = %d without Prometheus, want an error", status)
	}
}
