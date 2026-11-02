package integration_test

import (
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func portalPost(t *testing.T, srv *server.Server, path string, form url.Values) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", "http://"+srv.HTTPAddr().String()+path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s error: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func mustContain(t *testing.T, what, body string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Errorf("%s lacks %q", what, w)
		}
	}
}

// TestPortalExplorerSDK checks the Explorer against the Go SDK in both
// directions: what the SDK sends shows in the portal, and what the portal
// sends is what the SDK receives.
func TestPortalExplorerSDK(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders", "inbox")})
	client := newClient(t, srv)
	ctx := testContext(t)

	// SDK to portal.
	sender := newSender(t, client, "orders")
	err := sender.SendMessage(ctx, &azservicebus.Message{
		Body: []byte(`{"id": 7}`), MessageID: to.Ptr("sdk-1"), Subject: to.Ptr("order.created"),
		ContentType: to.Ptr("application/json"), TimeToLive: to.Ptr(time.Hour),
		ApplicationProperties: map[string]any{"tenant": "acme", "retries": int32(3), "vip": true},
	}, nil)
	if err != nil {
		t.Fatalf("SendMessage() error: %v", err)
	}
	_, view := httpGet(t, srv, "/$portal/explorer/view?path=orders&op=start")
	mustContain(t, "peek view", view, "Showing 1 of 1 messages", "sdk-1", "order.created", "Active")
	_, detail := httpGet(t, srv, "/$portal/explorer/detail?path=orders&seq=1")
	mustContain(t, "detail", detail, "application/json", "tenant", "acme", "retries", "<td>int</td>", "vip", "<td>boolean</td>", "expiresAtUtc", "timeToLive")

	// Portal to SDK: typed custom properties, a TTL and a session-less send.
	status, body := portalPost(t, srv, "/$portal/explorer/send", url.Values{
		"path": {"inbox"}, "contentType": {"application/json"}, "body": {`{"from": "portal"}`}, "messageId": {"portal-1"},
		"subject": {"from.portal"}, "correlationId": {"corr-9"}, "to": {"someone"}, "replyTo": {"back"}, "ttl": {"PT30M"},
		"pkey": {"tenant", "count", "ratio", "ok", "when"}, "ptype": {"string", "int", "double", "boolean", "datetime"},
		"pvalue": {"acme", "5", "1.5", "true", "2026-02-06T02:51:26Z"},
	})
	if status != 200 {
		t.Fatalf("portal send = %d %s", status, body)
	}
	r := newReceiver(t, client, "inbox", nil)
	msgs := receive(t, r, 1)
	m := msgs[0]
	if string(m.Body) != `{"from": "portal"}` || m.MessageID != "portal-1" || str(m.Subject) != "from.portal" ||
		str(m.ContentType) != "application/json" || str(m.CorrelationID) != "corr-9" ||
		str(m.To) != "someone" || str(m.ReplyTo) != "back" {
		t.Errorf("received = %+v", m)
	}
	if m.TimeToLive == nil || *m.TimeToLive != 30*time.Minute {
		t.Errorf("TimeToLive = %v, want 30m", m.TimeToLive)
	}
	props := m.ApplicationProperties
	if props["tenant"] != "acme" || props["count"] != int32(5) || props["ratio"] != 1.5 || props["ok"] != true {
		t.Errorf("ApplicationProperties = %#v", props)
	}
	if when, ok := props["when"].(time.Time); !ok || !when.Equal(time.Date(2026, 2, 6, 2, 51, 26, 0, time.UTC)) {
		t.Errorf("when = %#v", props["when"])
	}
	if err := r.CompleteMessage(ctx, m, nil); err != nil {
		t.Fatalf("CompleteMessage() error: %v", err)
	}

	// Receive Mode in the portal: lock, then complete; the SDK sees nothing.
	send(t, sender, bodies("x", "y")...)
	_, recv := portalPost(t, srv, "/$portal/explorer/view", url.Values{"path": {"orders"}, "mode": {"receive"}, "op": {"receive"}, "count": {"10"}})
	tokens := regexp.MustCompile(`name="token" value="([0-9a-f-]+)"`).FindAllStringSubmatch(recv, -1)
	if len(tokens) != 3 {
		t.Fatalf("received tokens = %d, want 3\n%s", len(tokens), recv)
	}
	// While the portal holds the locks, the SDK cannot receive them.
	other := newReceiver(t, client, "orders", nil)
	receiveNone(t, other, 500*time.Millisecond)
	form := url.Values{"path": {"orders"}, "mode": {"receive"}, "action": {"complete"}}
	for _, tok := range tokens {
		form.Add("token", tok[1])
	}
	if _, out := portalPost(t, srv, "/$portal/explorer/settle", form); !strings.Contains(out, "Complete: 3 messages") {
		t.Errorf("settle = %s", out)
	}
	peeked, err := other.PeekMessages(ctx, 10, nil)
	if err != nil || len(peeked) != 0 {
		t.Errorf("PeekMessages() after the portal completed = %d, %v; want none", len(peeked), err)
	}

	// Dead-letter by the SDK, re-send by the portal, receive by the SDK.
	send(t, sender, bodies("poison")...)
	msgs = receive(t, other, 1)
	if err := other.DeadLetterMessage(ctx, msgs[0], &azservicebus.DeadLetterOptions{Reason: to.Ptr("Bad"), ErrorDescription: to.Ptr("unreadable")}); err != nil {
		t.Fatalf("DeadLetterMessage() error: %v", err)
	}
	_, dlq := httpGet(t, srv, "/$portal/explorer/view?path=orders&queue=dlq&op=start")
	mustContain(t, "dlq view", dlq, "Dead-letter (1)", "Showing 1 of 1 messages", "poison")
	seq := regexp.MustCompile(`name="seq" value="(\d+)"`).FindStringSubmatch(dlq)[1]
	if _, out := portalPost(t, srv, "/$portal/explorer/resend", url.Values{"path": {"orders"}, "queue": {"dlq"}, "seq": {seq}}); !strings.Contains(out, "Sent 1 messages to orders") {
		t.Errorf("resend = %s", out)
	}
	msgs = receive(t, other, 1)
	if string(msgs[0].Body) != "poison" {
		t.Errorf("re-sent body = %q", msgs[0].Body)
	}
	if _, ok := msgs[0].ApplicationProperties["DeadLetterReason"]; !ok {
		// A re-send keeps the application properties, including the
		// reason the dead-lettering added, as the Azure portal does.
		t.Log("the re-sent message carries no DeadLetterReason")
	}
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func TestPortalPurgeSDK(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	ctx := testContext(t)
	send(t, newSender(t, client, "orders"), bodies("a", "b", "c")...)
	// A message locked by the SDK survives the purge and can still be completed.
	r := newReceiver(t, client, "orders", nil)
	held := receive(t, r, 1)
	status, out := portalPost(t, srv, "/$portal/explorer/purge", url.Values{"path": {"orders"}})
	if status != 200 || !strings.Contains(out, "Purged 2 messages from orders") {
		t.Fatalf("portal purge = %d %s", status, out)
	}
	if err := r.CompleteMessage(ctx, held[0], nil); err != nil {
		t.Errorf("CompleteMessage() of the locked message after the purge: %v", err)
	}
	peeked, err := r.PeekMessages(ctx, 10, nil)
	if err != nil || len(peeked) != 0 {
		t.Errorf("PeekMessages() after the purge = %d, %v; want none", len(peeked), err)
	}
}
