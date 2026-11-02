package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/export"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/mcp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// mcpClient is a minimal MCP client over Streamable HTTP.
type mcpClient struct {
	t    *testing.T
	url  string
	next int
}

// startMCP serves the MCP endpoint for srv with every tool on.
func startMCP(t *testing.T, srv *server.Server, edit ...func(*mcp.Options)) *mcpClient {
	t.Helper()
	opts := mcp.Options{Addr: "127.0.0.1:0", Emulator: srv, AllowData: true}
	for _, e := range edit {
		e(&opts)
	}
	m := mcp.New(opts)
	if err := m.Start(); err != nil {
		t.Fatalf("mcp Start() error: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return &mcpClient{t: t, url: "http://" + m.Addr().String() + "/mcp"}
}

// post sends a raw body and returns the status and the answer.
func (c *mcpClient) post(body string, header http.Header) (int, string) {
	c.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, c.url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("POST error: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// rpc calls method and returns its result, or the JSON-RPC error.
func (c *mcpClient) rpc(method string, params any) (map[string]any, map[string]any) {
	c.t.Helper()
	c.next++
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": c.next, "method": method, "params": params})
	status, out := c.post(string(body), nil)
	if status != http.StatusOK {
		c.t.Fatalf("%s: status %d: %s", method, status, out)
	}
	var r struct {
		Result map[string]any `json:"result"`
		Error  map[string]any `json:"error"`
	}
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		c.t.Fatalf("%s: bad answer %q: %v", method, out, err)
	}
	return r.Result, r.Error
}

// call runs a tool and returns what it reports; isError results are
// returned as a failure message.
func (c *mcpClient) call(tool string, args map[string]any) (map[string]any, string) {
	c.t.Helper()
	if args == nil {
		args = map[string]any{}
	}
	res, rpcErr := c.rpc("tools/call", map[string]any{"name": tool, "arguments": args})
	if rpcErr != nil {
		c.t.Fatalf("%s: protocol error: %v", tool, rpcErr)
	}
	content, _ := res["content"].([]any)
	text := ""
	if len(content) > 0 {
		text, _ = content[0].(map[string]any)["text"].(string)
	}
	if res["isError"] == true {
		return nil, text
	}
	sc, _ := res["structuredContent"].(map[string]any)
	if sc == nil {
		c.t.Fatalf("%s: no structuredContent in %v", tool, res)
	}
	return sc, ""
}

// ok runs a tool that must succeed.
func (c *mcpClient) ok(tool string, args map[string]any) map[string]any {
	c.t.Helper()
	out, errText := c.call(tool, args)
	if errText != "" {
		c.t.Fatalf("%s(%v) failed: %s", tool, args, errText)
	}
	return out
}

// fails runs a tool that must fail with a message holding want.
func (c *mcpClient) fails(tool string, args map[string]any, want string) {
	c.t.Helper()
	out, errText := c.call(tool, args)
	if errText == "" {
		c.t.Fatalf("%s(%v) = %v, want an error holding %q", tool, args, out, want)
	}
	if !strings.Contains(errText, want) {
		c.t.Errorf("%s(%v) error = %q, want it to hold %q", tool, args, errText, want)
	}
}

func (c *mcpClient) toolNames() map[string]bool {
	c.t.Helper()
	res, _ := c.rpc("tools/list", nil)
	names := map[string]bool{}
	for _, tl := range res["tools"].([]any) {
		names[tl.(map[string]any)["name"].(string)] = true
	}
	return names
}

func items(m map[string]any) []map[string]any {
	var out []map[string]any
	for _, x := range m["items"].([]any) {
		out = append(out, x.(map[string]any))
	}
	return out
}

func pathsOf(m map[string]any) []string {
	var out []string
	for _, it := range items(m) {
		out = append(out, it["path"].(string))
	}
	return out
}

func TestMCPProtocol(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	c := startMCP(t, srv)

	res, rpcErr := c.rpc("initialize", map[string]any{"protocolVersion": "2024-11-05"})
	if rpcErr != nil || res["protocolVersion"] != "2024-11-05" {
		t.Errorf("initialize(2024-11-05) = %v, %v, want the version echoed", res, rpcErr)
	}
	res, _ = c.rpc("initialize", map[string]any{"protocolVersion": "1999-01-01"})
	if res["protocolVersion"] != "2025-06-18" {
		t.Errorf("initialize(unknown) version = %v, want the newest, 2025-06-18", res["protocolVersion"])
	}
	if info := res["serverInfo"].(map[string]any); info["name"] != "open-service-bus" {
		t.Errorf("serverInfo = %v", info)
	}
	if res, rpcErr := c.rpc("ping", nil); rpcErr != nil || len(res) != 0 {
		t.Errorf("ping = %v, %v, want {}", res, rpcErr)
	}
	if _, rpcErr := c.rpc("no/such", nil); rpcErr == nil || rpcErr["code"] != float64(-32601) {
		t.Errorf("unknown method error = %v, want code -32601", rpcErr)
	}
	if _, rpcErr := c.rpc("tools/call", map[string]any{"name": "no_such"}); rpcErr == nil || rpcErr["code"] != float64(-32602) {
		t.Errorf("unknown tool error = %v, want code -32602", rpcErr)
	}
	// A notification has no answer.
	if status, body := c.post(`{"jsonrpc":"2.0","method":"notifications/initialized"}`, nil); status != http.StatusAccepted || body != "" {
		t.Errorf("notification = %d %q, want 202 and no body", status, body)
	}
	// A batch answers its requests only.
	status, body := c.post(`[{"jsonrpc":"2.0","id":1,"method":"ping"},{"jsonrpc":"2.0","method":"notifications/x"},{"jsonrpc":"2.0","id":2,"method":"ping"}]`, nil)
	var batch []map[string]any
	if json.Unmarshal([]byte(body), &batch); status != http.StatusOK || len(batch) != 2 {
		t.Errorf("batch = %d %q, want two answers", status, body)
	}
	if status, body := c.post(`{not json`, nil); status != http.StatusOK || !strings.Contains(body, "-32700") {
		t.Errorf("bad JSON = %d %q, want a parse error", status, body)
	}
	// Only POST is served.
	resp, err := http.Get(c.url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", resp.StatusCode)
	}
	// A page of another site must not drive the emulator.
	if status, _ := c.post(`{"jsonrpc":"2.0","id":1,"method":"ping"}`, http.Header{"Origin": {"https://evil.example"}}); status != http.StatusForbidden {
		t.Errorf("foreign Origin status = %d, want 403", status)
	}
	if status, _ := c.post(`{"jsonrpc":"2.0","id":1,"method":"ping"}`, http.Header{"Origin": {"http://localhost:3000"}}); status != http.StatusOK {
		t.Errorf("localhost Origin status = %d, want 200", status)
	}
	// Bad arguments are reported to the model, not as protocol errors.
	c.fails("get_entity", map[string]any{"bogus": 1}, "invalid arguments")
	c.fails("get_entity", map[string]any{"path": "missing"}, "not found")
	c.fails("get_entity", map[string]any{"path": "orders", "namespace": "nope"}, "namespace \"nope\" not found")
}

func TestMCPToolGating(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})

	noData := startMCP(t, srv, func(o *mcp.Options) { o.AllowData = false }).toolNames()
	for _, name := range []string{"peek_messages", "send_message", "purge_entity", "requeue_dead_letters", "get_session_state"} {
		if noData[name] {
			t.Errorf("%s is offered without -mcp-allow-data", name)
		}
	}
	for _, name := range []string{"create_queue", "list_entities", "apply_config", "reset_namespace"} {
		if !noData[name] {
			t.Errorf("%s is missing", name)
		}
	}

	ro := startMCP(t, srv, func(o *mcp.Options) { o.ReadOnly = true })
	names := ro.toolNames()
	for _, name := range []string{"create_queue", "delete_entity", "send_message", "apply_config", "set_lock", "purge_entity", "geo_dr_pair"} {
		if names[name] {
			t.Errorf("%s is offered in read-only mode", name)
		}
	}
	for _, name := range []string{"list_entities", "get_entity", "peek_messages", "diff_config", "export_config", "test_rule"} {
		if !names[name] {
			t.Errorf("%s is missing in read-only mode", name)
		}
	}
	if _, rpcErr := ro.rpc("tools/call", map[string]any{"name": "create_queue", "arguments": map[string]any{"name": "x"}}); rpcErr == nil {
		t.Error("create_queue worked in read-only mode")
	}

	full := startMCP(t, srv)
	res, _ := full.rpc("tools/list", nil)
	byName := map[string]map[string]any{}
	for _, tl := range res["tools"].([]any) {
		m := tl.(map[string]any)
		byName[m["name"].(string)] = m
	}
	hints := func(name string) map[string]any { return byName[name]["annotations"].(map[string]any) }
	if hints("delete_entity")["destructiveHint"] != true || hints("delete_entity")["readOnlyHint"] != false {
		t.Errorf("delete_entity hints = %v", hints("delete_entity"))
	}
	if hints("list_entities")["readOnlyHint"] != true || hints("list_entities")["destructiveHint"] != false {
		t.Errorf("list_entities hints = %v", hints("list_entities"))
	}
	schema := byName["create_queue"]["inputSchema"].(map[string]any)
	if diff := cmp.Diff([]any{"name"}, schema["required"]); diff != "" {
		t.Errorf("create_queue required mismatch (-want +got):\n%s", diff)
	}
	if _, ok := schema["properties"].(map[string]any)["namespace"]; !ok {
		t.Errorf("create_queue schema has no namespace property: %v", schema)
	}
}

func TestMCPEntities(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{})
	c := startMCP(t, srv)
	admin := newAdminClient(t, srv)
	ctx := testContext(t)

	q := c.ok("create_queue", map[string]any{"name": "mcp-orders", "properties": map[string]any{
		"lockDuration": "PT45M", "maxDeliveryCount": 3, "requiresSession": false,
		"defaultMessageTimeToLive": 172800, "maxSizeInMegabytes": 4096,
	}})
	props := q["properties"].(map[string]any)
	if props["lockDuration"] != "PT45M" || props["maxDeliveryCount"] != float64(3) || props["defaultMessageTimeToLive"] != "P2D" {
		t.Errorf("create_queue properties = %v", props)
	}
	// What the MCP made is what the SDK's administration client sees, with
	// no cap at the limits of the official emulator.
	got, err := admin.GetQueue(ctx, "mcp-orders", nil)
	if err != nil || got == nil {
		t.Fatalf("GetQueue() = %v, %v", got, err)
	}
	if *got.LockDuration != "PT45M" || *got.MaxDeliveryCount != 3 || *got.MaxSizeInMegabytes != 4096 {
		t.Errorf("admin sees LockDuration=%s MaxDeliveryCount=%d MaxSize=%d", *got.LockDuration, *got.MaxDeliveryCount, *got.MaxSizeInMegabytes)
	}

	c.fails("create_queue", map[string]any{"name": "mcp-orders"}, "already exists")
	again := c.ok("create_queue", map[string]any{"name": "mcp-orders", "if_not_exists": true, "properties": map[string]any{"maxDeliveryCount": 99}})
	if again["properties"].(map[string]any)["maxDeliveryCount"] != float64(3) {
		t.Errorf("if_not_exists changed the queue: %v", again["properties"])
	}
	c.fails("create_queue", map[string]any{"name": "bad", "properties": map[string]any{"lockDurration": "PT1M"}}, "unknown property")
	c.fails("create_queue", map[string]any{"name": "bad", "properties": map[string]any{"lockDuration": "five"}}, "lockDuration")
	c.fails("create_queue", map[string]any{"name": "bad", "properties": map[string]any{"maxDeliveryCount": 0}}, "max delivery count")
	c.fails("create_queue", map[string]any{"name": "bad name"}, "")

	up := c.ok("update_entity", map[string]any{"path": "mcp-orders", "properties": map[string]any{"maxDeliveryCount": 8, "forwardTo": ""}})
	if up["properties"].(map[string]any)["maxDeliveryCount"] != float64(8) || up["properties"].(map[string]any)["lockDuration"] != "PT45M" {
		t.Errorf("update_entity properties = %v, want maxDeliveryCount 8 and lockDuration kept", up["properties"])
	}

	c.ok("create_topic", map[string]any{"name": "mcp-events"})
	c.ok("create_subscription", map[string]any{"topic": "mcp-events", "name": "audit", "properties": map[string]any{"maxDeliveryCount": 2}})
	c.ok("create_subscription", map[string]any{"topic": "mcp-events", "name": "billing"})
	c.fails("create_subscription", map[string]any{"topic": "nope", "name": "x"}, "not found")

	if diff := cmp.Diff([]string{"mcp-orders"}, pathsOf(c.ok("list_entities", map[string]any{"kind": "queue"}))); diff != "" {
		t.Errorf("queues mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"mcp-events/Subscriptions/audit", "mcp-events/Subscriptions/billing"}, pathsOf(c.ok("list_entities", map[string]any{"topic": "mcp-events"}))); diff != "" {
		t.Errorf("subscriptions mismatch (-want +got):\n%s", diff)
	}
	if got := pathsOf(c.ok("list_entities", map[string]any{"query": "EVENTS"})); len(got) != 3 {
		t.Errorf("query=EVENTS found %v, want the topic and its two subscriptions", got)
	}
	sub := c.ok("get_entity", map[string]any{"path": "mcp-events/Subscriptions/audit"})
	if rules := sub["rules"].([]any); len(rules) != 1 || rules[0].(map[string]any)["name"] != "$Default" {
		t.Errorf("subscription rules = %v, want $Default", sub["rules"])
	}

	// The SDK sees the messages in the counts.
	client := newClient(t, srv)
	send(t, newSender(t, client, "mcp-orders"), bodies("a", "b", "c")...)
	if n := c.ok("get_entity", map[string]any{"path": "mcp-orders"})["counts"].(map[string]any)["active"]; n != float64(3) {
		t.Errorf("active = %v, want 3", n)
	}

	c.fails("delete_entity", map[string]any{"path": "mcp-events"}, "confirm")
	del := c.ok("delete_entity", map[string]any{"path": "mcp-events", "confirm": true})
	if diff := cmp.Diff([]any{"mcp-events", "mcp-events/Subscriptions/audit", "mcp-events/Subscriptions/billing"}, del["deleted"]); diff != "" {
		t.Errorf("deleted mismatch (-want +got):\n%s", diff)
	}
	if _, err := admin.GetTopic(ctx, "mcp-events", nil); err != nil && responseStatus(err) != 404 {
		t.Errorf("GetTopic() error: %v", err)
	}
	if got, _ := admin.GetTopic(ctx, "mcp-events", nil); got != nil {
		t.Error("the topic is still there for the admin client")
	}
}

func TestMCPRules(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Topics: []server.Topic{{Name: "ev", Subscriptions: []server.Subscription{{Name: "red"}, {Name: "eu"}, {Name: "all"}}}}})
	c := startMCP(t, srv)
	client := newClient(t, srv)
	sub := func(name string) string { return "ev/Subscriptions/" + name }

	c.ok("delete_rule", map[string]any{"path": sub("red"), "name": "$Default"})
	c.ok("add_rule", map[string]any{"path": sub("red"), "rule": map[string]any{
		"name": "red", "sql": "color = 'red' AND qty > @min", "parameters": map[string]any{"min": 2},
		"action": "SET picked = TRUE",
	}})
	c.ok("delete_rule", map[string]any{"path": sub("eu"), "name": "$Default"})
	c.ok("add_rule", map[string]any{"path": sub("eu"), "rule": map[string]any{
		"name": "eu", "correlation": map[string]any{"subject": "order", "properties": map[string]any{"region": "eu"}},
	}})
	c.fails("add_rule", map[string]any{"path": sub("red"), "rule": map[string]any{"name": "red", "sql": "1 = 1"}}, "")
	c.fails("add_rule", map[string]any{"path": sub("red"), "rule": map[string]any{"name": "bad", "sql": "color ="}}, "bad")
	c.fails("add_rule", map[string]any{"path": sub("red"), "rule": map[string]any{"name": "bad", "filter": "Weird"}}, "unknown filter")

	rules := items(c.ok("list_rules", map[string]any{"path": sub("red")}))
	if len(rules) != 1 || rules[0]["sql"] != "color = 'red' AND qty > @min" || rules[0]["action"] != "SET picked = TRUE" {
		t.Errorf("list_rules = %v", rules)
	}
	if r := c.ok("get_rule", map[string]any{"path": sub("eu"), "name": "eu"}); r["filter"] != "Correlation" {
		t.Errorf("get_rule = %v, want a Correlation filter", r)
	}

	// test_rule predicts the routing without sending anything.
	res := c.ok("test_rule", map[string]any{"path": sub("red"), "properties": map[string]any{"color": "red", "qty": 5}})
	r0 := res["results"].([]any)[0].(map[string]any)
	if r0["matched"] != true || r0["after"].(map[string]any)["properties"].(map[string]any)["picked"] != true {
		t.Errorf("test_rule(red, qty 5) = %v, want a match with picked set", r0)
	}
	res = c.ok("test_rule", map[string]any{"path": sub("red"), "properties": map[string]any{"color": "red", "qty": 1}})
	if res["results"].([]any)[0].(map[string]any)["matched"] != false {
		t.Errorf("test_rule(red, qty 1) = %v, want no match", res)
	}
	res = c.ok("test_rule", map[string]any{"rules": []any{map[string]any{"name": "t", "sql": "sys.Label = 'x'"}}, "system": map[string]any{"Subject": "x"}})
	if res["results"].([]any)[0].(map[string]any)["matched"] != true {
		t.Errorf("test_rule(inline) = %v, want a match", res)
	}
	c.fails("test_rule", map[string]any{"path": sub("red"), "system": map[string]any{"Bogus": 1}}, "unknown system property")
	c.fails("test_rule", map[string]any{}, "give the path")

	// The prediction holds for the real topic.
	send(t, newSender(t, client, "ev"),
		&azservicebus.Message{Body: []byte("m1"), Subject: to.Ptr("order"), ApplicationProperties: map[string]any{"color": "red", "qty": int64(5), "region": "eu"}},
		&azservicebus.Message{Body: []byte("m2"), Subject: to.Ptr("order"), ApplicationProperties: map[string]any{"color": "blue", "qty": int64(5), "region": "us"}},
		&azservicebus.Message{Body: []byte("m3"), Subject: to.Ptr("order"), ApplicationProperties: map[string]any{"color": "red", "qty": int64(1), "region": "eu"}},
	)
	red := receive(t, newSubReceiver(t, client, "ev", "red", nil), 1)
	if string(red[0].Body) != "m1" || red[0].ApplicationProperties["picked"] != true {
		t.Errorf("red got %q %v, want m1 with picked", red[0].Body, red[0].ApplicationProperties)
	}
	eu := newSubReceiver(t, client, "ev", "eu", nil)
	if diff := cmp.Diff([]string{"m1", "m3"}, bodiesOf(receive(t, eu, 2))); diff != "" {
		t.Errorf("eu mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"m1", "m2", "m3"}, bodiesOf(receive(t, newSubReceiver(t, client, "ev", "all", nil), 3))); diff != "" {
		t.Errorf("all mismatch (-want +got):\n%s", diff)
	}

	c.ok("update_rule", map[string]any{"path": sub("red"), "rule": map[string]any{"name": "red", "sql": "color = 'blue'"}})
	send(t, newSender(t, client, "ev"), &azservicebus.Message{Body: []byte("m4"), ApplicationProperties: map[string]any{"color": "blue"}})
	if got := bodiesOf(receive(t, newSubReceiver(t, client, "ev", "red", nil), 1)); got[0] != "m4" {
		t.Errorf("red after update_rule got %v, want m4", got)
	}
	c.ok("set_rules", map[string]any{"path": sub("red"), "rules": []any{
		map[string]any{"name": "one", "sql": "n = 1"}, map[string]any{"name": "any"},
	}})
	if diff := cmp.Diff(2, len(items(c.ok("list_rules", map[string]any{"path": sub("red")})))); diff != "" {
		t.Errorf("set_rules left a wrong number of rules (-want +got):\n%s", diff)
	}
	c.ok("delete_rule", map[string]any{"path": sub("red"), "name": "one"})
	c.fails("get_rule", map[string]any{"path": sub("red"), "name": "one"}, "not found")
}

func TestMCPMessages(t *testing.T) {
	t.Parallel()
	dlq := broker.DefaultProperties(broker.KindQueue)
	srv := startServer(t, server.Options{
		Queues: []server.Queue{{Name: "qpeek"}, {Name: "qsend"}, {Name: "qpurge"}, {Name: "qbody"}, {Name: "dead", Properties: dlq}, {Name: "sched"}, {Name: "sess", Properties: func() broker.Properties { p := dlq; p.RequiresSession = true; return p }()}},
		Topics: []server.Topic{{Name: "t", Subscriptions: []server.Subscription{{Name: "s1"}, {Name: "s2"}}}},
	})
	c := startMCP(t, srv)
	client := newClient(t, srv)

	t.Run("peek", func(t *testing.T) {
		send(t, newSender(t, client, "qpeek"), &azservicebus.Message{
			Body: []byte(`{"id":7}`), MessageID: to.Ptr("m-7"), ContentType: to.Ptr("application/json"), Subject: to.Ptr("hello"),
			CorrelationID: to.Ptr("corr"), ApplicationProperties: map[string]any{"n": int64(3), "ok": true, "s": "text"},
		})
		got := items(c.ok("peek_messages", map[string]any{"path": "qpeek"}))
		if len(got) != 1 {
			t.Fatalf("peek_messages = %v, want one message", got)
		}
		m := got[0]
		body := m["body"].(map[string]any)
		if m["messageId"] != "m-7" || m["subject"] != "hello" || m["contentType"] != "application/json" || m["correlationId"] != "corr" ||
			body["text"] != `{"id":7}` || body["encoding"] != "utf8" || m["state"] != "Active" {
			t.Errorf("message = %v", m)
		}
		if diff := cmp.Diff(map[string]any{"n": float64(3), "ok": true, "s": "text"}, m["applicationProperties"]); diff != "" {
			t.Errorf("applicationProperties mismatch (-want +got):\n%s", diff)
		}
		// Peeking leaves the message for the receiver.
		if got := bodiesOf(receive(t, newReceiver(t, client, "qpeek", nil), 1)); got[0] != `{"id":7}` {
			t.Errorf("receive after peek = %v", got)
		}
		cut := c.ok("peek_messages", map[string]any{"path": "qpeek", "from_sequence": 0, "max": 5})
		if cut["note"] == nil {
			t.Error("peek_messages carries no warning about untrusted content")
		}
	})

	t.Run("send", func(t *testing.T) {
		res := c.ok("send_message", map[string]any{
			"path": "qsend", "body": "from mcp", "content_type": "text/plain", "message_id": "mcp-1", "subject": "s", "correlation_id": "c1",
			"ttl": "PT1H", "properties": map[string]any{"count": 5, "ratio": 1.5, "flag": true, "name": "x"},
		})
		if res["sent"] != float64(1) {
			t.Fatalf("send_message = %v", res)
		}
		r := newReceiver(t, client, "qsend", nil)
		m := receive(t, r, 1)[0]
		if err := r.CompleteMessage(testContext(t), m, nil); err != nil {
			t.Fatalf("CompleteMessage() error: %v", err)
		}
		if string(m.Body) != "from mcp" || m.MessageID != "mcp-1" || *m.Subject != "s" || *m.CorrelationID != "c1" || *m.ContentType != "text/plain" {
			t.Errorf("received %q %+v", m.Body, m)
		}
		want := map[string]any{"count": int64(5), "ratio": 1.5, "flag": true, "name": "x"}
		if diff := cmp.Diff(want, m.ApplicationProperties); diff != "" {
			t.Errorf("ApplicationProperties mismatch (-want +got):\n%s", diff)
		}
		if m.TimeToLive == nil || *m.TimeToLive != time.Hour {
			t.Errorf("TimeToLive = %v, want 1h", m.TimeToLive)
		}

		batch := c.ok("send_batch", map[string]any{"path": "t", "messages": []any{
			map[string]any{"body": "b1"}, map[string]any{"body": "b2", "properties": map[string]any{"k": "v"}},
		}})
		if batch["sent"] != float64(2) {
			t.Errorf("send_batch = %v", batch)
		}
		for _, s := range []string{"s1", "s2"} {
			if diff := cmp.Diff([]string{"b1", "b2"}, bodiesOf(receive(t, newSubReceiver(t, client, "t", s, nil), 2))); diff != "" {
				t.Errorf("%s mismatch (-want +got):\n%s", s, diff)
			}
		}
		c.ok("send_message", map[string]any{"path": "qsend", "body": "x", "count": 4})
		if n := c.ok("get_entity", map[string]any{"path": "qsend"})["counts"].(map[string]any)["active"]; n != float64(4) {
			t.Errorf("active after count=4 = %v, want 4", n)
		}
		c.fails("send_message", map[string]any{"path": "qsend", "body": "x", "count": 2, "message_id": "same"}, "message_id")
		c.fails("send_message", map[string]any{"path": "nope", "body": "x"}, "not found")
		c.fails("send_message", map[string]any{"path": "qsend", "body": "x", "ttl": "soon"}, "ime to live")
		c.fails("send_message", map[string]any{"path": "qsend", "body": "x", "properties": map[string]any{"o": map[string]any{"a": 1}}}, "only text")
	})

	t.Run("purge", func(t *testing.T) {
		send(t, newSender(t, client, "qpurge"), bodies("1", "2", "3")...)
		c.fails("purge_entity", map[string]any{"path": "qpurge"}, "confirm")
		if n := c.ok("purge_entity", map[string]any{"path": "qpurge", "confirm": true})["deleted"]; n != float64(3) {
			t.Errorf("purged %v, want 3", n)
		}
		receiveNone(t, newReceiver(t, client, "qpurge", nil), 200*time.Millisecond)
	})

	t.Run("dead letters", func(t *testing.T) {
		r := newReceiver(t, client, "dead", nil)
		send(t, newSender(t, client, "dead"), bodies("bad1", "bad2")...)
		for _, m := range receive(t, r, 2) {
			if err := r.DeadLetterMessage(testContext(t), m, &azservicebus.DeadLetterOptions{Reason: to.Ptr("Poison"), ErrorDescription: to.Ptr("cannot parse " + string(m.Body))}); err != nil {
				t.Fatalf("DeadLetterMessage() error: %v", err)
			}
		}
		dead := items(c.ok("peek_messages", map[string]any{"path": "dead", "queue": "deadletter"}))
		if len(dead) != 2 || dead[0]["deadLetterReason"] != "Poison" || dead[0]["deadLetterErrorDescription"] != "cannot parse bad1" {
			t.Errorf("dead-letter peek = %v", dead)
		}
		if n := c.ok("get_entity", map[string]any{"path": "dead"})["counts"].(map[string]any)["deadLetter"]; n != float64(2) {
			t.Errorf("deadLetter count = %v, want 2", n)
		}
		ov := c.ok("get_overview", nil)
		if wd := ov["withDeadLetters"].([]any); len(wd) != 1 || wd[0].(map[string]any)["path"] != "dead" {
			t.Errorf("overview withDeadLetters = %v", ov["withDeadLetters"])
		}

		c.fails("requeue_dead_letters", map[string]any{"path": "dead"}, "confirm")
		res := c.ok("requeue_dead_letters", map[string]any{"path": "dead", "confirm": true, "max": 1})
		if res["moved"] != float64(1) {
			t.Errorf("requeue max=1 = %v", res)
		}
		res = c.ok("requeue_dead_letters", map[string]any{"path": "dead", "confirm": true})
		if res["moved"] != float64(1) {
			t.Errorf("requeue rest = %v", res)
		}
		if diff := cmp.Diff([]string{"bad1", "bad2"}, bodiesOf(receive(t, newReceiver(t, client, "dead", nil), 2))); diff != "" {
			t.Errorf("requeued mismatch (-want +got):\n%s", diff)
		}
		if n := c.ok("get_entity", map[string]any{"path": "dead"})["counts"].(map[string]any)["deadLetter"]; n != float64(0) {
			t.Errorf("deadLetter count after requeue = %v, want 0", n)
		}
	})

	t.Run("scheduled", func(t *testing.T) {
		at := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		res := c.ok("send_message", map[string]any{"path": "sched", "body": "later", "scheduled_enqueue_time": at})
		seqs := res["sequenceNumbers"].([]any)
		if len(seqs) != 1 {
			t.Fatalf("send_message = %v", res)
		}
		if n := c.ok("get_entity", map[string]any{"path": "sched"})["counts"].(map[string]any)["scheduled"]; n != float64(1) {
			t.Errorf("scheduled = %v, want 1", n)
		}
		peek := items(c.ok("peek_messages", map[string]any{"path": "sched"}))
		if len(peek) != 1 || peek[0]["state"] != "Scheduled" {
			t.Errorf("peek of scheduled = %v", peek)
		}
		c.ok("cancel_scheduled", map[string]any{"path": "sched", "sequence_numbers": seqs})
		if n := c.ok("get_entity", map[string]any{"path": "sched"})["counts"].(map[string]any)["scheduled"]; n != float64(0) {
			t.Errorf("scheduled after cancel = %v, want 0", n)
		}
	})

	t.Run("sessions", func(t *testing.T) {
		for _, id := range []string{"A", "B"} {
			c.ok("send_message", map[string]any{"path": "sess", "body": "in " + id, "session_id": id})
		}
		if diff := cmp.Diff([]any{"A", "B"}, c.ok("list_sessions", map[string]any{"path": "sess"})["items"]); diff != "" {
			t.Errorf("sessions mismatch (-want +got):\n%s", diff)
		}
		if got := items(c.ok("peek_messages", map[string]any{"path": "sess", "session_id": "B"})); len(got) != 1 || got[0]["sessionId"] != "B" {
			t.Errorf("peek of session B = %v", got)
		}
		c.ok("set_session_state", map[string]any{"path": "sess", "session_id": "A", "state": "step 2"})
		st := c.ok("get_session_state", map[string]any{"path": "sess", "session_id": "A"})
		if st["state"].(map[string]any)["text"] != "step 2" {
			t.Errorf("session state = %v", st)
		}
		c.ok("set_session_state", map[string]any{"path": "sess", "session_id": "A", "state": "AQID", "encoding": "base64"})
		if st := c.ok("get_session_state", map[string]any{"path": "sess", "session_id": "A"}); st["size"] != float64(3) {
			t.Errorf("binary session state = %v, want 3 bytes", st)
		}
	})

	t.Run("body limits", func(t *testing.T) {
		c.ok("send_message", map[string]any{"path": "qbody", "body": strings.Repeat("é", 100)})
		m := items(c.ok("peek_messages", map[string]any{"path": "qbody", "max_body_bytes": 25}))[0]["body"].(map[string]any)
		if m["truncated"] != true || m["size"] != float64(200) || strings.Contains(m["text"].(string), "�") {
			t.Errorf("truncated body = %v, want 200 bytes cut cleanly", m)
		}
		c.ok("purge_entity", map[string]any{"path": "qbody", "confirm": true})
	})
}

const mcpTerraform = `
resource "azurerm_servicebus_namespace" "main" {
  name                = "sb-mcp"
  location            = "East US"
  resource_group_name = "rg"
  sku                 = "Standard"
}
resource "azurerm_servicebus_queue" "inbox" {
  name               = "tf-inbox"
  namespace_id       = azurerm_servicebus_namespace.main.id
  lock_duration      = "PT3M"
  max_delivery_count = 12
}
resource "azurerm_servicebus_topic" "events" {
  name         = "tf-events"
  namespace_id = azurerm_servicebus_namespace.main.id
}
resource "azurerm_servicebus_subscription" "eu" {
  name               = "eu"
  topic_id           = azurerm_servicebus_topic.events.id
  max_delivery_count = 4
}
resource "azurerm_servicebus_subscription_rule" "eu" {
  name            = "only-eu"
  subscription_id = azurerm_servicebus_subscription.eu.id
  filter_type     = "SqlFilter"
  sql_filter      = "region = 'eu'"
}
`

func TestMCPConfig(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("keep"), Export: export.Files})
	c := startMCP(t, srv)
	client := newClient(t, srv)
	admin := newAdminClient(t, srv)
	ctx := testContext(t)
	changes := func(res map[string]any) []string {
		var out []string
		for _, x := range res["changes"].([]any) {
			m := x.(map[string]any)
			out = append(out, fmt.Sprintf("%s %s", m["action"], m["path"]))
		}
		return out
	}

	tf := map[string]any{"format": "terraform", "content": mcpTerraform}
	diff := c.ok("diff_config", tf)
	if diff["dryRun"] != true {
		t.Errorf("diff_config dryRun = %v", diff["dryRun"])
	}
	if d := cmp.Diff([]string{"create tf-inbox", "create tf-events", "create tf-events/Subscriptions/eu", "update tf-events/Subscriptions/eu"}, changes(diff)); d != "" {
		t.Errorf("diff mismatch (-want +got):\n%s", d)
	}
	if q, _ := admin.GetQueue(ctx, "tf-inbox", nil); q != nil {
		t.Error("diff_config created the queue")
	}

	applied := c.ok("apply_config", tf)
	if !strings.Contains(applied["summary"].(string), "3 create") {
		t.Errorf("apply_config summary = %v, want 3 creates", applied["summary"])
	}
	q, err := admin.GetQueue(ctx, "tf-inbox", nil)
	if err != nil || q == nil || *q.LockDuration != "PT3M" || *q.MaxDeliveryCount != 12 {
		t.Fatalf("GetQueue(tf-inbox) = %v, %v", q, err)
	}
	// The rule from the Terraform routes.
	send(t, newSender(t, client, "tf-events"),
		&azservicebus.Message{Body: []byte("eu"), ApplicationProperties: map[string]any{"region": "eu"}},
		&azservicebus.Message{Body: []byte("us"), ApplicationProperties: map[string]any{"region": "us"}},
	)
	if got := bodiesOf(receive(t, newSubReceiver(t, client, "tf-events", "eu", nil), 1)); got[0] != "eu" {
		t.Errorf("tf eu got %v", got)
	}

	// Applying again changes nothing; "keep", not declared, survives.
	for _, ch := range changes(c.ok("diff_config", tf)) {
		if !strings.HasPrefix(ch, "unchanged") {
			t.Errorf("second diff holds %q, want only unchanged entries", ch)
		}
	}
	if c.ok("get_entity", map[string]any{"path": "keep"})["path"] != "keep" {
		t.Error("keep is gone")
	}

	// A Config.json updates a property and prune deletes what it omits.
	cfg := `{"UserConfig":{"Namespaces":[{"Name":"emulator","Queues":[{"Name":"tf-inbox","Properties":{"MaxDeliveryCount":2}},{"Name":"fresh"}],"Topics":[]}]}}`
	pruneDiff := c.ok("diff_config", map[string]any{"format": "config_json", "content": cfg, "prune": true})
	want := []string{"update tf-inbox", "create fresh", "delete keep", "delete tf-events/Subscriptions/eu", "delete tf-events"}
	got := changes(pruneDiff)
	for _, w := range want {
		found := false
		for _, g := range got {
			found = found || g == w
		}
		if !found {
			t.Errorf("prune diff %v lacks %q", got, w)
		}
	}
	c.fails("apply_config", map[string]any{"format": "config_json", "content": cfg, "prune": true}, "confirm")
	c.ok("apply_config", map[string]any{"format": "config_json", "content": cfg, "prune": true, "confirm": true})
	if diff := cmp.Diff([]string{"fresh", "tf-inbox"}, pathsOf(c.ok("list_entities", nil))); diff != "" {
		t.Errorf("entities after prune mismatch (-want +got):\n%s", diff)
	}
	if q, _ := admin.GetQueue(ctx, "tf-inbox", nil); q == nil || *q.MaxDeliveryCount != 2 {
		t.Errorf("tf-inbox after apply = %+v, want MaxDeliveryCount 2", q)
	}
	c.fails("apply_config", map[string]any{"format": "config_json", "content": "{not json"}, "")
	c.fails("apply_config", map[string]any{"format": "terraform", "content": ""}, "give content")
	c.fails("apply_config", map[string]any{"format": "yaml", "content": "x"}, "format")
	// Terraform arriving over the port reads no file of this machine.
	c.fails("diff_config", map[string]any{"format": "terraform", "files": map[string]any{
		"main.tf": "module \"m\" {\n  source = \"../../..\"\n}\n",
	}}, "outside")
	c.fails("diff_config", map[string]any{"format": "terraform", "files": map[string]any{"../x.tf": "x"}}, "outside the directory")

	// export_config returns what a fresh emulator can start from.
	exp := c.ok("export_config", nil)
	if !strings.Contains(exp["config_json"].(string), "fresh") || !strings.Contains(exp["terraform"].(string), "azurerm_servicebus_queue") {
		t.Errorf("export_config = %v", exp)
	}
	if only := c.ok("export_config", map[string]any{"format": "terraform"}); only["config_json"] != nil {
		t.Errorf("export_config(terraform) also returned config_json")
	}

	// reset_namespace leaves a clean slate, or the initial declaration.
	send(t, newSender(t, client, "fresh"), bodies("x")...)
	c.fails("reset_namespace", nil, "confirm")
	res := c.ok("reset_namespace", map[string]any{"confirm": true, "restore_initial_config": false})
	if res["queues"] != float64(0) || res["topics"] != float64(0) {
		t.Errorf("reset (no restore) = %v", res)
	}
	c.ok("create_queue", map[string]any{"name": "again"})
	res = c.ok("reset_namespace", map[string]any{"confirm": true})
	if diff := cmp.Diff([]string{"keep"}, pathsOf(c.ok("list_entities", nil))); diff != "" {
		t.Errorf("entities after reset mismatch (-want +got):\n%s", diff)
	}
	if res["restored"] != true {
		t.Errorf("reset = %v", res)
	}
}

func TestMCPNamespacesLocksAndAdmin(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{
		Queues:     queues("orders"),
		Namespaces: []server.NamespaceOptions{{Name: "other", Queues: queues("remote")}},
		Prometheus: true,
	})
	c := startMCP(t, srv)

	ns := c.ok("list_namespaces", nil)["namespaces"]
	if diff := cmp.Diff([]any{"emulator (default)", "other"}, ns); diff != "" {
		t.Errorf("namespaces mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"remote"}, pathsOf(c.ok("list_entities", map[string]any{"namespace": "other"}))); diff != "" {
		t.Errorf("other namespace entities mismatch (-want +got):\n%s", diff)
	}
	c.ok("create_queue", map[string]any{"namespace": "other", "name": "second"})
	if diff := cmp.Diff([]string{"orders"}, pathsOf(c.ok("list_entities", nil))); diff != "" {
		t.Errorf("default namespace changed (-want +got):\n%s", diff)
	}

	info := c.ok("get_namespace_info", nil)
	if info["name"] != "emulator" || info["queues"] != float64(1) || !strings.Contains(info["connectionString"].(string), "UseDevelopmentEmulator=true") {
		t.Errorf("get_namespace_info = %v", info)
	}
	status := c.ok("get_server_status", nil)
	if status["address"] == nil || status["mcp"].(map[string]any)["allowData"] != true || status["certificate"] == nil {
		t.Errorf("get_server_status = %v", status)
	}
	if cert := c.ok("get_certificate", nil); !strings.HasPrefix(cert["pem"].(string), "-----BEGIN CERTIFICATE-----") {
		t.Errorf("get_certificate = %v", cert)
	}

	// Locks stop changes made through the MCP, as they stop the portal's.
	c.ok("set_lock", map[string]any{"name": "freeze", "level": "ReadOnly", "scope": "orders", "notes": "release"})
	c.fails("update_entity", map[string]any{"path": "orders", "properties": map[string]any{"maxDeliveryCount": 2}}, "")
	c.fails("delete_entity", map[string]any{"path": "orders", "confirm": true}, "")
	if locks := c.ok("list_locks", nil)["locks"].([]any); len(locks) != 1 || locks[0].(map[string]any)["name"] != "freeze" {
		t.Errorf("list_locks = %v", locks)
	}
	c.fails("set_lock", map[string]any{"name": "x", "level": "Both"}, "level")
	c.ok("delete_lock", map[string]any{"name": "freeze"})
	c.ok("update_entity", map[string]any{"path": "orders", "properties": map[string]any{"maxDeliveryCount": 2}})
	c.ok("set_tags", map[string]any{"tags": map[string]any{"env": "test"}})
	wantTags := map[string]any{"env": "test"}
	if diff := cmp.Diff(wantTags, c.ok("list_locks", nil)["tags"]); diff != "" {
		t.Errorf("tags mismatch (-want +got):\n%s", diff)
	}

	// Policies and tokens.
	c.ok("create_sas_policy", map[string]any{"name": "sender", "rights": []any{"Send"}, "scope": "orders"})
	rules := items(c.ok("list_authorization_rules", map[string]any{"all": true}))
	var found map[string]any
	for _, r := range rules {
		if r["name"] == "sender" {
			found = r
		}
	}
	if found == nil || found["scope"] != "orders" || found["primaryKey"] == "" {
		t.Errorf("list_authorization_rules = %v", rules)
	}
	tok := c.ok("generate_sas_token", map[string]any{"policy": "sender", "scope": "orders", "resource": "sb://localhost/orders", "ttl": 600})
	if !strings.HasPrefix(tok["token"].(string), "SharedAccessSignature sr=") || !strings.Contains(tok["token"].(string), "skn=sender") {
		t.Errorf("generate_sas_token = %v", tok)
	}
	c.fails("generate_sas_token", map[string]any{"policy": "ghost", "resource": "x"}, "not found")
	c.fails("create_sas_policy", map[string]any{"name": "p", "rights": []any{"Fly"}}, "")
	c.ok("delete_sas_policy", map[string]any{"name": "sender", "scope": "orders"})
	c.fails("issue_dev_token", map[string]any{"principal": "me"}, "dev-issuer")
}

func TestMCPObservability(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("watched")})
	c := startMCP(t, srv)
	client := newClient(t, srv)

	send(t, newSender(t, client, "watched"), bodies("1", "2", "3")...)
	receive(t, newReceiver(t, client, "watched", nil), 2)

	c.ok("create_queue", map[string]any{"name": "via-mcp"})
	log := items(c.ok("query_activity_log", map[string]any{"actor": "mcp"}))
	if len(log) != 1 || log[0]["operation"] != "CreateQueue" || log[0]["entity"] != "via-mcp" || log[0]["status"] != "Succeeded" {
		t.Errorf("activity log of mcp = %v", log)
	}
	if got := items(c.ok("query_activity_log", map[string]any{"entity": "via-mcp", "status": "Failed"})); len(got) != 0 {
		t.Errorf("failed entries = %v, want none", got)
	}
	c.fails("create_queue", map[string]any{"name": "via-mcp"}, "already exists")
	if got := items(c.ok("query_activity_log", map[string]any{"status": "Failed"})); len(got) == 0 {
		t.Error("the failed CreateQueue is not in the activity log")
	}
	c.fails("query_activity_log", map[string]any{"last": "yesterday"}, "last")

	// Counters come from the real traffic of the SDK.
	totals := c.ok("get_metric_totals", nil)
	var in float64
	for _, x := range totals["counters"].([]any) {
		m := x.(map[string]any)
		if m["metric"] == "IncomingMessages" && m["entity"] == "watched" {
			in = m["value"].(float64)
		}
	}
	if in != 3 {
		t.Errorf("IncomingMessages of watched = %v, want 3", in)
	}
	series := c.ok("get_metrics", map[string]any{"entity": "watched", "last": "PT30M", "step": "PT1M"})["series"].(map[string]any)
	if _, ok := series["IncomingMessages"]; !ok {
		t.Errorf("get_metrics series = %v, want IncomingMessages", series)
	}
	ov := c.ok("get_overview", nil)
	if b := ov["biggestBacklogs"].([]any); len(b) != 1 || b[0].(map[string]any)["path"] != "watched" || b[0].(map[string]any)["active"] != float64(3) {
		t.Errorf("biggestBacklogs = %v", ov["biggestBacklogs"])
	}
	c.ok("update_entity", map[string]any{"path": "via-mcp", "properties": map[string]any{"status": "SendDisabled"}})
	if na := c.ok("get_overview", nil)["notActive"].([]any); len(na) != 1 {
		t.Errorf("notActive = %v", na)
	}
}

func TestMCPResourcesAndPrompts(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{
		Queues: queues("orders"), Export: export.Files,
		Topics: []server.Topic{{Name: "ev", Subscriptions: []server.Subscription{{Name: "audit"}}}},
	})
	c := startMCP(t, srv)

	res, _ := c.rpc("resources/list", nil)
	uris := map[string]bool{}
	for _, r := range res["resources"].([]any) {
		uris[r.(map[string]any)["uri"].(string)] = true
	}
	for _, u := range []string{"osb://namespaces", "osb://emulator/entities", "osb://emulator/info", "osb://export/config.json", "osb://certificate.pem"} {
		if !uris[u] {
			t.Errorf("resources/list lacks %s", u)
		}
	}
	if tpl, _ := c.rpc("resources/templates/list", nil); len(tpl["resourceTemplates"].([]any)) != 3 {
		t.Errorf("resource templates = %v", tpl)
	}
	read := func(uri string) (string, map[string]any) {
		res, rpcErr := c.rpc("resources/read", map[string]any{"uri": uri})
		if rpcErr != nil {
			return "", rpcErr
		}
		return res["contents"].([]any)[0].(map[string]any)["text"].(string), nil
	}
	if text, _ := read("osb://emulator/queues/orders"); !strings.Contains(text, `"path": "orders"`) {
		t.Errorf("queue resource = %s", text)
	}
	if text, _ := read("osb://emulator/topics/ev/subscriptions/audit"); !strings.Contains(text, "$Default") {
		t.Errorf("subscription resource = %s", text)
	}
	if text, _ := read("osb://export/main.tf"); !strings.Contains(text, "azurerm_servicebus_topic") {
		t.Errorf("terraform resource = %s", text)
	}
	if text, _ := read("osb://certificate.pem"); !strings.HasPrefix(text, "-----BEGIN CERTIFICATE-----") {
		t.Errorf("certificate resource = %.40s", text)
	}
	if _, rpcErr := read("osb://emulator/queues/missing"); rpcErr == nil {
		t.Error("reading a missing queue worked")
	}
	if _, rpcErr := read("http://elsewhere/x"); rpcErr == nil {
		t.Error("reading a foreign URI worked")
	}

	list, _ := c.rpc("prompts/list", nil)
	if n := len(list["prompts"].([]any)); n != 3 {
		t.Errorf("prompts = %d, want 3", n)
	}
	p, rpcErr := c.rpc("prompts/get", map[string]any{"name": "explain_routing", "arguments": map[string]any{"topic": "ev", "message": "red order"}})
	if rpcErr != nil {
		t.Fatalf("prompts/get error: %v", rpcErr)
	}
	text := p["messages"].([]any)[0].(map[string]any)["content"].(map[string]any)["text"].(string)
	if !strings.Contains(text, `"ev"`) || !strings.Contains(text, "red order") || !strings.Contains(text, "test_rule") {
		t.Errorf("prompt text = %s", text)
	}
	if _, rpcErr := c.rpc("prompts/get", map[string]any{"name": "explain_routing"}); rpcErr == nil {
		t.Error("prompts/get without its required arguments worked")
	}
	if _, rpcErr := c.rpc("prompts/get", map[string]any{"name": "nope"}); rpcErr == nil {
		t.Error("prompts/get of an unknown prompt worked")
	}
}

func TestMCPGeo(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{
		Queues:     queues("orders"),
		Namespaces: []server.NamespaceOptions{{Name: "dr"}},
	})
	c := startMCP(t, srv)

	c.ok("geo_dr_pair", map[string]any{"alias": "bus", "primary": "emulator", "secondary": "dr"})
	st := items(c.ok("geo_dr_status", nil))
	if len(st) != 1 || st[0]["paired"] != true || st[0]["primary"] != "emulator" {
		t.Errorf("geo_dr_status = %v", st)
	}
	c.fails("geo_dr_pair", map[string]any{"alias": "bus", "primary": "emulator", "secondary": "dr"}, "")
	c.ok("geo_dr_failover", map[string]any{"alias": "bus", "force": true})
	c.ok("geo_dr_delete_alias", map[string]any{"alias": "bus"})

	rep := c.ok("geo_replication_status", nil)
	if rep["enabled"] != false {
		t.Errorf("geo_replication_status = %v", rep)
	}
	c.ok("geo_replication_enable", map[string]any{"synchronous": true, "primary_region": "East US", "secondary_region": "West US"})
	if rep := c.ok("geo_replication_status", nil); rep["enabled"] != true || rep["synchronous"] != true || rep["primaryRegion"] != "East US" {
		t.Errorf("geo_replication_status after enable = %v", rep)
	}
	c.ok("geo_replication_promote", nil)
	c.ok("geo_replication_disable", nil)
	c.fails("geo_replication_enable", map[string]any{"max_lag": "soon"}, "max_lag")
}

func TestMCPLogLevel(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{})
	if startMCP(t, srv).toolNames()["set_log_level"] {
		t.Error("set_log_level is offered without a level to change")
	}
}

// lockedBuffer collects a process's output that the test reads meanwhile.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// TestBinaryMCP checks the command-line switches: the endpoint is off by
// default, has a port of its own, and set_log_level reaches the logger.
func TestBinaryMCP(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	t.Parallel()
	bin := buildBinary(t)

	start := func(t *testing.T, env []string, args ...string) (addr string, logs func() string) {
		t.Helper()
		addr = freeAddr(t)
		cmd := exec.Command(bin, append([]string{"-listen", addr, "-log-level", "info", "-queue", "orders"}, args...)...)
		stderr := &lockedBuffer{}
		cmd.Stderr = stderr
		cmd.Env = append(os.Environ(), env...)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
		waitForPort(t, addr)
		return addr, stderr.String
	}

	t.Run("off by default", func(t *testing.T) {
		addr, logs := start(t, nil)
		// Without -mcp-addr no endpoint starts, and the emulator's own port
		// does not answer MCP.
		resp, err := http.Post("http://"+addr+"/mcp", "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if strings.Contains(string(body), "jsonrpc") {
			t.Errorf("the emulator's port answers MCP: %s", body)
		}
		if strings.Contains(logs(), "mcp listening") {
			t.Errorf("the log announces an MCP endpoint:\n%s", logs())
		}
	})

	t.Run("own port", func(t *testing.T) {
		mcpAddr := freeAddr(t)
		_, logs := start(t, nil, "-mcp-addr", mcpAddr)
		waitForPort(t, mcpAddr)
		c := &mcpClient{t: t, url: "http://" + mcpAddr + "/mcp"}
		names := c.toolNames()
		if names["peek_messages"] {
			t.Error("peek_messages is offered without -mcp-allow-data")
		}
		if !names["set_log_level"] || !names["create_queue"] {
			t.Errorf("tools = %v", names)
		}
		if got := pathsOf(c.ok("list_entities", nil)); len(got) != 1 || got[0] != "orders" {
			t.Errorf("list_entities = %v", got)
		}
		c.ok("set_log_level", map[string]any{"level": "debug"})
		c.fails("set_log_level", map[string]any{"level": "loud"}, "level")
		if !strings.Contains(logs(), "mcp listening") {
			t.Errorf("the log does not announce the endpoint:\n%s", logs())
		}
		// The AMQP port does not speak MCP, and the MCP port does not speak AMQP.
		resp, err := http.Post("http://"+mcpAddr+"/other", "application/json", strings.NewReader("{}"))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("POST /other status = %d, want 404", resp.StatusCode)
			}
		}
	})

	t.Run("flags", func(t *testing.T) {
		mcpAddr := freeAddr(t)
		start(t, nil, "-mcp-addr", mcpAddr, "-mcp-readonly", "-mcp-allow-data")
		waitForPort(t, mcpAddr)
		c := &mcpClient{t: t, url: "http://" + mcpAddr + "/mcp"}
		names := c.toolNames()
		if !names["peek_messages"] || names["create_queue"] || names["send_message"] {
			t.Errorf("-mcp-readonly -mcp-allow-data tools: peek=%v create=%v send=%v", names["peek_messages"], names["create_queue"], names["send_message"])
		}
	})

	t.Run("environment", func(t *testing.T) {
		mcpAddr := freeAddr(t)
		start(t, []string{"OSB_MCP_ADDR=" + mcpAddr, "OSB_MCP_READONLY=true", "OSB_MCP_ALLOW_DATA=1"})
		waitForPort(t, mcpAddr)
		names := (&mcpClient{t: t, url: "http://" + mcpAddr + "/mcp"}).toolNames()
		if !names["peek_messages"] || names["create_queue"] {
			t.Errorf("tools from the environment: peek=%v create=%v, want read-only with data", names["peek_messages"], names["create_queue"])
		}
	})

	t.Run("flag beats environment", func(t *testing.T) {
		unused, mcpAddr := freeAddr(t), freeAddr(t)
		start(t, []string{"OSB_MCP_ADDR=" + unused, "OSB_MCP_READONLY=true"}, "-mcp-addr", mcpAddr, "-mcp-readonly=false")
		waitForPort(t, mcpAddr)
		if c, err := net.DialTimeout("tcp", unused, 200*time.Millisecond); err == nil {
			c.Close()
			t.Errorf("the endpoint also listens on the address from the environment")
		}
		if !(&mcpClient{t: t, url: "http://" + mcpAddr + "/mcp"}).toolNames()["create_queue"] {
			t.Error("-mcp-readonly=false did not beat OSB_MCP_READONLY=true")
		}
	})

	t.Run("bad environment", func(t *testing.T) {
		cmd := exec.Command(bin, "-listen", freeAddr(t))
		cmd.Env = append(os.Environ(), "OSB_MCP_READONLY=maybe")
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), "OSB_MCP_READONLY") {
			t.Errorf("a bad OSB_MCP_READONLY gave %v and %q, want a failure that names it", err, out)
		}
	})

	t.Run("bad address", func(t *testing.T) {
		out, err := exec.Command(bin, "-listen", freeAddr(t), "-mcp-addr", "256.0.0.1:1").CombinedOutput()
		if err == nil || !strings.Contains(string(out), "mcp") {
			t.Errorf("a bad -mcp-addr gave %v and %q, want a failure that names mcp", err, out)
		}
	})
}

// TestMCPResetKeepsDeclaredPolicies checks that resetting a namespace gives
// back the shared access policies scoped to its entities, which deleting the
// entities removed, with the keys they were declared with.
func TestMCPResetKeepsDeclaredPolicies(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{
		Queues: queues("orders"),
		Policies: []auth.Policy{
			{Scope: "orders", Name: "sender", Rights: auth.Send, PrimaryKey: "primary-key", SecondaryKey: "secondary-key"},
		},
	})
	c := startMCP(t, srv)
	find := func() map[string]any {
		for _, r := range items(c.ok("list_authorization_rules", map[string]any{"all": true})) {
			if r["name"] == "sender" {
				return r
			}
		}
		return nil
	}
	if find() == nil {
		t.Fatal("the declared policy is not listed")
	}
	// A policy made later for the queue goes with it, as with any entity.
	c.ok("create_sas_policy", map[string]any{"name": "adhoc", "scope": "orders", "rights": []any{"Listen"}})
	c.ok("reset_namespace", map[string]any{"confirm": true})
	got := find()
	if got == nil || got["primaryKey"] != "primary-key" || got["secondaryKey"] != "secondary-key" || got["scope"] != "orders" {
		t.Errorf("policy after reset = %v, want the declared one with its keys", got)
	}
	for _, r := range items(c.ok("list_authorization_rules", map[string]any{"all": true})) {
		if r["name"] == "adhoc" {
			t.Errorf("the ad hoc policy of a deleted queue survived the reset: %v", r)
		}
	}
}

func TestMCPSchemaRequiresConfirmOnlyWhereItMust(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{})
	c := startMCP(t, srv)
	res, _ := c.rpc("tools/list", nil)
	required := map[string][]any{}
	for _, tl := range res["tools"].([]any) {
		m := tl.(map[string]any)
		r, _ := m["inputSchema"].(map[string]any)["required"].([]any)
		required[m["name"].(string)] = r
	}
	has := func(tool, field string) bool {
		for _, f := range required[tool] {
			if f == field {
				return true
			}
		}
		return false
	}
	for _, tool := range []string{"delete_entity", "purge_entity", "reset_namespace", "requeue_dead_letters"} {
		if !has(tool, "confirm") {
			t.Errorf("%s does not require confirm", tool)
		}
	}
	if has("apply_config", "confirm") {
		t.Error("apply_config requires confirm even without prune")
	}
	c.fails("apply_config", map[string]any{"format": "config_json", "content": `{"UserConfig":{"Namespaces":[{"Name":"emulator","Queues":[]}]}}`, "prune": true}, "confirm")
	// Empty lists are lists, not null.
	if got := c.ok("list_entities", nil)["items"]; got == nil {
		t.Error("list_entities items is null for no entities")
	}
	if got := c.ok("get_overview", nil)["notActive"]; got == nil {
		t.Error("get_overview notActive is null for no entities")
	}
}
