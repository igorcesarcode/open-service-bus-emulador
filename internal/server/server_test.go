package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	goamqp "github.com/Azure/go-amqp"
	"github.com/coder/websocket"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
)

func TestDialAddr(t *testing.T) {
	tests := []struct {
		listen string
		want   string
	}{
		{"[::]:5672", "localhost:5672"},
		{"0.0.0.0:5672", "localhost:5672"},
		{":5672", "localhost:5672"},
		{"127.0.0.1:5672", "127.0.0.1:5672"},
		{"[::1]:5672", "[::1]:5672"},
		{"emulator.local:5672", "emulator.local:5672"},
	}
	for _, tc := range tests {
		addr := &fakeAddr{tc.listen}
		if got := dialAddr(addr); got != tc.want {
			t.Errorf("dialAddr(%q) = %q, want %q", tc.listen, got, tc.want)
		}
	}
}

type fakeAddr struct{ s string }

func (a *fakeAddr) Network() string { return "tcp" }
func (a *fakeAddr) String() string  { return a.s }

func TestConnectionStringWildcardListen(t *testing.T) {
	srv, err := New(context.Background(), Options{Addr: ":0"})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	defer srv.Close()
	if cs := srv.ConnectionString(); !strings.Contains(cs, "Endpoint=sb://localhost:") {
		t.Errorf("ConnectionString() = %q, want an sb://localhost endpoint", cs)
	}
}

// TestCloseWithIdleConnection checks that a client that connects and sends
// nothing does not delay shutdown.
func TestCloseWithIdleConnection(t *testing.T) {
	srv, err := New(context.Background(), Options{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	c, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}
	defer c.Close()
	time.Sleep(50 * time.Millisecond) // let the server accept the connection

	start := time.Now()
	if err := srv.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("Close() took %v with an idle connection, want under 1s", d)
	}
}

var insecure = &tls.Config{InsecureSkipVerify: true}

func startTestServer(t *testing.T, opts Options) *Server {
	t.Helper()
	opts.Queues = []Queue{{Name: "q"}}
	srv, err := New(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}

func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// sendAMQP sends a message to queue q over conn, or over a new connection
// to url when conn is nil.
func sendAMQP(ctx context.Context, url string, nc net.Conn) error {
	opts := &goamqp.ConnOptions{SASLType: goamqp.SASLTypeAnonymous(), TLSConfig: insecure}
	var conn *goamqp.Conn
	var err error
	if nc != nil {
		conn, err = goamqp.NewConn(ctx, nc, opts)
	} else {
		conn, err = goamqp.Dial(ctx, url, opts)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		return err
	}
	sender, err := sess.NewSender(ctx, "q", nil)
	if err != nil {
		return err
	}
	return sender.Send(ctx, goamqp.NewMessage([]byte("x")), nil)
}

// getQueue reads queue q through the management API at url.
func getQueue(ctx context.Context, url string) error {
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: insecure}}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url+"/q?api-version=2024-05", nil)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "<QueueDescription") {
		return fmt.Errorf("GET = %d %s, want the queue entry", resp.StatusCode, body)
	}
	return nil
}

// dialWebSocket opens AMQP over WebSocket at url.
func dialWebSocket(ctx context.Context, url string, protocols ...string) (net.Conn, error) {
	ws, _, err := websocket.Dial(ctx, url+"/$servicebus/websocket", &websocket.DialOptions{
		Subprotocols: protocols,
		HTTPClient:   &http.Client{Transport: &http.Transport{TLSClientConfig: insecure}},
	})
	if err != nil {
		return nil, err
	}
	return websocket.NetConn(context.Background(), ws, websocket.MessageBinary), nil
}

func sendWebSocket(ctx context.Context, url string) error {
	return sendWebSocketWith(ctx, url, "AMQPWSB10")
}

func sendWebSocketWith(ctx context.Context, url, subprotocol string) error {
	nc, err := dialWebSocket(ctx, url, subprotocol)
	if err != nil {
		return err
	}
	return sendAMQP(ctx, "", nc)
}

// TestProtocolsShareThePort connects with AMQP, AMQP over TLS, HTTP,
// HTTPS and AMQP over WebSocket, plain and over TLS, to the same port.
func TestProtocolsShareThePort(t *testing.T) {
	srv := startTestServer(t, Options{Addr: "127.0.0.1:0"})
	addr := srv.Addr().String()
	ctx := testContext(t)
	for name, err := range map[string]error{
		"amqp":  sendAMQP(ctx, "amqp://"+addr, nil),
		"amqps": sendAMQP(ctx, "amqps://"+addr, nil),
		"http":  getQueue(ctx, "http://"+addr),
		"https": getQueue(ctx, "https://"+addr),
		"ws":    sendWebSocket(ctx, "ws://"+addr),
		"wss":   sendWebSocket(ctx, "wss://"+addr),
	} {
		if err != nil {
			t.Errorf("%s error: %v", name, err)
		}
	}
}

// TestWebSocketSubprotocols checks that both the final subprotocol (amqp)
// and the one of the drafts (AMQPWSB10) carry AMQP.
func TestWebSocketSubprotocols(t *testing.T) {
	srv := startTestServer(t, Options{Addr: "127.0.0.1:0"})
	ctx := testContext(t)
	for _, sp := range []string{"amqp", "AMQPWSB10"} {
		if err := sendWebSocketWith(ctx, "ws://"+srv.Addr().String(), sp); err != nil {
			t.Errorf("subprotocol %q error: %v", sp, err)
		}
	}
}

func TestWebSocketNeedsSubprotocol(t *testing.T) {
	srv := startTestServer(t, Options{Addr: "127.0.0.1:0"})
	ctx := testContext(t)
	nc, err := dialWebSocket(ctx, "ws://"+srv.Addr().String())
	if err != nil {
		t.Fatalf("dialWebSocket() error: %v", err)
	}
	defer nc.Close()
	nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := nc.Read(make([]byte, 8)); err == nil {
		t.Error("Read() succeeded, want the connection closed for lacking a subprotocol")
	}
}

// TestSeparatePorts checks that AMQPAddr serves only AMQP and HTTPAddr
// only HTTP, WebSocket included.
func TestSeparatePorts(t *testing.T) {
	srv := startTestServer(t, Options{AMQPAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"})
	amqpAddr, httpAddr := srv.AMQPAddr().String(), srv.HTTPAddr().String()
	if amqpAddr == httpAddr || srv.Addr().String() != amqpAddr {
		t.Fatalf("Addr() = %v, AMQPAddr() = %s, HTTPAddr() = %s", srv.Addr(), amqpAddr, httpAddr)
	}
	if !strings.Contains(srv.HTTPConnectionString(), httpAddr[strings.LastIndex(httpAddr, ":"):]) {
		t.Errorf("HTTPConnectionString() = %q, want the HTTP port", srv.HTTPConnectionString())
	}
	ctx := testContext(t)
	for name, err := range map[string]error{
		"amqp":  sendAMQP(ctx, "amqp://"+amqpAddr, nil),
		"amqps": sendAMQP(ctx, "amqps://"+amqpAddr, nil),
		"http":  getQueue(ctx, "http://"+httpAddr),
		"https": getQueue(ctx, "https://"+httpAddr),
		"wss":   sendWebSocket(ctx, "wss://"+httpAddr),
	} {
		if err != nil {
			t.Errorf("%s error: %v", name, err)
		}
	}
	for name, err := range map[string]error{
		"http on the AMQP port":  getQueue(ctx, "http://"+amqpAddr),
		"amqp on the HTTP port":  sendAMQP(ctx, "amqp://"+httpAddr, nil),
		"amqps on the HTTP port": sendAMQP(ctx, "amqps://"+httpAddr, nil),
	} {
		if err == nil {
			t.Errorf("%s succeeded, want it refused", name)
		}
	}
}

func TestStartNeedsAnAddress(t *testing.T) {
	srv, err := New(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if err := srv.Start(); err == nil {
		t.Error("Start() without addresses succeeded, want an error")
	}
}

func TestSelfSignedCertificateHosts(t *testing.T) {
	cert, err := selfSignedCertificate([]string{"servicebus", "10.1.2.3"})
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"localhost", "servicebus", "127.0.0.1", "10.1.2.3"} {
		if err := cert.Leaf.VerifyHostname(host); err != nil {
			t.Errorf("VerifyHostname(%q) error: %v", host, err)
		}
	}
	if err := cert.Leaf.VerifyHostname("other"); err == nil {
		t.Error("VerifyHostname(other) succeeded, want error")
	}
}

// TestRedeclaringLeavesNoTrace checks that restarting on the same database
// with the same declarations adds nothing to the activity log, as a no-op
// terraform apply adds nothing to Azure's.
func TestRedeclaringLeavesNoTrace(t *testing.T) {
	ctx := context.Background()
	opts := Options{
		DBPath: t.TempDir() + "/osb.db",
		Queues: []Queue{{Name: "orders"}},
		Topics: []Topic{{Name: "events", Subscriptions: []Subscription{{
			Name:  "audit",
			Rules: []broker.Rule{broker.TrueRule("all"), {Name: "none", Filter: broker.RuleFilter{Kind: broker.FilterFalse}}},
		}}}},
	}
	activity := func() int {
		s, err := New(ctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		entries, err := s.Broker().Activity(ctx, broker.ActivityQuery{Limit: 1000})
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	first := activity()
	if first == 0 {
		t.Fatal("the first start logged nothing")
	}
	if second := activity(); second != first {
		t.Errorf("activity entries = %d after a restart, want %d", second, first)
	}
}
