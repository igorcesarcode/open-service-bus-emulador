package integration_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/coder/websocket"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// newWebSocketClient connects the SDK over WebSocket to the host and port
// of connStr. The SDK hands NewWebSocketConn a wss:// URL; the emulator's
// certificate is self-signed.
func newWebSocketClient(t *testing.T, connStr string) *azservicebus.Client {
	t.Helper()
	return newWebSocketClientWith(t, connStr, "AMQPWSB10")
}

// newWebSocketClientWith is newWebSocketClient offering the given
// subprotocol: "amqp" is what the final binding and Azure.Amqp for .NET use,
// "AMQPWSB10" what the drafts and the JavaScript SDK use.
func newWebSocketClientWith(t *testing.T, connStr, subprotocol string) *azservicebus.Client {
	t.Helper()
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	var urls []string
	client, err := azservicebus.NewClientFromConnectionString(connStr, &azservicebus.ClientOptions{
		RetryOptions: azservicebus.RetryOptions{MaxRetries: 1, RetryDelay: 10 * time.Millisecond},
		NewWebSocketConn: func(ctx context.Context, args azservicebus.NewWebSocketConnArgs) (net.Conn, error) {
			urls = append(urls, args.Host)
			ws, _, err := websocket.Dial(ctx, args.Host, &websocket.DialOptions{
				Subprotocols: []string{subprotocol},
				HTTPClient:   httpClient,
			})
			if err != nil {
				return nil, err
			}
			return websocket.NetConn(context.Background(), ws, websocket.MessageBinary), nil
		},
	})
	if err != nil {
		t.Fatalf("NewClientFromConnectionString() error: %v", err)
	}
	t.Cleanup(func() {
		client.Close(context.Background())
		for _, u := range urls {
			if !strings.HasPrefix(u, "wss://") || !strings.HasSuffix(u, "/$servicebus/websocket") {
				t.Errorf("NewWebSocketConn got %q, want a wss://.../$servicebus/websocket URL", u)
			}
		}
	})
	return client
}

func TestWebSocket(t *testing.T) {
	t.Parallel()
	for _, subprotocol := range []string{"AMQPWSB10", "amqp"} {
		t.Run(subprotocol, func(t *testing.T) {
			t.Parallel()
			testWebSocket(t, subprotocol)
		})
	}
}

func testWebSocket(t *testing.T, subprotocol string) {
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newWebSocketClientWith(t, srv.ConnectionString(), subprotocol)

	send(t, newSender(t, client, "orders"), bodies("over", "websocket")...)
	r := newReceiver(t, client, "orders", nil)
	msgs := receive(t, r, 2)
	for _, m := range msgs {
		if err := r.CompleteMessage(testContext(t), m, nil); err != nil {
			t.Errorf("CompleteMessage() error: %v", err)
		}
	}
	if diff := cmp.Diff([]string{"over", "websocket"}, bodiesOf(msgs)); diff != "" {
		t.Errorf("received bodies mismatch (-want +got):\n%s", diff)
	}
}

// TestSeparatePortsSDK serves AMQP and HTTP on their own ports: the AMQP
// client uses the AMQP port, the admin and WebSocket clients the HTTP one.
func TestSeparatePortsSDK(t *testing.T) {
	t.Parallel()
	split := startSplitServer(t)

	ac := newAdminClientFor(t, split.HTTPConnectionString())
	if _, err := ac.CreateQueue(testContext(t), "split", nil); err != nil {
		t.Fatalf("CreateQueue() on the HTTP port error: %v", err)
	}
	amqpClient := newClientFor(t, split.ConnectionString())
	send(t, newSender(t, amqpClient, "split"), bodies("amqp")...)
	wsClient := newWebSocketClient(t, split.HTTPConnectionString())
	send(t, newSender(t, wsClient, "split"), bodies("ws")...)

	if diff := cmp.Diff([]string{"amqp", "ws"}, sortedBodies(receive(t, newReceiver(t, amqpClient, "split", nil), 2))); diff != "" {
		t.Errorf("received bodies mismatch (-want +got):\n%s", diff)
	}
}

// startSplitServer runs an emulator with separate AMQP and HTTP ports and
// no shared port.
func startSplitServer(t *testing.T) *server.Server {
	t.Helper()
	srv, err := server.New(context.Background(), server.Options{AMQPAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("server.New() error: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}
