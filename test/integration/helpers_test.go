// Package integration_test exercises the emulator through the official
// Azure Service Bus SDK for Go.
package integration_test

import (
	"context"
	"crypto/tls"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// startServer runs an emulator on a free loopback port for one test.
func startServer(t *testing.T, opts server.Options) *server.Server {
	t.Helper()
	opts.Addr = "127.0.0.1:0"
	srv, err := server.New(context.Background(), opts)
	if err != nil {
		t.Fatalf("server.New() error: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close() error: %v", err)
		}
	})
	return srv
}

// queues declares queues with default properties.
func queues(names ...string) []server.Queue {
	out := make([]server.Queue, len(names))
	for i, n := range names {
		out[i] = server.Queue{Name: n}
	}
	return out
}

// queue declares a queue whose defaults are adjusted by edit.
func queue(name string, edit func(p *broker.Properties)) server.Queue {
	p := broker.DefaultProperties(broker.KindQueue)
	edit(&p)
	return server.Queue{Name: name, Properties: p}
}

func newClient(t *testing.T, srv *server.Server) *azservicebus.Client {
	t.Helper()
	return newClientFor(t, srv.ConnectionString())
}

// noEmulatorFlag runs the suites with connection strings that do not carry
// UseDevelopmentEmulator, the way a client written for the real service
// connects: AMQP over TLS, to the namespace's name. Set OSB_NO_EMULATOR_FLAG=1.
func noEmulatorFlag() bool { return os.Getenv("OSB_NO_EMULATOR_FLAG") != "" }

// realHost is the name a production namespace has.
const realHost = "osb-test.servicebus.windows.net"

// withoutEmulatorFlag rewrites connStr as the portal of the real service
// hands it out: no UseDevelopmentEmulator, no port, the namespace's
// servicebus.windows.net name. It returns the address the Endpoint named,
// where the emulator listens, for the clients to dial instead of the name.
func withoutEmulatorFlag(connStr string) (string, string) {
	var parts []string
	addr := ""
	for _, p := range strings.Split(connStr, ";") {
		switch {
		case strings.HasPrefix(p, "UseDevelopmentEmulator="):
			continue
		case strings.HasPrefix(p, "Endpoint=sb://"):
			addr = strings.TrimSuffix(strings.TrimPrefix(p, "Endpoint=sb://"), "/")
			p = "Endpoint=sb://" + realHost + "/"
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, ";"), addr
}

func newClientFor(t *testing.T, connStr string) *azservicebus.Client {
	t.Helper()
	opts := &azservicebus.ClientOptions{
		RetryOptions: azservicebus.RetryOptions{MaxRetries: 1, RetryDelay: 10 * time.Millisecond},
	}
	if noEmulatorFlag() {
		var addr string
		connStr, addr = withoutEmulatorFlag(connStr)
		opts.CustomEndpoint = addr
		opts.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	client, err := azservicebus.NewClientFromConnectionString(connStr, opts)
	if err != nil {
		t.Fatalf("NewClientFromConnectionString() error: %v", err)
	}
	t.Cleanup(func() { client.Close(context.Background()) })
	return client
}

func newSender(t *testing.T, client *azservicebus.Client, entity string) *azservicebus.Sender {
	t.Helper()
	sender, err := client.NewSender(entity, nil)
	if err != nil {
		t.Fatalf("NewSender(%q) error: %v", entity, err)
	}
	t.Cleanup(func() { sender.Close(context.Background()) })
	return sender
}

func newReceiver(t *testing.T, client *azservicebus.Client, queue string, opts *azservicebus.ReceiverOptions) *azservicebus.Receiver {
	t.Helper()
	r, err := client.NewReceiverForQueue(queue, opts)
	if err != nil {
		t.Fatalf("NewReceiverForQueue(%q) error: %v", queue, err)
	}
	t.Cleanup(func() { r.Close(context.Background()) })
	return r
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func send(t *testing.T, sender *azservicebus.Sender, msgs ...*azservicebus.Message) {
	t.Helper()
	for i, m := range msgs {
		if err := sender.SendMessage(testContext(t), m, nil); err != nil {
			t.Fatalf("SendMessage(%d) error: %v", i, err)
		}
	}
}

func bodies(bs ...string) []*azservicebus.Message {
	out := make([]*azservicebus.Message, len(bs))
	for i, b := range bs {
		out[i] = &azservicebus.Message{Body: []byte(b)}
	}
	return out
}

// receive collects exactly n messages, failing the test if they do not
// arrive within the test context.
func receive(t *testing.T, r *azservicebus.Receiver, n int) []*azservicebus.ReceivedMessage {
	t.Helper()
	ctx := testContext(t)
	var out []*azservicebus.ReceivedMessage
	for len(out) < n {
		msgs, err := r.ReceiveMessages(ctx, n-len(out), nil)
		if err != nil {
			t.Fatalf("ReceiveMessages() after %d/%d messages error: %v", len(out), n, err)
		}
		out = append(out, msgs...)
	}
	return out
}

// receiveNone checks that no message arrives within wait.
func receiveNone(t *testing.T, r *azservicebus.Receiver, wait time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	msgs, err := r.ReceiveMessages(ctx, 1, nil)
	if len(msgs) > 0 {
		t.Fatalf("ReceiveMessages() = %d messages (first body %q), want none", len(msgs), msgs[0].Body)
	}
	if err != nil && ctx.Err() == nil {
		t.Fatalf("ReceiveMessages() error: %v", err)
	}
}

func bodiesOf(msgs []*azservicebus.ReceivedMessage) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.Body)
	}
	return out
}
