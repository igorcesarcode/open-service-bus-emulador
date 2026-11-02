package integration_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// namespaceHost connects to the emulator's port but presents host as the
// namespace's name, the way a client with a DNS name for it would. Dialing
// 127.0.0.1 whatever the name keeps the tests independent of the machine's
// resolver.
func newClientAt(t *testing.T, srv *server.Server, host string) *azservicebus.Client {
	t.Helper()
	client, err := azservicebus.NewClientFromConnectionString(
		"Endpoint=sb://"+host+";SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE",
		&azservicebus.ClientOptions{
			CustomEndpoint: srv.Addr().String(),
			TLSConfig:      &tls.Config{InsecureSkipVerify: true},
			RetryOptions:   azservicebus.RetryOptions{MaxRetries: -1},
		})
	if err != nil {
		t.Fatalf("NewClientFromConnectionString(%s) error: %v", host, err)
	}
	t.Cleanup(func() { client.Close(context.Background()) })
	return client
}

func newAdminAt(t *testing.T, srv *server.Server, host string) *admin.Client {
	t.Helper()
	port := srv.Addr().String()
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, port)
		},
	}
	_, p, _ := net.SplitHostPort(port)
	client, err := admin.NewClientFromConnectionString(
		"Endpoint=sb://"+host+":"+p+";SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE",
		&admin.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: &http.Client{Transport: transport}}})
	if err != nil {
		t.Fatalf("admin client for %s: %v", host, err)
	}
	return client
}

func twoNamespaces(t *testing.T, dbPath string) *server.Server {
	t.Helper()
	return startServer(t, server.Options{
		DBPath: dbPath,
		Queues: queues("shared", "only-default"),
		Namespaces: []server.NamespaceOptions{
			{Name: "orders", Queues: queues("shared", "orders-q")},
			{Name: "billing", Topics: []server.Topic{{Name: "invoices", Subscriptions: []server.Subscription{{Name: "audit"}}}}},
		},
	})
}

func TestNamespacesAreSeparatedByHostname(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srv := twoNamespaces(t, filepath.Join(dir, "osb.db"))
	def := newClientAt(t, srv, "localhost")
	orders := newClientAt(t, srv, "orders.servicebus.windows.net")
	billing := newClientAt(t, srv, "billing.localhost")
	ctx := testContext(t)

	// The same queue name in two namespaces holds different messages.
	send(t, newSender(t, def, "shared"), bodies("in-default")...)
	send(t, newSender(t, orders, "shared"), bodies("in-orders")...)
	radOpts := &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete}
	if diff := cmp.Diff([]string{"in-orders"}, bodiesOf(receive(t, newReceiver(t, orders, "shared", radOpts), 1))); diff != "" {
		t.Errorf("orders mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"in-default"}, bodiesOf(receive(t, newReceiver(t, def, "shared", radOpts), 1))); diff != "" {
		t.Errorf("default mismatch (-want +got):\n%s", diff)
	}

	// A queue exists only where it was declared.
	send(t, newSender(t, orders, "orders-q"), bodies("x")...)
	sender, err := def.NewSender("orders-q", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close(ctx)
	if err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("lost")}, nil); err == nil {
		t.Error("the default namespace accepted a message for a queue of another namespace")
	}
	// A topic of another namespace.
	send(t, newSender(t, billing, "invoices"), bodies("inv-1")...)
	if diff := cmp.Diff([]string{"inv-1"}, bodiesOf(receive(t, newSubReceiver(t, billing, "invoices", "audit", radOpts), 1))); diff != "" {
		t.Errorf("billing mismatch (-want +got):\n%s", diff)
	}

	// Hosts that name no namespace fall to the default one: an IP, a name
	// with no dot and a name nobody declared.
	for _, host := range []string{"127.0.0.1", "plainname", "unknown.servicebus.windows.net"} {
		c := newClientAt(t, srv, host)
		send(t, newSender(t, c, "only-default"), bodies("via "+host)...)
	}
	if n := peekCount(t, def, "only-default", nil); n != 3 {
		t.Errorf("only-default holds %d messages, want 3", n)
	}

	// The administration API answers for the namespace of its Host.
	list := func(host string) []string {
		var names []string
		pager := newAdminAt(t, srv, host).NewListQueuesPager(nil)
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				t.Fatalf("listing queues of %s: %v", host, err)
			}
			for _, q := range page.Queues {
				names = append(names, q.QueueName)
			}
		}
		return names
	}
	if diff := cmp.Diff([]string{"orders-q", "shared"}, sorted(list("orders.localhost"))); diff != "" {
		t.Errorf("orders queues mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"only-default", "shared"}, sorted(list("localhost"))); diff != "" {
		t.Errorf("default queues mismatch (-want +got):\n%s", diff)
	}
	if _, err := newAdminAt(t, srv, "orders.localhost").CreateQueue(ctx, "made-in-orders", nil); err != nil {
		t.Fatal(err)
	}
	if n := len(list("localhost")); n != 2 {
		t.Errorf("a queue made in orders appeared in the default namespace (%d queues)", n)
	}

	// Each namespace has its own database file next to the default one.
	for _, name := range []string{"osb.db", "osb-orders.db", "osb-billing.db"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("database %s: %v", name, err)
		}
	}
	// Metrics and the activity log are per namespace.
	_, orders1 := httpGetHost(t, srv, "orders.localhost", "/$api/activity?operation=CreateQueue")
	if !strings.Contains(orders1, "made-in-orders") {
		t.Errorf("orders activity lacks the new queue: %s", orders1)
	}
	_, def1 := httpGetHost(t, srv, "localhost", "/$api/activity?operation=CreateQueue")
	if strings.Contains(def1, "made-in-orders") {
		t.Errorf("default activity has a queue of orders: %s", def1)
	}
}

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// httpGetHost fetches path from the emulator with the given Host header.
func httpGetHost(t *testing.T, srv *server.Server, host, path string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", "http://"+srv.HTTPAddr().String()+path, nil)
	req.Host = host
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return resp.StatusCode, b.String()
}

func TestNamespaceNameValidation(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"", "a.b", "has space", "x:y"} {
		_, err := server.New(context.Background(), server.Options{Addr: "127.0.0.1:0", Namespaces: []server.NamespaceOptions{{Name: name}}})
		if err == nil {
			t.Errorf("server.New() with namespace %q succeeded", name)
		}
	}
	_, err := server.New(context.Background(), server.Options{Addr: "127.0.0.1:0", Namespaces: []server.NamespaceOptions{{Name: "emulator"}}})
	if err == nil {
		t.Error("a namespace with the default's name was accepted")
	}
}
