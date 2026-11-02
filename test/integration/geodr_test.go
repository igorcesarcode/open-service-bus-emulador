package integration_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func geoDRServer(t *testing.T) *server.Server {
	t.Helper()
	return startServer(t, server.Options{
		Namespace: "primary",
		Queues:    queues("orders"),
		Topics: []server.Topic{{Name: "events", Subscriptions: []server.Subscription{
			subscription("audit", []broker.Rule{sqlRule("big", "quantity > 10", "SET tier = 'big'")}, func(p *broker.Properties) { p.MaxDeliveryCount = 3 }),
		}}},
		Namespaces: []server.NamespaceOptions{{Name: "secondary"}, {Name: "spare"}},
		GeoDR:      []server.GeoDRConfig{{Alias: "bus", Primary: "primary", Secondary: "secondary"}},
	})
}

func queueNames(t *testing.T, ac *admin.Client) []string {
	t.Helper()
	var names []string
	pager := ac.NewListQueuesPager(nil)
	for pager.More() {
		page, err := pager.NextPage(testContext(t))
		if err != nil {
			t.Fatalf("listing queues: %v", err)
		}
		for _, q := range page.Queues {
			names = append(names, q.QueueName)
		}
	}
	return sorted(names)
}

func TestGeoDRCopiesMetadataNotMessages(t *testing.T) {
	t.Parallel()
	srv := geoDRServer(t)
	ctx := testContext(t)
	primary := newAdminAt(t, srv, "primary.localhost")
	secondary := newAdminAt(t, srv, "secondary.localhost")

	// The pairing copied what the primary had, rules included.
	if diff := cmp.Diff([]string{"orders"}, queueNames(t, secondary)); diff != "" {
		t.Errorf("secondary queues mismatch (-want +got):\n%s", diff)
	}
	sub, err := secondary.GetSubscription(ctx, "events", "audit", nil)
	if err != nil || sub == nil || *sub.MaxDeliveryCount != 3 {
		t.Fatalf("GetSubscription() on the secondary = %+v, %v", sub, err)
	}
	rule, err := secondary.GetRule(ctx, "events", "audit", "big", nil)
	if err != nil || rule == nil {
		t.Fatalf("GetRule() on the secondary = %+v, %v", rule, err)
	}

	// A change on the primary reaches the secondary.
	if _, err := primary.CreateQueue(ctx, "added-later", nil); err != nil {
		t.Fatal(err)
	}
	if err := srv.SyncGeoDR(ctx, "bus"); err != nil {
		t.Fatalf("SyncGeoDR() error: %v", err)
	}
	if diff := cmp.Diff([]string{"added-later", "orders"}, queueNames(t, secondary)); diff != "" {
		t.Errorf("secondary queues after a change mismatch (-want +got):\n%s", diff)
	}
	if _, err := primary.DeleteQueue(ctx, "added-later", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the delete to reach the secondary", func() bool {
		return cmp.Equal(queueNames(t, secondary), []string{"orders"})
	})

	// Messages are not copied.
	send(t, newSender(t, newClientAt(t, srv, "primary.localhost"), "orders"), bodies("m1", "m2")...)
	srv.SyncGeoDR(ctx, "bus")
	if n := peekCount(t, newClientAt(t, srv, "secondary.localhost"), "orders", nil); n != 0 {
		t.Errorf("messages on the secondary = %d, want 0", n)
	}

	// The secondary refuses changes of its entities, and the status says so.
	if _, err := secondary.CreateQueue(ctx, "nope", nil); responseStatus(err) != http.StatusForbidden {
		t.Errorf("CreateQueue() on the secondary = %v, want 403", err)
	}
	if _, err := secondary.DeleteQueue(ctx, "orders", nil); responseStatus(err) != http.StatusForbidden {
		t.Errorf("DeleteQueue() on the secondary = %v, want 403", err)
	}
	st := srv.GeoDRAliases()
	if len(st) != 1 || st[0].Alias != "bus" || st[0].Primary != "primary" || st[0].Secondary != "secondary" || !st[0].Paired {
		t.Errorf("GeoDRAliases() = %+v", st)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestGeoDRAliasAndFailover(t *testing.T) {
	t.Parallel()
	srv := geoDRServer(t)
	ctx := testContext(t)
	alias := newClientAt(t, srv, "bus.servicebus.windows.net")

	// The alias serves the primary.
	send(t, newSender(t, alias, "orders"), bodies("before")...)
	if n := peekCount(t, newClientAt(t, srv, "primary.localhost"), "orders", nil); n != 1 {
		t.Fatalf("the primary holds %d messages sent through the alias, want 1", n)
	}
	if n := peekCount(t, newClientAt(t, srv, "secondary.localhost"), "orders", nil); n != 0 {
		t.Fatalf("the secondary holds %d messages, want 0", n)
	}

	// Failover: the alias moves to the secondary, with the same entities and
	// none of the messages, and the pairing is broken.
	if err := srv.FailoverGeoDR(ctx, "bus", false); err != nil {
		t.Fatalf("FailoverGeoDR() error: %v", err)
	}
	st := srv.GeoDRAliases()
	if len(st) != 1 || st[0].Primary != "secondary" || st[0].Paired || st[0].Secondary != "" {
		t.Errorf("after the failover GeoDRAliases() = %+v", st)
	}
	alias2 := newClientAt(t, srv, "bus.servicebus.windows.net")
	send(t, newSender(t, alias2, "orders"), bodies("after")...)
	r := newReceiver(t, newClientAt(t, srv, "secondary.localhost"), "orders", &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	if diff := cmp.Diff([]string{"after"}, bodiesOf(receive(t, r, 1))); diff != "" {
		t.Errorf("the secondary mismatch (-want +got):\n%s", diff)
	}
	if n := peekCount(t, newClientAt(t, srv, "primary.localhost"), "orders", nil); n != 1 {
		t.Errorf("the old primary lost or gained messages: %d, want its 1", n)
	}
	// The secondary is writable now, and no longer follows the old primary.
	secondary := newAdminAt(t, srv, "secondary.localhost")
	if _, err := secondary.CreateQueue(ctx, "only-on-new-primary", nil); err != nil {
		t.Errorf("CreateQueue() on the promoted namespace: %v", err)
	}
	if _, err := newAdminAt(t, srv, "primary.localhost").CreateQueue(ctx, "only-on-old-primary", nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if diff := cmp.Diff([]string{"only-on-new-primary", "orders"}, queueNames(t, secondary)); diff != "" {
		t.Errorf("the promoted namespace mismatch (-want +got):\n%s", diff)
	}
	// A failover cannot be repeated, nor can the broken pairing be broken.
	if err := srv.FailoverGeoDR(ctx, "bus", false); !errors.Is(err, server.ErrGeoDR) {
		t.Errorf("a second failover = %v, want a Geo-DR error", err)
	}
	if err := srv.BreakGeoDR(ctx, "bus"); !errors.Is(err, server.ErrGeoDR) {
		t.Errorf("BreakGeoDR() of an unpaired alias = %v", err)
	}

	// Moving forward: pair the alias again with a new secondary. Only its
	// current primary can be the primary.
	if err := srv.PairGeoDR(ctx, "bus", "primary", "spare"); !errors.Is(err, server.ErrGeoDR) {
		t.Errorf("pairing with the old primary = %v, want a Geo-DR error", err)
	}
	if err := srv.PairGeoDR(ctx, "bus", "secondary", "spare"); err != nil {
		t.Fatalf("PairGeoDR() with a new secondary error: %v", err)
	}
	if diff := cmp.Diff([]string{"only-on-new-primary", "orders"}, queueNames(t, newAdminAt(t, srv, "spare.localhost"))); diff != "" {
		t.Errorf("the new secondary mismatch (-want +got):\n%s", diff)
	}
}

func TestGeoDRBreakDeleteAndValidation(t *testing.T) {
	t.Parallel()
	srv := geoDRServer(t)
	ctx := testContext(t)

	// A namespace is in one pairing at a time, and the alias names matter.
	for name, args := range map[string][3]string{
		"already paired":       {"other", "primary", "spare"},
		"secondary paired":     {"other", "spare", "secondary"},
		"itself":               {"x", "spare", "spare"},
		"unknown namespace":    {"x", "spare", "ghost"},
		"alias with a dot":     {"a.b", "spare", "primary"},
		"alias of a namespace": {"primary", "spare", "secondary"},
	} {
		if err := srv.PairGeoDR(ctx, args[0], args[1], args[2]); !errors.Is(err, server.ErrGeoDR) {
			t.Errorf("%s: PairGeoDR(%v) = %v, want a Geo-DR error", name, args, err)
		}
	}

	// Breaking leaves the alias on the primary and frees the secondary.
	if err := srv.BreakGeoDR(ctx, "bus"); err != nil {
		t.Fatal(err)
	}
	if _, err := newAdminAt(t, srv, "secondary.localhost").CreateQueue(ctx, "free-now", nil); err != nil {
		t.Errorf("the secondary stayed read-only after the pairing broke: %v", err)
	}
	if st := srv.GeoDRAliases(); len(st) != 1 || st[0].Primary != "primary" || st[0].Paired {
		t.Errorf("after breaking GeoDRAliases() = %+v", st)
	}
	// An alias that reuses its primary's name is allowed.
	if err := srv.PairGeoDR(ctx, "primary", "primary", "spare"); err != nil {
		t.Errorf("an alias named like its primary: %v", err)
	}
	// Deleting breaks the pairing and the hostname falls to the default.
	if err := srv.DeleteGeoDRAlias(ctx, "bus"); err != nil {
		t.Fatal(err)
	}
	if err := srv.DeleteGeoDRAlias(ctx, "bus"); !errors.Is(err, server.ErrGeoDR) {
		t.Errorf("deleting twice = %v", err)
	}
	if n := len(srv.GeoDRAliases()); n != 1 {
		t.Errorf("aliases left = %d, want 1 (the one named primary)", n)
	}
}

func TestGeoDRSurvivesARestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := func() server.Options {
		return server.Options{
			Addr: "127.0.0.1:0", DBPath: dir + "/osb.db", Namespace: "primary", Queues: queues("orders"),
			Namespaces: []server.NamespaceOptions{{Name: "secondary"}, {Name: "spare"}},
			GeoDR:      []server.GeoDRConfig{{Alias: "bus", Primary: "primary", Secondary: "secondary"}},
		}
	}
	ctx := context.Background()
	srv, err := server.New(ctx, opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.FailoverGeoDR(ctx, "bus", false); err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	// The configuration still lists the pairing, but the failover stands.
	srv, err = server.New(ctx, opts())
	if err != nil {
		t.Fatalf("restart error: %v", err)
	}
	defer srv.Close()
	st := srv.GeoDRAliases()
	if len(st) != 1 || st[0].Primary != "secondary" || st[0].Paired {
		t.Errorf("after the restart GeoDRAliases() = %+v, want the failover kept", st)
	}
}
