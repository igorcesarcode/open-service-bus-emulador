package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func geoRepServer(t *testing.T, g server.GeoReplication) (*server.Server, *server.Namespace) {
	t.Helper()
	srv := startServer(t, server.Options{
		Namespace:      "geo",
		Queues:         []server.Queue{queue("orders", func(p *broker.Properties) { p.LockDuration = time.Minute })},
		Namespaces:     []server.NamespaceOptions{{Name: "other"}},
		GeoReplication: &g,
	})
	ns, _ := srv.Namespace("geo")
	return srv, ns
}

func waitReady(t *testing.T, ns *server.Namespace) {
	t.Helper()
	waitFor(t, "the secondary region to be Ready", func() bool { return ns.GeoReplicationStatus().State == server.RegionReady })
}

func TestGeoReplicationPlannedPromotionKeepsEverything(t *testing.T) {
	t.Parallel()
	srv, ns := geoRepServer(t, server.GeoReplication{Synchronous: true, PrimaryRegion: "westeurope", SecondaryRegion: "northeurope"})
	client := newClientFor(t, srv.ConnectionString())
	ctx := testContext(t)
	st := ns.GeoReplicationStatus()
	if !st.Enabled || !st.Synchronous || st.PrimaryRegion != "westeurope" || st.SecondaryRegion != "northeurope" || st.State != server.RegionReady {
		t.Fatalf("status = %+v", st)
	}

	send(t, newSender(t, client, "orders"), bodies("m1", "m2", "m3", "m4")...)
	r := newReceiver(t, client, "orders", nil)
	got := receive(t, r, 2) // m1 and m2 are locked.
	if err := r.CompleteMessage(ctx, got[0], nil); err != nil {
		t.Fatal(err)
	}
	if err := ns.PromoteGeoReplication(ctx, false); err != nil {
		t.Fatalf("PromoteGeoReplication() error: %v", err)
	}
	st = ns.GeoReplicationStatus()
	if st.PrimaryRegion != "northeurope" || st.SecondaryRegion != "westeurope" || st.LostChanges != 0 || st.LastPromotion.IsZero() {
		t.Errorf("status after the promotion = %+v", st)
	}

	// Nothing was lost: the completed message stays completed, the lock of
	// the other one still holds, and the rest is there.
	if err := r.CompleteMessage(ctx, got[1], nil); err != nil {
		t.Errorf("completing a message locked before the promotion: %v", err)
	}
	rad := &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete}
	if diff := cmp.Diff([]string{"m3", "m4"}, sortedBodies(receive(t, newReceiver(t, client, "orders", rad), 2))); diff != "" {
		t.Errorf("messages after the promotion (-want +got):\n%s", diff)
	}

	// The old primary is rebuilt as the secondary, and the roles can swap
	// back. A second promotion needs it Ready first.
	waitReady(t, ns)
	send(t, newSender(t, client, "orders"), bodies("later")...)
	if err := ns.PromoteGeoReplication(ctx, false); err != nil {
		t.Fatalf("second PromoteGeoReplication() error: %v", err)
	}
	if st := ns.GeoReplicationStatus(); st.PrimaryRegion != "westeurope" {
		t.Errorf("after promoting back the primary region is %q", st.PrimaryRegion)
	}
	if diff := cmp.Diff([]string{"later"}, bodiesOf(receive(t, newReceiver(t, client, "orders", rad), 1))); diff != "" {
		t.Errorf("messages after promoting back (-want +got):\n%s", diff)
	}
	// Entities and policies come along: the admin API sees the same queue.
	if names := queueNames(t, newAdminClient(t, srv)); !cmp.Equal(names, []string{"orders"}) {
		t.Errorf("queues = %v", names)
	}
}

func TestGeoReplicationAsyncLagAndForcedPromotion(t *testing.T) {
	t.Parallel()
	srv, ns := geoRepServer(t, server.GeoReplication{Delay: 400 * time.Millisecond, MaxLag: time.Hour})
	client := newClientFor(t, srv.ConnectionString())
	ctx := testContext(t)

	// The secondary lags by the delay, and the status says so.
	send(t, newSender(t, client, "orders"), bodies("a", "b")...)
	st := ns.GeoReplicationStatus()
	if st.Synchronous || st.LagCount == 0 || st.LagDuration <= 0 || st.State != server.RegionReady {
		t.Fatalf("status right after a send = %+v, want a lag", st)
	}
	waitFor(t, "the secondary to catch up", func() bool { return ns.GeoReplicationStatus().LagCount == 0 })

	// A forced promotion drops what the secondary has not applied.
	send(t, newSender(t, client, "orders"), bodies("lost-1", "lost-2")...)
	if err := ns.PromoteGeoReplication(ctx, true); err != nil {
		t.Fatalf("forced PromoteGeoReplication() error: %v", err)
	}
	if st := ns.GeoReplicationStatus(); st.LostChanges == 0 {
		t.Errorf("a forced promotion with lag lost no changes: %+v", st)
	}
	rad := &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete}
	if diff := cmp.Diff([]string{"a", "b"}, sortedBodies(receive(t, newReceiver(t, client, "orders", rad), 2))); diff != "" {
		t.Errorf("messages after the forced promotion (-want +got):\n%s", diff)
	}
	if n := peekCount(t, client, "orders", nil); n != 0 {
		t.Errorf("%d messages that the secondary never got came back", n)
	}

	// A planned promotion waits for the lag instead.
	waitReady(t, ns)
	send(t, newSender(t, client, "orders"), bodies("kept-1", "kept-2")...)
	if ns.GeoReplicationStatus().LagCount == 0 {
		t.Fatal("no lag to catch up")
	}
	if err := ns.PromoteGeoReplication(ctx, false); err != nil {
		t.Fatalf("planned PromoteGeoReplication() error: %v", err)
	}
	if st := ns.GeoReplicationStatus(); st.LostChanges != 0 {
		t.Errorf("a planned promotion lost %d changes", st.LostChanges)
	}
	if diff := cmp.Diff([]string{"kept-1", "kept-2"}, sortedBodies(receive(t, newReceiver(t, client, "orders", rad), 2))); diff != "" {
		t.Errorf("messages after the planned promotion (-want +got):\n%s", diff)
	}
}

func TestGeoReplicationThrottlesAtTheMaximumLag(t *testing.T) {
	t.Parallel()
	srv, ns := geoRepServer(t, server.GeoReplication{Delay: 3 * time.Second, MaxLag: 300 * time.Millisecond})
	client := newClientFor(t, srv.ConnectionString())
	sender := newSender(t, client, "orders")
	ctx := testContext(t)
	if err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("first")}, nil); err != nil {
		t.Fatalf("the first send: %v", err)
	}
	// Once the secondary has lagged past the maximum, the primary refuses.
	waitFor(t, "the primary to throttle", func() bool {
		err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("more")}, nil)
		return err != nil
	})
	var sbErr *azservicebus.Error
	err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("again")}, nil)
	if err == nil {
		t.Fatal("a send was accepted while throttled")
	}
	_ = errors.As(err, &sbErr)
	if st := ns.GeoReplicationStatus(); st.LagDuration < 300*time.Millisecond {
		t.Errorf("throttling with a lag of %v", st.LagDuration)
	}
	// It recovers when the secondary catches up.
	waitFor(t, "the lag to drain", func() bool { return ns.GeoReplicationStatus().LagCount == 0 })
	if err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("recovered")}, nil); err != nil {
		t.Errorf("a send after the lag drained: %v", err)
	}
}

func TestGeoReplicationAndGeoDRExcludeEachOther(t *testing.T) {
	t.Parallel()
	srv, ns := geoRepServer(t, server.GeoReplication{Synchronous: true})
	ctx := testContext(t)
	if err := srv.PairGeoDR(ctx, "bus", "geo", "other"); !errors.Is(err, server.ErrGeoDR) {
		t.Errorf("pairing a geo-replicated namespace = %v, want a Geo-DR error", err)
	}
	if err := srv.PairGeoDR(ctx, "bus", "other", "geo"); !errors.Is(err, server.ErrGeoDR) {
		t.Errorf("pairing with a geo-replicated secondary = %v, want a Geo-DR error", err)
	}
	other, _ := srv.Namespace("other")
	if err := other.EnableGeoReplication(ctx, server.GeoReplication{}); err != nil {
		t.Fatal(err)
	}
	if err := ns.EnableGeoReplication(ctx, server.GeoReplication{}); !errors.Is(err, server.ErrGeoReplication) {
		t.Errorf("enabling twice = %v", err)
	}
	if err := other.DisableGeoReplication(); err != nil {
		t.Fatal(err)
	}
	if st := other.GeoReplicationStatus(); st.Enabled {
		t.Errorf("status after disabling = %+v", st)
	}
	if err := other.DisableGeoReplication(); !errors.Is(err, server.ErrGeoReplication) {
		t.Errorf("disabling twice = %v", err)
	}
	// Without replication, promotion makes no sense.
	if err := other.PromoteGeoReplication(ctx, false); !errors.Is(err, server.ErrGeoReplication) {
		t.Errorf("promoting without replication = %v", err)
	}
}

func TestGeoReplicationInAFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	srv, err := server.New(ctx, server.Options{
		Addr: "127.0.0.1:0", DBPath: dir + "/osb.db", Queues: queues("orders"),
		GeoReplication: &server.GeoReplication{Synchronous: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	client := newClientFor(t, srv.ConnectionString())
	send(t, newSender(t, client, "orders"), bodies("persisted")...)
	ns, _ := srv.Namespace("emulator")
	if err := ns.PromoteGeoReplication(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	// After a promotion the old primary's file was rebuilt as the
	// secondary, so the file the server opens holds everything.
	srv2, err := server.New(ctx, server.Options{Addr: "127.0.0.1:0", DBPath: dir + "/osb.db"})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv2.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	rad := &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete}
	client2 := newClientFor(t, srv2.ConnectionString())
	if diff := cmp.Diff([]string{"persisted"}, bodiesOf(receive(t, newReceiver(t, client2, "orders", rad), 1))); diff != "" {
		t.Errorf("messages after the restart (-want +got):\n%s", diff)
	}
}
