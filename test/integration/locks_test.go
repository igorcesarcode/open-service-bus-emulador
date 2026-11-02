package integration_test

import (
	"context"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func TestResourceLocksSDK(t *testing.T) {
	t.Parallel()
	idle := func(p *broker.Properties) { p.AutoDeleteOnIdle = time.Second }
	srv := startServer(t, server.Options{
		Queues: []server.Queue{
			{Name: "frozen"}, {Name: "kept"}, {Name: "free"},
			queue("idle-kept", idle), queue("idle-free", idle),
		},
		Topics: []server.Topic{{Name: "events", Subscriptions: []server.Subscription{{Name: "audit"}, {Name: "billing"}}}},
		Locks: []broker.Lock{
			{Name: "freeze", Level: broker.LockReadOnly, Scope: "frozen", Notes: "do not touch"},
			{Name: "keep", Level: broker.LockCanNotDelete, Scope: "kept"},
			{Name: "keep-idle", Level: broker.LockCanNotDelete, Scope: "idle-kept"},
			{Name: "keep-audit", Level: broker.LockCanNotDelete, Scope: "events/subscriptions/audit"},
		},
	})
	ac := newAdminClient(t, srv)
	client := newClient(t, srv)
	ctx := testContext(t)

	// ReadOnly stops changes and deletes, as a 409 with the Azure message.
	got, err := ac.GetQueue(ctx, "frozen", nil)
	if err != nil || got == nil {
		t.Fatalf("GetQueue(frozen) = %v, %v", got, err)
	}
	got.MaxDeliveryCount = to.Ptr(int32(3))
	_, err = ac.UpdateQueue(ctx, "frozen", got.QueueProperties, nil)
	if responseStatus(err) != http.StatusConflict || !strings.Contains(err.Error(), "ScopeLocked") || !strings.Contains(err.Error(), "locked") {
		t.Errorf("UpdateQueue(frozen) error = %v, want 409 ScopeLocked", err)
	}
	if _, err := ac.DeleteQueue(ctx, "frozen", nil); responseStatus(err) != http.StatusConflict {
		t.Errorf("DeleteQueue(frozen) error = %v, want 409", err)
	}
	// Messages still flow.
	send(t, newSender(t, client, "frozen"), &azservicebus.Message{Body: []byte("x")})
	msgs, err := newReceiver(t, client, "frozen", nil).ReceiveMessages(ctx, 1, nil)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("ReceiveMessages(frozen) = %d, %v", len(msgs), err)
	}

	// CanNotDelete stops deletes, not changes.
	kept, _ := ac.GetQueue(ctx, "kept", nil)
	kept.MaxDeliveryCount = to.Ptr(int32(4))
	if _, err := ac.UpdateQueue(ctx, "kept", kept.QueueProperties, nil); err != nil {
		t.Errorf("UpdateQueue(kept) error: %v", err)
	}
	if _, err := ac.DeleteQueue(ctx, "kept", nil); responseStatus(err) != http.StatusConflict {
		t.Errorf("DeleteQueue(kept) error = %v, want 409", err)
	}
	if _, err := ac.DeleteQueue(ctx, "free", nil); err != nil {
		t.Errorf("DeleteQueue(free) error: %v", err)
	}

	// A lock below an entity stops the delete of the entity, and the lock
	// on a subscription its rules' changes only when it is ReadOnly.
	if _, err := ac.DeleteTopic(ctx, "events", nil); responseStatus(err) != http.StatusConflict {
		t.Errorf("DeleteTopic(events) error = %v, want 409 for the locked subscription", err)
	}
	if _, err := ac.DeleteSubscription(ctx, "events", "audit", nil); responseStatus(err) != http.StatusConflict {
		t.Errorf("DeleteSubscription(audit) error = %v, want 409", err)
	}
	if _, err := ac.DeleteSubscription(ctx, "events", "billing", nil); err != nil {
		t.Errorf("DeleteSubscription(billing) error: %v", err)
	}
	if _, err := ac.CreateRule(ctx, "events", "audit", &admin.CreateRuleOptions{Name: to.Ptr("r"), Filter: &admin.TrueFilter{}}); err != nil {
		t.Errorf("CreateRule(audit) error: %v", err)
	}

	// The auto-delete on idle respects CanNotDelete.
	deadline := time.Now().Add(10 * time.Second)
	for {
		q, err := ac.GetQueue(ctx, "idle-free", nil)
		if err != nil {
			t.Fatalf("GetQueue(idle-free) error: %v", err)
		}
		if q == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle-free was not deleted")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if q, err := ac.GetQueue(ctx, "idle-kept", nil); err != nil || q == nil {
		t.Errorf("GetQueue(idle-kept) = %v, %v; want it kept by its lock", q, err)
	}

	// A lock on the namespace, added and removed at run time.
	b := srv.Broker()
	if err := b.SetLock(ctx, broker.Lock{Name: "ns", Level: broker.LockReadOnly}); err != nil {
		t.Fatal(err)
	}
	if _, err := ac.CreateQueue(ctx, "new", nil); responseStatus(err) != http.StatusConflict {
		t.Errorf("CreateQueue(new) under a ReadOnly lock error = %v, want 409", err)
	}
	if err := b.DeleteLock(ctx, "ns"); err != nil {
		t.Fatal(err)
	}
	if _, err := ac.CreateQueue(ctx, "new", nil); err != nil {
		t.Errorf("CreateQueue(new) after removing the lock error: %v", err)
	}

	// The refusals are in the activity log.
	entries, err := b.Activity(ctx, broker.ActivityQuery{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	failed := 0
	for _, e := range entries {
		if e.Status == "Failed" && strings.Contains(e.Detail, "ScopeLocked") {
			failed++
		}
	}
	if failed < 4 {
		t.Errorf("activity log has %d ScopeLocked failures, want at least 4", failed)
	}
}

func TestNamespaceSettingsPersist(t *testing.T) {
	t.Parallel()
	db := filepath.Join(t.TempDir(), "osb.db")
	open := func(opts server.Options) *server.Server {
		opts.DBPath = db
		opts.Addr = "127.0.0.1:0"
		srv, err := server.New(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		return srv
	}
	first := open(server.Options{
		Queues:     queues("orders"),
		Tags:       map[string]string{"team": "billing"},
		Locks:      []broker.Lock{{Name: "keep", Level: broker.LockCanNotDelete, Scope: "orders", Notes: "n"}},
		Encryption: &broker.Encryption{KeyVaultKeyIDs: []string{"https://vault/keys/k"}, Identity: "id", InfrastructureEncryption: true},
	})
	want := first.Broker().Meta()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopened with no declaration at all, the namespace keeps them.
	second := open(server.Options{})
	defer second.Close()
	if got := second.Broker().Meta(); !reflect.DeepEqual(got, want) {
		t.Errorf("Meta() after a restart = %+v, want %+v", got, want)
	}
	if err := second.Broker().DeleteEntity(context.Background(), "orders"); err == nil {
		t.Error("DeleteEntity(orders) after a restart succeeded under a CanNotDelete lock")
	}
	if err := second.Broker().SetTags(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if tags := second.Broker().Meta().Tags; len(tags) != 0 {
		t.Errorf("Tags = %v after clearing", tags)
	}
}
