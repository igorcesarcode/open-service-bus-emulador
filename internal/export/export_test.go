package export_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/config"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/export"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/terraform"
)

func props(kind broker.EntityKind, edit func(*broker.Properties)) broker.Properties {
	p := broker.DefaultProperties(kind)
	edit(&p)
	return p
}

func sqlRule(name, expr, action string) broker.Rule {
	return broker.Rule{Name: name, Filter: broker.RuleFilter{Kind: broker.FilterSQL, SQL: expr}, Action: action}
}

// fixture is a server with something of everything an export carries.
func fixture(t *testing.T, geoReplication bool) *server.Server {
	t.Helper()
	opts := server.Options{
		Namespace: "orders",
		Queues: []server.Queue{
			{Name: "inbox", Properties: props(broker.KindQueue, func(p *broker.Properties) {
				p.LockDuration = 2 * time.Minute
				p.MaxDeliveryCount = 5
				p.RequiresDuplicateDetection = true
				p.DefaultMessageTimeToLive = 14 * 24 * time.Hour
				p.ForwardDeadLetteredMessagesTo = "poison"
				p.MaxSizeInMegabytes = 5120
			})},
			{Name: "poison"},
			{Name: "sessions", Properties: props(broker.KindQueue, func(p *broker.Properties) { p.RequiresSession = true; p.AutoDeleteOnIdle = time.Hour })},
		},
		Topics: []server.Topic{{
			Name:       "events",
			Properties: props(broker.KindTopic, func(p *broker.Properties) { p.SupportOrdering = false; p.MaxSizeInMegabytes = 2048 }),
			Subscriptions: []server.Subscription{
				{Name: "plain"},
				{Name: "audit", Properties: props(broker.KindSubscription, func(p *broker.Properties) { p.MaxDeliveryCount = 3 }), Rules: []broker.Rule{
					broker.TrueRule(broker.DefaultRuleName),
					sqlRule("big", "quantity > 10 AND region = 'eu'", "SET tier = 'big'"),
					{Name: "eu", Filter: broker.RuleFilter{Kind: broker.FilterCorrelation, Correlation: &filter.CorrelationFilter{
						Subject: "order.created", ContentType: "application/json", Properties: filter.Values{"region": "eu"}}}},
				}},
			},
		}},
		Policies: []auth.Policy{
			{Name: "app", Rights: auth.Listen | auth.Send},
			{Scope: "inbox", Name: "reader", Rights: auth.Listen},
			{Scope: "events", Name: "publisher", Rights: auth.Send},
		},
		Assignments: []server.Assignment{
			{Principal: "11111111-1111-1111-1111-111111111111", Role: auth.RoleSender, Scope: "inbox"},
			{Principal: "22222222-2222-2222-2222-222222222222", Role: auth.RoleOwner},
		},
		DisableLocalAuth: true,
		Tags:             map[string]string{"env": "dev", "team": "billing"},
		Locks: []broker.Lock{
			{Name: "keep-inbox", Level: broker.LockCanNotDelete, Scope: "inbox", Notes: "production data"},
			{Name: "freeze", Level: broker.LockReadOnly},
			{Name: "keep-audit", Level: broker.LockCanNotDelete, Scope: "events/subscriptions/audit"},
		},
		Encryption: &broker.Encryption{KeyVaultKeyIDs: []string{"https://vault.example/keys/k1"}, Identity: "/subscriptions/s/identities/i", InfrastructureEncryption: true},
		Namespaces: []server.NamespaceOptions{{Name: "spare", Queues: []server.Queue{{Name: "extra"}}}},
		GeoDR:      []server.GeoDRConfig{{Alias: "bus", Primary: "orders", Secondary: "spare"}},
	}
	if geoReplication {
		opts.Namespaces = []server.NamespaceOptions{{Name: "spare", Queues: []server.Queue{{Name: "extra"}},
			GeoReplication: &server.GeoReplication{Synchronous: true, PrimaryRegion: "west", SecondaryRegion: "east"}}}
		opts.GeoDR = nil
	}
	srv, err := server.New(context.Background(), opts)
	if err != nil {
		t.Fatalf("server.New() error: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}

func start(t *testing.T, c *config.Config) *server.Server {
	t.Helper()
	def, others := c.Split("")
	opts := server.Options{
		Namespace: def.Name, Queues: def.Queues, Topics: def.Topics, Policies: def.Policies, Assignments: def.Assignments,
		DisableLocalAuth: def.LocalAuthDisabled, Tags: def.Tags, Locks: def.Locks, Encryption: def.Encryption,
		GeoReplication: def.GeoReplication, GeoDR: c.GeoDR,
	}
	for _, o := range others {
		opts.Namespaces = append(opts.Namespaces, server.NamespaceOptions{
			Name: o.Name, Queues: o.Queues, Topics: o.Topics, Policies: o.Policies, Assignments: o.Assignments,
			DisableLocalAuth: o.LocalAuthDisabled, Tags: o.Tags, Locks: o.Locks, Encryption: o.Encryption, GeoReplication: o.GeoReplication,
		})
	}
	srv, err := server.New(context.Background(), opts)
	if err != nil {
		t.Fatalf("starting from the export: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}

func dump(t *testing.T, c *config.Config) string {
	t.Helper()
	b, err := config.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestConfigJSONRoundTrip(t *testing.T) {
	for _, geo := range []bool{false, true} {
		first := fixture(t, geo)
		want := dump(t, config.Snapshot(first))

		files, err := export.Files(first)
		if err != nil {
			t.Fatal(err)
		}
		again, err := config.LoadJSON(files[0].Data)
		if err != nil {
			t.Fatalf("LoadJSON(export) error: %v\n%s", err, files[0].Data)
		}
		got := dump(t, config.Snapshot(start(t, again)))
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("geo replication %v: Config.json after a round trip differs (-first +second):\n%s", geo, diff)
		}
	}
}

// withoutWhatTerraformLoses removes what Terraform cannot carry: the keys of
// access policies, which it computes, and Geo-Replication.
func withoutWhatTerraformLoses(c *config.Config) {
	for _, ns := range append([]*config.Config{c}, c.Namespaces...) {
		for i := range ns.Policies {
			ns.Policies[i].PrimaryKey, ns.Policies[i].SecondaryKey = "", ""
		}
		ns.GeoReplication = nil
	}
}

func TestTerraformRoundTrip(t *testing.T) {
	first := fixture(t, false)
	snap := config.Snapshot(first)
	files, err := export.Files(first)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, files[1].Name), files[1].Data, 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := terraform.Load(dir, terraform.Options{})
	if err != nil {
		t.Fatalf("terraform.Load(export) error: %v\n%s", err, files[1].Data)
	}
	got := config.Snapshot(start(t, r.Config))
	withoutWhatTerraformLoses(snap)
	withoutWhatTerraformLoses(got)
	if diff := cmp.Diff(dump(t, snap), dump(t, got)); diff != "" {
		t.Errorf("Terraform after a round trip differs (-first +second):\n%s\n%s", diff, files[1].Data)
	}
	for _, want := range []string{"azurerm_management_lock", "lock_level", "customer_managed_key", "azurerm_servicebus_namespace_disaster_recovery_config", "tags"} {
		if !strings.Contains(string(files[1].Data), want) {
			t.Errorf("main.tf has no %q:\n%s", want, files[1].Data)
		}
	}
}

func TestWriteDirectory(t *testing.T) {
	srv := fixture(t, false)
	dir := filepath.Join(t.TempDir(), "out", "nested")
	if err := export.Write(srv, dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Config.json", "main.tf"} {
		if st, err := os.Stat(filepath.Join(dir, name)); err != nil || st.Size() == 0 {
			t.Errorf("%s: %v", name, err)
		}
	}
}
