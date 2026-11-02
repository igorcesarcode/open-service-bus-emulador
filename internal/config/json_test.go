package config

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func TestLoadJSON(t *testing.T) {
	data := `{
	  // Comments, as in the official sample.
	  "UserConfig": {
	    "Namespaces": [{
	      "Name": "sbemulatorns",
	      "Queues": [
	        {"Name": "plain"},
	        {
	          "Name": "orders",
	          "Properties": {
	            "LockDuration": "PT5S",
	            "MaxDeliveryCount": 3,
	            "DefaultMessageTimeToLive": "P30D", /* beyond the official 1h cap */
	            "ForwardTo": "http://not-a-comment//x",
	            "UserMetadata": "say \"hi\" // still a string"
	          }
	        }
	      ],
	      "Topics": [{
	        "Name": "events",
	        "Properties": {"DuplicateDetectionHistoryTimeWindow": "PT20S", "RequiresDuplicateDetection": true},
	        "Subscriptions": [{"Name": "audit", "Properties": {"MaxDeliveryCount": 1}}]
	      }]
	    }],
	    "Logging": {"Type": "File"}
	  }
	}`
	got, err := LoadJSON([]byte(data))
	if err != nil {
		t.Fatalf("LoadJSON() error: %v", err)
	}

	orders := broker.DefaultProperties(broker.KindQueue)
	orders.LockDuration = 5 * time.Second
	orders.MaxDeliveryCount = 3
	orders.DefaultMessageTimeToLive = 30 * 24 * time.Hour
	orders.ForwardTo = "http://not-a-comment//x"
	orders.UserMetadata = `say "hi" // still a string`
	events := broker.DefaultProperties(broker.KindTopic)
	events.DuplicateDetectionHistoryTimeWindow = 20 * time.Second
	events.RequiresDuplicateDetection = true
	audit := broker.DefaultProperties(broker.KindSubscription)
	audit.MaxDeliveryCount = 1
	want := &Config{
		Name: "sbemulatorns",
		Queues: []server.Queue{
			{Name: "plain", Properties: broker.DefaultProperties(broker.KindQueue)},
			{Name: "orders", Properties: orders},
		},
		Topics: []server.Topic{{
			Name:          "events",
			Properties:    events,
			Subscriptions: []server.Subscription{{Name: "audit", Properties: audit}},
		}},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("LoadJSON() mismatch (-want +got):\n%s", diff)
	}
}

func TestLoadJSONErrors(t *testing.T) {
	tests := []struct {
		name, data, want string
	}{
		{"syntax", `{"UserConfig": `, "unexpected EOF"},
		{"unknown field", `{"UserConfig": {"Namespaces": [{"Queues": [{"Name": "q", "Properties": {"LockDuraton": "PT1M"}}]}]}}`, "LockDuraton"},
		{"bad duration", `{"UserConfig": {"Namespaces": [{"Queues": [{"Name": "q", "Properties": {"LockDuration": "1m"}}]}]}}`, "ISO 8601"},
		{"invalid value", `{"UserConfig": {"Namespaces": [{"Queues": [{"Name": "q", "Properties": {"MaxDeliveryCount": 0}}]}]}}`, `queue "q"`},
		{"rule type", `{"UserConfig": {"Namespaces": [{"Topics": [{"Name": "t", "Subscriptions": [{"Name": "s", "Rules": [{"Name": "r"}]}]}]}]}}`, "unknown FilterType"},
		{"sql syntax", `{"UserConfig": {"Namespaces": [{"Topics": [{"Name": "t", "Subscriptions": [{"Name": "s", "Rules": [{"Name": "r", "Properties": {"FilterType": "Sql", "SqlFilter": {"SqlExpression": "a ="}}}]}]}]}]}}`, "syntax error"},
		{"action syntax", `{"UserConfig": {"Namespaces": [{"Topics": [{"Name": "t", "Subscriptions": [{"Name": "s", "Rules": [{"Name": "r", "Properties": {"FilterType": "Sql", "SqlFilter": {"SqlExpression": "1=1"}, "Action": {"SqlExpression": "UPDATE"}}}]}]}]}]}}`, "SET or REMOVE"},
		{"forward with sessions", `{"UserConfig": {"Namespaces": [{"Queues": [{"Name": "q", "Properties": {"RequiresSession": true, "ForwardTo": "x"}}]}]}}`, "cannot forward"},
		{"trailing data", `{} {}`, "after the document"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadJSON([]byte(tt.data))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("LoadJSON() error = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}

// TestLoadOfficialSample loads the official emulator's sample, with its
// correlation and SQL rules.
func TestMerge(t *testing.T) {
	long := broker.DefaultProperties(broker.KindQueue)
	long.LockDuration = time.Hour
	rules := []broker.Rule{broker.TrueRule("all")}
	c := &Config{
		Queues: []server.Queue{{Name: "jobs"}, {Name: "kept"}},
		Topics: []server.Topic{{Name: "orders", Subscriptions: []server.Subscription{{Name: "audit", Rules: rules}, {Name: "kept"}}}},
	}
	c.Merge(&Config{
		Queues: []server.Queue{{Name: "JOBS", Properties: long}, {Name: "new"}},
		Topics: []server.Topic{
			{Name: "orders", Subscriptions: []server.Subscription{{Name: "audit", Properties: long}, {Name: "new"}}},
			{Name: "events"},
		},
	})
	want := &Config{
		Queues: []server.Queue{{Name: "JOBS", Properties: long}, {Name: "kept"}, {Name: "new"}},
		Topics: []server.Topic{
			{Name: "orders", Subscriptions: []server.Subscription{{Name: "audit", Properties: long, Rules: rules}, {Name: "kept"}, {Name: "new"}}},
			{Name: "events"},
		},
	}
	if diff := cmp.Diff(want, c, cmp.AllowUnexported(broker.Rule{})); diff != "" {
		t.Errorf("Merge() mismatch (-want +got):\n%s", diff)
	}
}

func TestLoadJSONAuth(t *testing.T) {
	c, err := LoadJSON([]byte(`{"UserConfig": {"Namespaces": [{
		"Name": "ns",
		"LocalAuthEnabled": false,
		"AuthorizationRules": [{"Name": "app", "Rights": ["Listen", "Send"], "PrimaryKey": "pk"}],
		"RoleAssignments": [{"Principal": "oid-1", "Role": "sender", "Scope": "orders"}],
		"Queues": [{"Name": "orders", "AuthorizationRules": [{"Name": "only-send", "Rights": ["send"]}]}],
		"Topics": [{"Name": "events", "AuthorizationRules": [{"Name": "admin", "Rights": ["Manage"]}]}]
	}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if !c.LocalAuthDisabled {
		t.Error("LocalAuthEnabled false was not read")
	}
	byName := map[string]auth.Policy{}
	for _, p := range c.Policies {
		byName[p.Name] = p
	}
	if p := byName["app"]; p.Scope != "" || p.Rights != auth.Listen|auth.Send || p.PrimaryKey != "pk" || p.SecondaryKey != auth.DeclaredKey("", "app", true) {
		t.Errorf("app = %+v", p)
	}
	if p := byName["only-send"]; p.Scope != "orders" || p.Rights != auth.Send || p.PrimaryKey != auth.DeclaredKey("orders", "only-send", false) {
		t.Errorf("only-send = %+v", p)
	}
	if p := byName["admin"]; p.Scope != "events" || !p.Rights.Has(auth.Manage) {
		t.Errorf("admin = %+v", p)
	}
	if len(c.Assignments) != 1 || c.Assignments[0].Role != auth.RoleSender || c.Assignments[0].Scope != "orders" {
		t.Errorf("assignments = %+v", c.Assignments)
	}
	for _, bad := range []string{
		`{"UserConfig": {"Namespaces": [{"Name": "n", "AuthorizationRules": [{"Name": "x", "Rights": ["Read"]}]}]}}`,
		`{"UserConfig": {"Namespaces": [{"Name": "n", "RoleAssignments": [{"Principal": "p", "Role": "Reader"}]}]}}`,
	} {
		if _, err := LoadJSON([]byte(bad)); err == nil {
			t.Errorf("LoadJSON(%s) succeeded", bad)
		}
	}

	// Merge: a later declaration of the same policy replaces the earlier.
	a := &Config{Policies: []auth.Policy{{Name: "p", Rights: auth.Listen}}}
	a.Merge(&Config{Policies: []auth.Policy{{Name: "p", Rights: auth.Send}, {Name: "q", Rights: auth.Listen}}, LocalAuthDisabled: true})
	if len(a.Policies) != 2 || a.Policies[0].Rights != auth.Send || !a.LocalAuthDisabled {
		t.Errorf("merged = %+v", a)
	}
}

func TestLoadJSONSeveralNamespaces(t *testing.T) {
	c, err := LoadJSON([]byte(`{"UserConfig": {
		"Namespaces": [
			{"Name": "orders", "Queues": [{"Name": "q1"}]},
			{"Name": "billing", "Queues": [{"Name": "q1"}, {"Name": "q2"}],
			 "GeoReplication": {"Mode": "Synchronous", "SecondaryRegion": "north"}}
		],
		"GeoDR": [{"Alias": "bus", "Primary": "orders", "Secondary": "billing"}]
	}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Namespaces) != 2 || c.Namespaces[0].Name != "orders" || len(c.Namespaces[1].Queues) != 2 {
		t.Fatalf("namespaces = %+v", c.Namespaces)
	}
	if g := c.Namespaces[1].GeoReplication; g == nil || !g.Synchronous || g.SecondaryRegion != "north" {
		t.Errorf("geo-replication = %+v", g)
	}
	if len(c.GeoDR) != 1 || c.GeoDR[0] != (server.GeoDRConfig{Alias: "bus", Primary: "orders", Secondary: "billing"}) {
		t.Errorf("geo-dr = %+v", c.GeoDR)
	}

	// The default namespace is the first, or the one asked for.
	def, others := c.Split("")
	if def.Name != "orders" || len(others) != 1 || others[0].Name != "billing" {
		t.Errorf("Split(\"\") = %s and %d others", def.Name, len(others))
	}
	def, others = c.Split("BILLING")
	if def.Name != "BILLING" || len(def.Queues) != 2 || len(others) != 1 || others[0].Name != "orders" {
		t.Errorf("Split(BILLING) = %s with %d queues and %d others", def.Name, len(def.Queues), len(others))
	}

	// A bad mode is an error.
	if _, err := LoadJSON([]byte(`{"UserConfig": {"Namespaces": [{"Name": "n", "GeoReplication": {"Mode": "Eventually"}}]}}`)); err == nil {
		t.Error("a bad geo-replication mode was accepted")
	}
}

func TestMergeNamespaces(t *testing.T) {
	a := &Config{Namespaces: []*Config{{Name: "orders", Queues: []server.Queue{{Name: "a"}}}}}
	// Entities with no namespace (from flags or Terraform) join the default.
	b := &Config{Queues: []server.Queue{{Name: "loose"}}}
	c := &Config{Name: "orders", Queues: []server.Queue{{Name: "b"}}}
	a.Merge(b)
	a.Merge(c)
	def, others := a.Split("")
	if def.Name != "orders" || len(def.Queues) != 3 || len(others) != 0 {
		t.Errorf("merged default = %s with %d queues, %d others; want orders with a, loose and b", def.Name, len(def.Queues), len(others))
	}
}

func TestLoadJSONSettings(t *testing.T) {
	c, err := LoadJSON([]byte(`{"UserConfig": {"Namespaces": [{
		"Name": "orders",
		"Tags": {"env": "dev"},
		"Locks": [{"Name": "keep", "Level": "cannotdelete", "Scope": "Orders", "Notes": "n"}, {"Name": "freeze", "Level": "ReadOnly"}],
		"Encryption": {"KeyVaultKeyIds": ["https://vault/keys/k"], "Identity": "id", "InfrastructureEncryption": true},
		"Queues": [{"Name": "Orders"}]
	}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Tags["env"] != "dev" {
		t.Errorf("tags = %v", c.Tags)
	}
	want := []broker.Lock{{Name: "keep", Level: broker.LockCanNotDelete, Scope: "orders", Notes: "n"}, {Name: "freeze", Level: broker.LockReadOnly}}
	if diff := cmp.Diff(want, c.Locks); diff != "" {
		t.Errorf("locks (-want +got):\n%s", diff)
	}
	if e := c.Encryption; e == nil || !e.InfrastructureEncryption || e.Identity != "id" || len(e.KeyVaultKeyIDs) != 1 {
		t.Errorf("encryption = %+v", e)
	}
	for _, bad := range []string{
		`{"UserConfig": {"Namespaces": [{"Name": "a", "Locks": [{"Name": "x", "Level": "Write"}]}]}}`,
		`{"UserConfig": {"Namespaces": [{"Name": "a", "Locks": [{"Level": "ReadOnly"}]}]}}`,
	} {
		if _, err := LoadJSON([]byte(bad)); err == nil {
			t.Errorf("LoadJSON(%s) succeeded, want an error", bad)
		}
	}
}

func TestLoadJSONReportsUnsupportedFeatures(t *testing.T) {
	c, err := LoadJSON([]byte(`{"UserConfig": {"Namespaces": [{
		"Name": "orders",
		"NetworkRuleSet": {"IpRules": [{"IpMask": "1.2.3.4"}], "VirtualNetworkRules": [{"Subnet": "x"}]},
		"PublicNetworkAccess": "Disabled",
		"PrivateEndpoints": [{"Name": "pe"}],
		"Queues": [{"Name": "q"}]
	}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range c.Unsupported {
		got[f.Feature.ID] = true
	}
	for _, id := range []string{"ip-filter", "vnet", "private-link"} {
		if !got[id] {
			t.Errorf("%s was not reported: %+v", id, c.Unsupported)
		}
	}
	if len(c.Queues) != 1 {
		t.Errorf("queues = %+v", c.Queues)
	}
}
