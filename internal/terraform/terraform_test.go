package terraform

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/config"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// writeFiles creates files, keyed by path relative to a temporary
// directory, and returns the directory.
func writeFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func mustLoad(t *testing.T, path string, opts Options) *Result {
	t.Helper()
	r, err := Load(path, opts)
	if err != nil {
		t.Fatalf("Load(%s) error: %v", path, err)
	}
	return r
}

func findQueue(t *testing.T, c *config.Config, name string) server.Queue {
	t.Helper()
	for _, q := range c.Queues {
		if q.Name == name {
			return q
		}
	}
	t.Fatalf("queue %q not declared", name)
	return server.Queue{}
}

func findTopic(t *testing.T, c *config.Config, name string) server.Topic {
	t.Helper()
	for _, tp := range c.Topics {
		if tp.Name == name {
			return tp
		}
	}
	t.Fatalf("topic %q not declared", name)
	return server.Topic{}
}

func findSub(t *testing.T, c *config.Config, topic, name string) server.Subscription {
	t.Helper()
	for _, s := range findTopic(t, c, topic).Subscriptions {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("subscription %s/%s not declared", topic, name)
	return server.Subscription{}
}

func names[T any](items []T, name func(T) string) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = name(it)
	}
	return out
}

func TestExample(t *testing.T) {
	r := mustLoad(t, "../../examples/terraform", Options{})
	c := r.Config

	if diff := cmp.Diff([]string{"orders-inbox", "orders-poison", "chat-sessions", "legacy-orders"},
		names(c.Queues, func(q server.Queue) string { return q.Name })); diff != "" {
		t.Errorf("queues mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"orders", "customer", "notification", "payment-hub"},
		names(c.Topics, func(t server.Topic) string { return t.Name })); diff != "" {
		t.Errorf("topics mismatch (-want +got):\n%s", diff)
	}

	want := broker.DefaultProperties(broker.KindQueue)
	want.LockDuration = 2 * time.Minute
	want.MaxDeliveryCount = 5
	want.DefaultMessageTimeToLive = 14 * 24 * time.Hour
	want.AutoDeleteOnIdle = duration.Infinite
	want.DeadLetteringOnMessageExpiration = true
	want.RequiresDuplicateDetection = true
	want.DuplicateDetectionHistoryTimeWindow = 10 * time.Minute
	want.MaxSizeInMegabytes = 5120
	want.MaxMessageSizeInKilobytes = 1024
	want.ForwardDeadLetteredMessagesTo = "orders-poison"
	if diff := cmp.Diff(want, findQueue(t, c, "orders-inbox").Properties); diff != "" {
		t.Errorf("orders-inbox properties mismatch (-want +got):\n%s", diff)
	}
	if got := findQueue(t, c, "legacy-orders").Properties.ForwardTo; got != "orders" {
		t.Errorf("legacy-orders ForwardTo = %q, want the topic orders", got)
	}
	if p := findTopic(t, c, "orders").Properties; p.DuplicateDetectionHistoryTimeWindow != 5*time.Minute || !p.SupportOrdering {
		t.Errorf("orders topic properties = %+v", p)
	}

	sub := findSub(t, c, "orders", "order-enrichment")
	if p := sub.Properties; p.MaxDeliveryCount != 3 || p.LockDuration != 30*time.Second || p.ForwardDeadLetteredMessagesTo != "orders-poison" {
		t.Errorf("order-enrichment properties = %+v", p)
	}
	// $Default stays next to the declared rule, as in Azure.
	wantRules := []broker.Rule{
		broker.TrueRule(broker.DefaultRuleName),
		{Name: "analyze", Filter: broker.RuleFilter{Kind: broker.FilterSQL, SQL: "event IN ('analyze', 'reanalyze') AND isApproved = TRUE"}, Action: "SET stage = 'enrichment'; REMOVE debug"},
	}
	if diff := cmp.Diff(wantRules, sub.Rules, cmp.AllowUnexported(broker.Rule{})); diff != "" {
		t.Errorf("order-enrichment rules mismatch (-want +got):\n%s", diff)
	}
	if got := findSub(t, c, "orders", "audit").Properties.ForwardTo; got != "orders-inbox" {
		t.Errorf("audit ForwardTo = %q, want orders-inbox", got)
	}
	crm := findSub(t, c, "customer", "crm")
	if !crm.Properties.RequiresSession || len(crm.Rules) != 2 {
		t.Fatalf("customer/crm = %+v", crm)
	}
	wantCorrelation := &filter.CorrelationFilter{
		ContentType: "application/json", CorrelationID: "crm", Subject: "customer-updated", MessageID: "fixed-message-id",
		ReplyTo: "crm-replies", ReplyToSessionID: "crm-session", SessionID: "tenant-1", To: "crm-service",
		Properties: map[string]any{"source": "profile-service", "version": "1"},
	}
	if diff := cmp.Diff(wantCorrelation, crm.Rules[1].Filter.Correlation); diff != "" {
		t.Errorf("crm correlation filter mismatch (-want +got):\n%s", diff)
	}
	// A subscription without rules keeps whatever it has.
	if s := findSub(t, c, "orders", "audit"); s.Rules != nil {
		t.Errorf("audit rules = %v, want nil", s.Rules)
	}
	if len(r.Warnings) != 4 {
		t.Errorf("warnings = %q, want one per subscription keeping $Default", r.Warnings)
	}
}

func TestDropDefaultRule(t *testing.T) {
	c := mustLoad(t, "../../examples/terraform", Options{DropDefaultRule: true}).Config
	rules := findSub(t, c, "orders", "report").Rules
	if len(rules) != 1 || rules[0].Name != "report" {
		t.Errorf("report rules = %+v, want only the declared rule", rules)
	}
}

func TestPrivateModule(t *testing.T) {
	r := mustLoad(t, "../../test/integration/testdata/private-module/main.tf", Options{})
	c := r.Config
	if len(c.Queues) != 1 || c.Queues[0].Name != "sherlog" {
		t.Errorf("queues = %v, want sherlog", c.Queues)
	}
	if len(c.Topics) != 36 {
		t.Errorf("%d topics, want 36", len(c.Topics))
	}
	subs := 0
	for _, tp := range c.Topics {
		subs += len(tp.Subscriptions)
	}
	if subs != 136 {
		t.Errorf("%d subscriptions, want 136", subs)
	}
	// Names repeat across topics.
	for _, topic := range []string{"customer", "orders", "coupon", "profile-service-worker", "parking", "eventhub"} {
		findSub(t, c, topic, "crm")
	}
	s := findSub(t, c, "orders", "control-order")
	if s.Properties.LockDuration != 30*time.Second || s.Properties.MaxDeliveryCount != 3 {
		t.Errorf("orders/control-order properties = %+v", s.Properties)
	}
	want := []broker.Rule{{Name: "$Default", Filter: broker.RuleFilter{Kind: broker.FilterSQL, SQL: "event in ('reanalyze', 'process', 'error_enrichment')"}}}
	if diff := cmp.Diff(want, s.Rules, cmp.AllowUnexported(broker.Rule{})); diff != "" {
		t.Errorf("control-order rules mismatch (-want +got):\n%s", diff)
	}
	if p := findSub(t, c, "genia-messages-ingress", "status-change-enrichment").Properties; p.LockDuration != 5*time.Minute || p.MaxDeliveryCount != 2 {
		t.Errorf("status-change-enrichment properties = %+v", p)
	}
	if len(r.Warnings) != 0 {
		t.Errorf("warnings = %q, want none", r.Warnings)
	}
}

func TestVariables(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"main.tf": `
variable "env" {
  type = string
}
variable "count_queues" {
  type    = number
  default = 1
}
variable "ttl" {
  default = "PT1H"
}
variable "from_file" {}
variable "from_env" {}
locals {
  names  = [for i in range(var.count_queues) : "${local.prefix}-${i}"]
  prefix = "q-${var.env}"
}
resource "azurerm_servicebus_queue" "q" {
  count               = length(local.names)
  name                = local.names[count.index]
  namespace_id        = "x"
  default_message_ttl = var.ttl
  forward_to          = "${var.from_file}-${var.from_env}"
}
`,
		"terraform.tfvars": `from_file = "file"`,
		"x.auto.tfvars":    `ttl = "PT2H"`,
	})
	t.Setenv("TF_VAR_from_env", "env")
	c := mustLoad(t, dir, Options{Vars: map[string]string{"env": "dev", "count_queues": "2"}}).Config
	if diff := cmp.Diff([]string{"q-dev-0", "q-dev-1"}, names(c.Queues, func(q server.Queue) string { return q.Name })); diff != "" {
		t.Errorf("queues mismatch (-want +got):\n%s", diff)
	}
	if p := c.Queues[1].Properties; p.DefaultMessageTimeToLive != 2*time.Hour || p.ForwardTo != "file-env" {
		t.Errorf("properties = %+v, want ttl from x.auto.tfvars and forward_to from tfvars and TF_VAR_", p)
	}

	// Without env, the queue names are unknown.
	if _, err := Load(dir, Options{}); err == nil || !strings.Contains(err.Error(), "-tf-var") {
		t.Errorf("Load() without env error = %v, want a hint to set -tf-var", err)
	}
}

func TestForEachAndReferences(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"main.tf": `
resource "azurerm_servicebus_namespace" "ns" {
  name                = "ns"
  resource_group_name = "rg"
}
resource "azurerm_servicebus_topic" "t" {
  for_each     = { orders = "P1D", events = "P2D" }
  name         = each.key
  namespace_id = azurerm_servicebus_namespace.ns.id
  default_message_ttl = each.value
}
resource "azurerm_servicebus_subscription" "s" {
  for_each           = azurerm_servicebus_topic.t
  name               = "all-${each.key}"
  topic_id           = each.value.id
  max_delivery_count = 2
  forward_to         = azurerm_servicebus_queue.sink.name
}
resource "azurerm_servicebus_queue" "sink" {
  name         = upper("sink")
  namespace_id = azurerm_servicebus_namespace.ns.id
  unknown_arg  = true
}
`,
	})
	r := mustLoad(t, dir, Options{})
	s := findSub(t, r.Config, "events", "all-events")
	if s.Properties.ForwardTo != "SINK" || s.Properties.MaxDeliveryCount != 2 {
		t.Errorf("all-events properties = %+v", s.Properties)
	}
	if ttl := findTopic(t, r.Config, "orders").Properties.DefaultMessageTimeToLive; ttl != 24*time.Hour {
		t.Errorf("orders ttl = %v, want 24h", ttl)
	}
	if len(r.Warnings) != 1 || !strings.Contains(r.Warnings[0], "unknown_arg") {
		t.Errorf("warnings = %q, want one for unknown_arg", r.Warnings)
	}
}

func TestOlderProviderArguments(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"main.tf": `
resource "azurerm_servicebus_topic" "t" {
  name                      = "t"
  namespace_name            = "ns"
  resource_group_name       = "rg"
  enable_batched_operations = false
}
resource "azurerm_servicebus_subscription" "s" {
  name                = "s"
  topic_name          = "t"
  namespace_name      = "ns"
  resource_group_name = "rg"
  max_delivery_count  = 1
}
resource "azurerm_servicebus_subscription_rule" "r" {
  name                = "r"
  topic_name          = "t"
  subscription_name   = "s"
  namespace_name      = "ns"
  resource_group_name = "rg"
  filter_type         = "SqlFilter"
  sql_filter          = "a = 1"
}
`,
	})
	c := mustLoad(t, dir, Options{DropDefaultRule: true}).Config
	if findTopic(t, c, "t").Properties.EnableBatchedOperations {
		t.Error("enable_batched_operations = false was not applied")
	}
	if rules := findSub(t, c, "t", "s").Rules; len(rules) != 1 || rules[0].Filter.SQL != "a = 1" {
		t.Errorf("rules = %+v", rules)
	}
}

func TestModules(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"main.tf": `
module "local" {
  source = "./modules/bus"
  queues = ["a", "b"]
  secret = var.missing
}
module "remote" {
  source = "git::https://example.com/bus.git"
  queues = ["c"]
}
variable "missing" {}
`,
		"modules/bus/main.tf": `
variable "queues" { type = list(string) }
variable "secret" {}
resource "azurerm_servicebus_queue" "q" {
  for_each     = toset(var.queues)
  name         = each.key
  namespace_id = "x"
}
`,
		// terraform init downloaded the remote module.
		".terraform/modules/modules.json": `{"Modules":[{"Key":"remote","Source":"git::https://example.com/bus.git","Dir":".terraform/modules/remote"}]}`,
		".terraform/modules/remote/main.tf": `
variable "queues" { type = list(string) }
resource "azurerm_servicebus_queue" "q" {
  for_each     = toset(var.queues)
  name         = "remote-${each.key}"
  namespace_id = "x"
}
`,
	})
	c := mustLoad(t, dir, Options{}).Config
	if diff := cmp.Diff([]string{"a", "b", "remote-c"}, names(c.Queues, func(q server.Queue) string { return q.Name })); diff != "" {
		t.Errorf("queues mismatch (-want +got):\n%s", diff)
	}
}

func TestErrors(t *testing.T) {
	for name, tf := range map[string]string{
		"bad sql": `
resource "azurerm_servicebus_topic" "t" { name = "t" }
resource "azurerm_servicebus_subscription" "s" {
  name     = "s"
  topic_id = azurerm_servicebus_topic.t.id
}
resource "azurerm_servicebus_subscription_rule" "r" {
  name            = "r"
  subscription_id = azurerm_servicebus_subscription.s.id
  filter_type     = "SqlFilter"
  sql_filter      = "a = = 1"
}`,
		"for_each over a list": `
resource "azurerm_servicebus_queue" "q" {
  for_each = ["a"]
  name     = each.key
}`,
		"duplicate queue": `
resource "azurerm_servicebus_queue" "a" { name = "q" }
resource "azurerm_servicebus_queue" "b" { name = "Q" }`,
		"bad duration": `
resource "azurerm_servicebus_queue" "q" {
  name          = "q"
  lock_duration = "1 minute"
}`,
		"undeclared subscription": `
resource "azurerm_servicebus_subscription_rule" "r" {
  name            = "r"
  subscription_id = "/topics/t/subscriptions/s"
  filter_type     = "SqlFilter"
  sql_filter      = "1=1"
}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := writeFiles(t, map[string]string{"main.tf": tf})
			_, err := Load(dir, Options{})
			if err == nil {
				t.Fatal("Load() succeeded, want an error")
			}
			t.Log(err)
		})
	}
}

func TestAuthorizationRules(t *testing.T) {
	dir := writeFiles(t, map[string]string{"main.tf": `
resource "azurerm_servicebus_namespace" "ns" {
  name                = "prod-bus"
  resource_group_name = "rg"
  location            = "westeurope"
  sku                 = "Standard"
  local_auth_enabled  = false
}
resource "azurerm_servicebus_queue" "orders" {
  name         = "orders"
  namespace_id = azurerm_servicebus_namespace.ns.id
}
resource "azurerm_servicebus_topic" "events" {
  name         = "events"
  namespace_id = azurerm_servicebus_namespace.ns.id
}
resource "azurerm_servicebus_namespace_authorization_rule" "app" {
  name         = "app"
  namespace_id = azurerm_servicebus_namespace.ns.id
  listen       = true
  send         = true
}
resource "azurerm_servicebus_queue_authorization_rule" "orders_send" {
  name     = "orders-send"
  queue_id = azurerm_servicebus_queue.orders.id
  send     = true
}
resource "azurerm_servicebus_topic_authorization_rule" "events_admin" {
  name     = "events-admin"
  topic_id = azurerm_servicebus_topic.events.id
  manage   = true
  listen   = true
  send     = true
}
resource "azurerm_role_assignment" "sender" {
  scope                = azurerm_servicebus_queue.orders.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = "11111111-1111-1111-1111-111111111111"
}
resource "azurerm_role_assignment" "owner" {
  scope                = azurerm_servicebus_namespace.ns.id
  role_definition_name = "Azure Service Bus Data Owner"
  principal_id         = "22222222-2222-2222-2222-222222222222"
}
resource "azurerm_role_assignment" "other" {
  scope                = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg"
  role_definition_name = "Reader"
  principal_id         = "33333333-3333-3333-3333-333333333333"
}
`})
	c := mustLoad(t, dir, Options{}).Config
	if !c.LocalAuthDisabled {
		t.Error("local_auth_enabled = false was not read")
	}
	type pol struct {
		Scope, Name string
		Rights      auth.Right
	}
	var got []pol
	for _, p := range c.Policies {
		got = append(got, pol{p.Scope, p.Name, p.Rights})
		if p.PrimaryKey != auth.DeclaredKey(p.Scope, p.Name, false) || p.SecondaryKey == p.PrimaryKey || p.PrimaryKey == "" {
			t.Errorf("policy %s keys = %q, %q", p.Name, p.PrimaryKey, p.SecondaryKey)
		}
	}
	want := []pol{
		{"", "app", auth.Listen | auth.Send},
		{"orders", "orders-send", auth.Send},
		{"events", "events-admin", auth.Listen | auth.Send | auth.Manage},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("policies mismatch (-want +got):\n%s", diff)
	}
	wantAssign := []server.Assignment{
		{Principal: "11111111-1111-1111-1111-111111111111", Role: auth.RoleSender, Scope: "orders"},
		{Principal: "22222222-2222-2222-2222-222222222222", Role: auth.RoleOwner},
	}
	if diff := cmp.Diff(wantAssign, c.Assignments); diff != "" {
		t.Errorf("assignments mismatch (-want +got):\n%s", diff)
	}
	// A rule with no right is an error.
	bad := writeFiles(t, map[string]string{"main.tf": `
resource "azurerm_servicebus_namespace_authorization_rule" "x" {
  name = "x"
}`})
	if _, err := Load(bad, Options{}); err == nil || !strings.Contains(err.Error(), "listen, send and manage") {
		t.Errorf("a rule without rights: %v", err)
	}
}

func TestSeveralNamespaces(t *testing.T) {
	dir := writeFiles(t, map[string]string{"main.tf": `
resource "azurerm_servicebus_namespace" "primary" {
  name                = "orders-eu"
  resource_group_name = "rg"
  location            = "westeurope"
  sku                 = "Premium"
}
resource "azurerm_servicebus_namespace" "secondary" {
  name                = "orders-us"
  resource_group_name = "rg"
  location            = "eastus"
  sku                 = "Premium"
  local_auth_enabled  = false
}
resource "azurerm_servicebus_queue" "eu" {
  name         = "work"
  namespace_id = azurerm_servicebus_namespace.primary.id
}
resource "azurerm_servicebus_queue" "us" {
  name         = "work"
  namespace_id = azurerm_servicebus_namespace.secondary.id
}
resource "azurerm_servicebus_topic" "events" {
  name         = "events"
  namespace_id = azurerm_servicebus_namespace.primary.id
}
resource "azurerm_servicebus_subscription" "audit" {
  name               = "audit"
  topic_id           = azurerm_servicebus_topic.events.id
  max_delivery_count = 3
}
resource "azurerm_servicebus_namespace_authorization_rule" "app" {
  name         = "app"
  namespace_id = azurerm_servicebus_namespace.secondary.id
  listen       = true
}
resource "azurerm_servicebus_namespace_disaster_recovery_config" "dr" {
  name                 = "orders-dr"
  primary_namespace_id = azurerm_servicebus_namespace.primary.id
  partner_namespace_id = azurerm_servicebus_namespace.secondary.id
}
`})
	c := mustLoad(t, dir, Options{}).Config
	if len(c.Namespaces) != 2 {
		t.Fatalf("namespaces = %d, want 2: %+v", len(c.Namespaces), c)
	}
	eu, us := c.Namespaces[0], c.Namespaces[1]
	if eu.Name != "orders-eu" || us.Name != "orders-us" {
		t.Fatalf("names = %q, %q", eu.Name, us.Name)
	}
	if len(eu.Queues) != 1 || len(us.Queues) != 1 || eu.Queues[0].Name != "work" || us.Queues[0].Name != "work" {
		t.Errorf("queues: eu %v, us %v; want one 'work' each", eu.Queues, us.Queues)
	}
	if len(eu.Topics) != 1 || eu.Topics[0].Name != "events" || len(eu.Topics[0].Subscriptions) != 1 || eu.Topics[0].Subscriptions[0].Properties.MaxDeliveryCount != 3 {
		t.Errorf("eu topics = %+v", eu.Topics)
	}
	if len(us.Topics) != 0 {
		t.Errorf("us has topics %+v", us.Topics)
	}
	if !us.LocalAuthDisabled || eu.LocalAuthDisabled {
		t.Errorf("local auth: eu disabled = %v, us disabled = %v", eu.LocalAuthDisabled, us.LocalAuthDisabled)
	}
	if len(us.Policies) != 1 || us.Policies[0].Name != "app" || len(eu.Policies) != 0 {
		t.Errorf("policies: eu %v, us %v", eu.Policies, us.Policies)
	}
	if want := []server.GeoDRConfig{{Alias: "orders-dr", Primary: "orders-eu", Secondary: "orders-us"}}; !reflect.DeepEqual(c.GeoDR, want) {
		t.Errorf("geo-dr = %+v, want %+v", c.GeoDR, want)
	}
	// With a default namespace named, the rest follow.
	def, others := c.Split("orders-us")
	if def.Name != "orders-us" || len(others) != 1 || others[0].Name != "orders-eu" {
		t.Errorf("Split() = %s, %d others", def.Name, len(others))
	}
}

func TestLocksTagsAndEncryption(t *testing.T) {
	dir := writeFiles(t, map[string]string{"main.tf": `
resource "azurerm_servicebus_namespace" "ns" {
  name                = "orders"
  resource_group_name = "rg"
  location            = "westeurope"
  sku                 = "Premium"
  tags = {
    env  = "prd"
    team = "billing"
  }
  customer_managed_key {
    key_vault_key_id                  = "https://vault.example/keys/k1"
    identity_id                       = "/subscriptions/s/identities/i"
    infrastructure_encryption_enabled = true
  }
}
resource "azurerm_servicebus_queue" "q" {
  name         = "work"
  namespace_id = azurerm_servicebus_namespace.ns.id
}
resource "azurerm_servicebus_topic" "t" {
  name         = "events"
  namespace_id = azurerm_servicebus_namespace.ns.id
}
resource "azurerm_servicebus_subscription" "s" {
  name               = "audit"
  topic_id           = azurerm_servicebus_topic.t.id
  max_delivery_count = 3
}
resource "azurerm_management_lock" "ns" {
  name       = "freeze"
  scope      = azurerm_servicebus_namespace.ns.id
  lock_level = "ReadOnly"
  notes      = "frozen"
}
resource "azurerm_management_lock" "q" {
  name       = "keep"
  scope      = azurerm_servicebus_queue.q.id
  lock_level = "CanNotDelete"
}
resource "azurerm_management_lock" "s" {
  name       = "keep-sub"
  scope      = azurerm_servicebus_subscription.s.id
  lock_level = "CanNotDelete"
}
resource "azurerm_management_lock" "group" {
  name       = "elsewhere"
  scope      = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg"
  lock_level = "CanNotDelete"
}
`})
	c := mustLoad(t, dir, Options{}).Config
	if want := map[string]string{"env": "prd", "team": "billing"}; !reflect.DeepEqual(c.Tags, want) {
		t.Errorf("tags = %v, want %v", c.Tags, want)
	}
	if want := (&broker.Encryption{KeyVaultKeyIDs: []string{"https://vault.example/keys/k1"}, Identity: "/subscriptions/s/identities/i", InfrastructureEncryption: true}); !reflect.DeepEqual(c.Encryption, want) {
		t.Errorf("encryption = %+v, want %+v", c.Encryption, want)
	}
	want := []broker.Lock{
		{Name: "freeze", Level: broker.LockReadOnly, Notes: "frozen"},
		{Name: "keep", Level: broker.LockCanNotDelete, Scope: "work"},
		{Name: "keep-sub", Level: broker.LockCanNotDelete, Scope: "events/subscriptions/audit"},
	}
	if !reflect.DeepEqual(c.Locks, want) {
		t.Errorf("locks = %+v, want %+v", c.Locks, want)
	}
}

func TestJSONSyntax(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"main.tf.json": `{
  "variable": {
    "queues": {"type": "list(string)", "default": ["a", "b"]},
    "ttl": {"type": "string", "default": "PT5M"}
  },
  "locals": {"prefix": "app"},
  "resource": {
    "azurerm_servicebus_namespace": {"ns": {"name": "orders", "sku": "Standard", "resource_group_name": "rg", "location": "x", "tags": {"env": "json"}}},
    "azurerm_servicebus_queue": {
      "q": {
        "for_each": "${toset(var.queues)}",
        "name": "${local.prefix}-${each.key}",
        "namespace_id": "${azurerm_servicebus_namespace.ns.id}",
        "default_message_ttl": "${var.ttl}",
        "max_delivery_count": 4
      }
    },
    "azurerm_servicebus_topic": {"t": {"name": "events", "namespace_id": "${azurerm_servicebus_namespace.ns.id}"}},
    "azurerm_servicebus_subscription": {"s": {"name": "audit", "topic_id": "${azurerm_servicebus_topic.t.id}", "max_delivery_count": 3}},
    "azurerm_servicebus_subscription_rule": {
      "r": {
        "name": "eu", "subscription_id": "${azurerm_servicebus_subscription.s.id}", "filter_type": "CorrelationFilter",
        "correlation_filter": {"label": "order.created", "properties": {"region": "eu"}}
      }
    }
  }
}`,
		"terraform.tfvars.json": `{"ttl": "PT10M"}`,
	})
	c := mustLoad(t, dir, Options{}).Config
	if c.Name != "orders" || c.Tags["env"] != "json" {
		t.Errorf("namespace = %q, tags %v", c.Name, c.Tags)
	}
	if len(c.Queues) != 2 {
		t.Fatalf("queues = %+v", c.Queues)
	}
	a := findQueue(t, c, "app-a")
	if a.Properties.MaxDeliveryCount != 4 || a.Properties.DefaultMessageTimeToLive != 10*time.Minute {
		t.Errorf("app-a = %+v", a.Properties)
	}
	rules := c.Topics[0].Subscriptions[0].Rules
	if len(rules) != 2 || rules[1].Name != "eu" || rules[1].Filter.Correlation.Subject != "order.created" || rules[1].Filter.Correlation.Properties["region"] != "eu" {
		t.Errorf("rules = %+v", rules)
	}
}

func TestDynamicBlocks(t *testing.T) {
	dir := writeFiles(t, map[string]string{"main.tf": `
variable "regions" {
  default = ["eu", "us"]
}
resource "azurerm_servicebus_namespace" "ns" {
  name                = "orders"
  resource_group_name = "rg"
  location            = "x"
  sku                 = "Premium"
  dynamic "customer_managed_key" {
    for_each = [1]
    content {
      key_vault_key_id = "https://vault/keys/k${customer_managed_key.value}"
      identity_id      = "id"
    }
  }
}
resource "azurerm_servicebus_topic" "t" {
  name         = "events"
  namespace_id = azurerm_servicebus_namespace.ns.id
}
resource "azurerm_servicebus_subscription" "s" {
  name               = "audit"
  topic_id           = azurerm_servicebus_topic.t.id
  max_delivery_count = 3
}
resource "azurerm_servicebus_subscription_rule" "r" {
  for_each        = toset(var.regions)
  name            = each.key
  subscription_id = azurerm_servicebus_subscription.s.id
  filter_type     = "CorrelationFilter"
  dynamic "correlation_filter" {
    for_each = [each.key]
    content {
      label      = "order.created"
      properties = { region = correlation_filter.value }
    }
  }
}
`})
	c := mustLoad(t, dir, Options{}).Config
	if c.Encryption == nil || len(c.Encryption.KeyVaultKeyIDs) != 1 || c.Encryption.KeyVaultKeyIDs[0] != "https://vault/keys/k1" {
		t.Errorf("encryption = %+v", c.Encryption)
	}
	rules := c.Topics[0].Subscriptions[0].Rules
	if len(rules) != 3 {
		t.Fatalf("rules = %+v", rules)
	}
	for _, r := range rules[1:] {
		if r.Filter.Correlation == nil || r.Filter.Correlation.Properties["region"] != r.Name {
			t.Errorf("rule %s = %+v", r.Name, r.Filter)
		}
	}
}

func TestModuleCountAndForEach(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"main.tf": `
module "per_env" {
  source   = "./mod"
  for_each = toset(["dev", "prd"])
  env      = each.key
}
module "numbered" {
  source = "./mod"
  count  = 2
  env    = "n${count.index}"
}
module "adapted" {
  source   = "git::https://example.com/private.git"
  for_each = { a = "x", b = "y" }
  asb_queues = [{ name = "adapted-${each.key}" }]
}
`,
		"mod/main.tf": `
variable "env" {}
resource "azurerm_servicebus_queue" "q" {
  name = "q-${var.env}"
}
`})
	r := mustLoad(t, dir, Options{})
	var names []string
	for _, q := range r.Config.Queues {
		names = append(names, q.Name)
	}
	sort.Strings(names)
	want := []string{"adapted-a", "adapted-b", "q-dev", "q-n0", "q-n1", "q-prd"}
	if !reflect.DeepEqual(names, want) {
		t.Errorf("queues = %v, want %v; warnings %v", names, want, r.Warnings)
	}
	for _, w := range r.Warnings {
		if strings.Contains(w, "not supported") {
			t.Errorf("unexpected warning %q", w)
		}
	}
}

func TestModuleMap(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"main.tf": `
module "bus" {
  source        = "git::https://example.com/private.git"
  my_queues     = [{ name = "jobs", max_delivery_count = 4 }]
  my_topics     = [{ name = "events" }]
  my_subs       = [{ name = "audit", topic = "events" }]
}
module "other" {
  source          = "git::https://example.com/other.git"
  all_queues_here = [{ name = "other-q" }]
}
`,
		"map.json": `{"bus": {"queues": "my_queues", "topics": "my_topics", "subscriptions": "my_subs"}, "git::https://example.com/other.git": {"queues": "all_queues_here"}}`,
	})
	mm, err := LoadModuleMap(filepath.Join(dir, "map.json"))
	if err != nil {
		t.Fatal(err)
	}
	c := mustLoad(t, filepath.Join(dir, "main.tf"), Options{ModuleMap: mm}).Config
	if len(c.Queues) != 2 || c.Queues[0].Name != "jobs" || c.Queues[0].Properties.MaxDeliveryCount != 4 || c.Queues[1].Name != "other-q" {
		t.Errorf("queues = %+v", c.Queues)
	}
	if len(c.Topics) != 1 || len(c.Topics[0].Subscriptions) != 1 {
		t.Errorf("topics = %+v", c.Topics)
	}
	if _, err := LoadModuleMap(filepath.Join(dir, "main.tf")); err == nil {
		t.Error("LoadModuleMap of a non-JSON file succeeded")
	}
}

func TestUnsupportedFeaturesAreReported(t *testing.T) {
	dir := writeFiles(t, map[string]string{"main.tf": `
resource "azurerm_servicebus_namespace" "ns" {
  name                          = "orders"
  resource_group_name           = "rg"
  location                      = "westeurope"
  sku                           = "Premium"
  public_network_access_enabled = false
  network_rule_set {
    default_action = "Deny"
    ip_rules       = ["1.2.3.4"]
    network_rules {
      subnet_id = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/v/subnets/x"
    }
  }
}
resource "azurerm_servicebus_namespace" "other" {
  name                = "other"
  resource_group_name = "rg"
  location            = "westeurope"
  sku                 = "Premium"
}
resource "azurerm_servicebus_namespace_network_rule_set" "rules" {
  namespace_id = azurerm_servicebus_namespace.other.id
  ip_rules     = ["5.6.7.8"]
  network_rules {
    subnet_id = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/v/subnets/y"
  }
}
resource "azurerm_private_endpoint" "pe" {
  name                = "pe"
  resource_group_name = "rg"
  location            = "westeurope"
  subnet_id           = "x"
  private_service_connection {
    name                           = "c"
    private_connection_resource_id = azurerm_servicebus_namespace.ns.id
    subresource_names              = ["namespace"]
    is_manual_connection           = false
  }
}
resource "azurerm_private_endpoint" "storage" {
  name                = "pe2"
  resource_group_name = "rg"
  location            = "westeurope"
  subnet_id           = "x"
  private_service_connection {
    name                           = "c"
    private_connection_resource_id = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/a"
    is_manual_connection           = false
  }
}
resource "azurerm_servicebus_topic" "t" {
  name         = "events"
  namespace_id = azurerm_servicebus_namespace.ns.id
}
resource "azurerm_servicebus_subscription" "jms" {
  name                               = "jms"
  topic_id                           = azurerm_servicebus_topic.t.id
  max_delivery_count                 = 3
  client_scoped_subscription_enabled = true
}
`})
	c := mustLoad(t, dir, Options{}).Config
	got := map[string]int{}
	for _, f := range c.Unsupported {
		got[f.Feature.ID]++
	}
	want := map[string]int{"private-link": 2, "ip-filter": 2, "vnet": 2, "jms-client-scoped-subscriptions": 1}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("unsupported = %v, want %v\n%+v", got, want, c.Unsupported)
	}
	// What is left out does not stop the rest from loading.
	if len(c.Namespaces) == 0 && c.Name == "" {
		t.Errorf("config = %+v", c)
	}
}

// TestSandbox checks that a configuration loaded in sandbox mode reads
// nothing outside its directory: not a module above it, not the environment.
func TestSandbox(t *testing.T) {
	outside := writeFiles(t, map[string]string{
		"secret/main.tf": `resource "azurerm_servicebus_queue" "leak" {
  name         = "leaked"
  namespace_id = "x"
}`,
	})
	parent := t.TempDir()
	inside := filepath.Join(parent, "conf")
	if err := os.MkdirAll(filepath.Join(inside, "mod"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(inside, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	rel, err := filepath.Rel(inside, filepath.Join(outside, "secret"))
	if err != nil {
		t.Fatal(err)
	}
	write("main.tf", `module "escape" {
  source = "`+filepath.ToSlash(rel)+`"
}`)
	if _, err := Load(inside, Options{}); err != nil {
		t.Fatalf("Load() without the sandbox error: %v", err)
	}
	_, err = Load(inside, Options{Sandbox: true})
	if err == nil || !strings.Contains(err.Error(), "outside") {
		t.Errorf("Load(Sandbox) error = %v, want the module outside the directory refused", err)
	}

	// A module inside the directory is fine.
	write("main.tf", `module "ok" {
  source = "./mod"
}`)
	write("mod/main.tf", `resource "azurerm_servicebus_queue" "q" {
  name         = "inside"
  namespace_id = "x"
}`)
	r, err := Load(inside, Options{Sandbox: true})
	if err != nil {
		t.Fatalf("Load(Sandbox) of an inner module error: %v", err)
	}
	if len(r.Config.Queues) != 1 || r.Config.Queues[0].Name != "inside" {
		t.Errorf("queues = %+v, want inside", r.Config.Queues)
	}

	// TF_VAR_ variables of the process stay out of reach.
	t.Setenv("TF_VAR_secret", "from-the-environment")
	write("main.tf", `variable "secret" {
  default = "default"
}
resource "azurerm_servicebus_queue" "q" {
  name         = var.secret
  namespace_id = "x"
}`)
	os.RemoveAll(filepath.Join(inside, "mod"))
	for _, tc := range []struct {
		sandbox bool
		want    string
	}{{false, "from-the-environment"}, {true, "default"}} {
		r, err := Load(inside, Options{Sandbox: tc.sandbox})
		if err != nil {
			t.Fatalf("Load(sandbox=%v) error: %v", tc.sandbox, err)
		}
		if got := r.Config.Queues[0].Name; got != tc.want {
			t.Errorf("sandbox=%v: queue name = %q, want %q", tc.sandbox, got, tc.want)
		}
	}
}
