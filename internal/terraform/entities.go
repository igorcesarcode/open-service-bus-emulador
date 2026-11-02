package terraform

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/ext/dynblock"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/convert"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/config"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/unsupported"
)

// property sets an entity property from a Terraform argument.
type property func(p *broker.Properties, v cty.Value) error

func durationProperty(field func(*broker.Properties) *time.Duration) property {
	return func(p *broker.Properties, v cty.Value) error {
		s, err := asString(v)
		if err != nil {
			return err
		}
		d, err := duration.Parse(s)
		if err != nil {
			return err
		}
		*field(p) = d
		return nil
	}
}

func boolProperty(field func(*broker.Properties) *bool) property {
	return func(p *broker.Properties, v cty.Value) error {
		v, err := convert.Convert(v, cty.Bool)
		if err != nil {
			return err
		}
		*field(p) = v.True()
		return nil
	}
}

func intProperty[T int32 | int64](field func(*broker.Properties) *T) property {
	return func(p *broker.Properties, v cty.Value) error {
		v, err := convert.Convert(v, cty.Number)
		if err != nil {
			return err
		}
		n, acc := v.AsBigFloat().Int64()
		if acc != big.Exact || int64(T(n)) != n {
			return fmt.Errorf("%s is not a valid integer", v.AsBigFloat())
		}
		*field(p) = T(n)
		return nil
	}
}

func stringProperty(field func(*broker.Properties) *string) property {
	return func(p *broker.Properties, v cty.Value) error {
		s, err := asString(v)
		*field(p) = s
		return err
	}
}

// properties maps the arguments of azurerm_servicebus_queue, _topic and
// _subscription, including the names of provider versions before 4.0, to
// entity properties.
var properties = map[string]property{
	"lock_duration":                             durationProperty(func(p *broker.Properties) *time.Duration { return &p.LockDuration }),
	"default_message_ttl":                       durationProperty(func(p *broker.Properties) *time.Duration { return &p.DefaultMessageTimeToLive }),
	"auto_delete_on_idle":                       durationProperty(func(p *broker.Properties) *time.Duration { return &p.AutoDeleteOnIdle }),
	"duplicate_detection_history_time_window":   durationProperty(func(p *broker.Properties) *time.Duration { return &p.DuplicateDetectionHistoryTimeWindow }),
	"max_delivery_count":                        intProperty(func(p *broker.Properties) *int32 { return &p.MaxDeliveryCount }),
	"max_size_in_megabytes":                     intProperty(func(p *broker.Properties) *int64 { return &p.MaxSizeInMegabytes }),
	"max_message_size_in_kilobytes":             intProperty(func(p *broker.Properties) *int64 { return &p.MaxMessageSizeInKilobytes }),
	"requires_duplicate_detection":              boolProperty(func(p *broker.Properties) *bool { return &p.RequiresDuplicateDetection }),
	"requires_session":                          boolProperty(func(p *broker.Properties) *bool { return &p.RequiresSession }),
	"dead_lettering_on_message_expiration":      boolProperty(func(p *broker.Properties) *bool { return &p.DeadLetteringOnMessageExpiration }),
	"dead_lettering_on_filter_evaluation_error": boolProperty(func(p *broker.Properties) *bool { return &p.DeadLetteringOnFilterEvaluationExceptions }),
	"batched_operations_enabled":                boolProperty(func(p *broker.Properties) *bool { return &p.EnableBatchedOperations }),
	"enable_batched_operations":                 boolProperty(func(p *broker.Properties) *bool { return &p.EnableBatchedOperations }),
	"partitioning_enabled":                      boolProperty(func(p *broker.Properties) *bool { return &p.EnablePartitioning }),
	"enable_partitioning":                       boolProperty(func(p *broker.Properties) *bool { return &p.EnablePartitioning }),
	"express_enabled":                           boolProperty(func(p *broker.Properties) *bool { return &p.EnableExpress }),
	"enable_express":                            boolProperty(func(p *broker.Properties) *bool { return &p.EnableExpress }),
	"support_ordering":                          boolProperty(func(p *broker.Properties) *bool { return &p.SupportOrdering }),
	"forward_to":                                stringProperty(func(p *broker.Properties) *string { return &p.ForwardTo }),
	"forward_dead_lettered_messages_to":         stringProperty(func(p *broker.Properties) *string { return &p.ForwardDeadLetteredMessagesTo }),
	"user_metadata":                             stringProperty(func(p *broker.Properties) *string { return &p.UserMetadata }),
	"status": func(p *broker.Properties, v cty.Value) error {
		s, err := asString(v)
		if err != nil {
			return err
		}
		for _, st := range []broker.Status{broker.StatusActive, broker.StatusDisabled, broker.StatusSendDisabled, broker.StatusReceiveDisabled} {
			if strings.EqualFold(s, string(st)) {
				p.Status = st
				return nil
			}
		}
		return fmt.Errorf("status %q: want Active, Disabled, SendDisabled or ReceiveDisabled", s)
	},
}

// ignored lists arguments that identify the entity or only matter in
// Azure.
var ignored = map[string]bool{
	"name": true, "namespace_id": true, "namespace_name": true, "resource_group_name": true,
	"topic_id": true, "topic_name": true, "subscription_id": true, "subscription_name": true,
	"client_scoped_subscription_enabled": true,
}

func asString(v cty.Value) (string, error) {
	v, err := convert.Convert(v, cty.String)
	if err != nil {
		return "", err
	}
	if v.IsNull() {
		return "", errors.New("value is null")
	}
	return v.AsString(), nil
}

// nsState holds what one namespace declares.
type nsState struct {
	name     string
	queues   []server.Queue
	topics   []*server.Topic
	topicIdx map[string]*server.Topic
	// subs indexes subscriptions by "topic/name", case-insensitively.
	subs map[string]*sub

	partitions        int
	policies          []auth.Policy
	assignments       []server.Assignment
	localAuthDisabled bool
	tags              map[string]string
	locks             []broker.Lock
	encryption        *broker.Encryption
}

// builder collects entities, keeping their declaration order. The fields of
// the namespace being read are promoted from the embedded nsState, which
// enter switches.
type builder struct {
	opts     Options
	warnings []string
	errs     []error
	// unsupported are the declared features the emulator leaves out.
	unsupported []unsupported.Finding

	*nsState
	states map[string]*nsState
	order  []string
	geoDR  []server.GeoDRConfig
}

type sub struct {
	// rules holds rules declared by azurerm_servicebus_subscription_rule,
	// or the filter an adapted module gave the subscription.
	rules []broker.Rule
	// filtered is set when an adapted module gave the subscription its
	// filter, which replaces $Default.
	filtered bool
}

func newBuilder(opts Options) *builder {
	b := &builder{opts: opts, states: map[string]*nsState{}}
	b.enter("")
	return b
}

// enter makes the namespace of that name the one entities are added to.
// The empty name is the default namespace, for resources that say none.
func (b *builder) enter(name string) {
	key := strings.ToLower(name)
	st, ok := b.states[key]
	if !ok {
		st = &nsState{name: name, topicIdx: map[string]*server.Topic{}, subs: map[string]*sub{}}
		b.states[key] = st
		b.order = append(b.order, key)
	}
	b.nsState = st
}

func (b *builder) leaveOut(f unsupported.Feature, where string) {
	b.unsupported = append(b.unsupported, unsupported.Finding{Feature: f, Where: where})
}

func (b *builder) warnf(format string, args ...any) {
	b.warnings = append(b.warnings, fmt.Sprintf(format, args...))
}

func (b *builder) errorf(format string, args ...any) {
	b.errs = append(b.errs, fmt.Errorf(format, args...))
}

func (b *builder) addQueue(address, name string, p broker.Properties) {
	for _, q := range b.queues {
		if strings.EqualFold(q.Name, name) {
			b.errorf("%s: queue %q is declared twice", address, name)
			return
		}
	}
	b.queues = append(b.queues, server.Queue{Name: name, Properties: p})
}

func (b *builder) addTopic(address, name string, p broker.Properties) {
	if _, ok := b.topicIdx[strings.ToLower(name)]; ok {
		b.errorf("%s: topic %q is declared twice", address, name)
		return
	}
	t := &server.Topic{Name: name, Properties: p}
	b.topics = append(b.topics, t)
	b.topicIdx[strings.ToLower(name)] = t
}

// topic returns the topic named name, declaring it with the default
// properties when no resource does, as for a topic managed elsewhere.
func (b *builder) topic(address, name string) *server.Topic {
	if t, ok := b.topicIdx[strings.ToLower(name)]; ok {
		return t
	}
	b.warnf("%s: topic %q is not declared; creating it with the default properties", address, name)
	b.addTopic(address, name, broker.DefaultProperties(broker.KindTopic))
	return b.topicIdx[strings.ToLower(name)]
}

func (b *builder) addSubscription(address, topic, name string, p broker.Properties) *sub {
	key := strings.ToLower(topic + "/" + name)
	if _, ok := b.subs[key]; ok {
		b.errorf("%s: subscription %q of topic %q is declared twice", address, name, topic)
		return nil
	}
	t := b.topic(address, topic)
	t.Subscriptions = append(t.Subscriptions, server.Subscription{Name: name, Properties: p})
	s := &sub{}
	b.subs[key] = s
	return s
}

// config resolves the rules and returns the entities of every namespace.
func (b *builder) config() (*config.Config, error) {
	var all []*config.Config
	for _, key := range b.order {
		b.enter(b.states[key].name)
		all = append(all, b.namespaceConfig())
	}
	if err := errors.Join(b.errs...); err != nil {
		return nil, err
	}
	// A namespace nothing was declared in, such as the default one when
	// every resource names its namespace, is left out.
	var used []*config.Config
	for _, c := range all {
		if c.Name != "" || len(c.Queues)+len(c.Topics)+len(c.Policies)+len(c.Assignments)+len(c.Tags)+len(c.Locks) > 0 || c.LocalAuthDisabled || c.Partitions > 0 || c.Encryption != nil {
			used = append(used, c)
		}
	}
	var out *config.Config
	switch len(used) {
	case 0:
		out = &config.Config{}
	case 1:
		out = used[0]
	default:
		out = &config.Config{Namespaces: used}
	}
	out.GeoDR = b.geoDR
	out.Unsupported = b.unsupported
	return out, nil
}

// namespaceConfig resolves the rules of the current namespace.
func (b *builder) namespaceConfig() *config.Config {
	for _, t := range b.topics {
		for i := range t.Subscriptions {
			s := b.subs[strings.ToLower(t.Name+"/"+t.Subscriptions[i].Name)]
			if s.filtered {
				t.Subscriptions[i].Rules = s.rules
				continue
			}
			if len(s.rules) == 0 {
				continue // $Default alone, as created by Azure.
			}
			rules := s.rules
			if !b.opts.DropDefaultRule && !hasRule(rules, broker.DefaultRuleName) {
				// Azure creates $Default with every subscription and
				// keeps it when other rules are added.
				rules = append([]broker.Rule{broker.TrueRule(broker.DefaultRuleName)}, rules...)
				b.warnf("subscription %s/%s keeps the $Default rule (1=1) next to its declared rules, as in Azure, so it receives every message; pass -tf-drop-default-rule to remove it", t.Name, t.Subscriptions[i].Name)
			}
			t.Subscriptions[i].Rules = rules
		}
	}
	c := &config.Config{Name: b.name, Queues: b.queues, Policies: b.policies, Assignments: b.assignments, LocalAuthDisabled: b.localAuthDisabled, Partitions: b.partitions,
		Tags: b.tags, Locks: b.locks, Encryption: b.encryption}
	for _, t := range b.topics {
		c.Topics = append(c.Topics, *t)
	}
	return c
}

func hasRule(rules []broker.Rule, name string) bool {
	for _, r := range rules {
		if strings.EqualFold(r.Name, name) {
			return true
		}
	}
	return false
}

// entities adds the Service Bus resources of m.
func (m *module) entities(b *builder) {
	byType := map[string][]*resource{}
	for _, r := range m.resources {
		byType[r.typ] = append(byType[r.typ], r)
	}
	// Parents first, so that subscriptions find their topics.
	for _, typ := range []string{"azurerm_servicebus_namespace", "azurerm_servicebus_queue", "azurerm_servicebus_topic", "azurerm_servicebus_subscription", "azurerm_servicebus_subscription_rule",
		"azurerm_servicebus_namespace_authorization_rule", "azurerm_servicebus_queue_authorization_rule", "azurerm_servicebus_topic_authorization_rule",
		"azurerm_role_assignment", "azurerm_servicebus_namespace_disaster_recovery_config", "azurerm_management_lock",
		"azurerm_servicebus_namespace_network_rule_set", "azurerm_private_endpoint"} {
		for _, r := range byType[typ] {
			address := m.prefix() + r.address()
			if !r.expanded {
				if r.expansion.HasErrors() {
					b.errorf("%s: for_each or count: %v", address, r.expansion)
				} else {
					b.errorf("%s: for_each or count is not known; set the variables it depends on with -tf-var", address)
				}
				continue
			}
			for _, in := range r.instances {
				m.instanceEntity(b, typ, in)
			}
		}
	}
}

func (m *module) prefix() string {
	if m.path == "" {
		return ""
	}
	return m.path + "."
}

func detail(diags hcl.Diagnostics) string {
	if diags.HasErrors() {
		return ": " + diags.Error()
	}
	return ""
}

// value returns a known argument of in, or reports why it is not known.
func (m *module) value(b *builder, in *instance, name string) (cty.Value, bool) {
	v, ok := in.attrs[name]
	if !ok {
		return cty.NilVal, false
	}
	if !v.IsWhollyKnown() {
		b.errorf("%s%s: %s is not known; set the variables it depends on with -tf-var%s", m.prefix(), in.address, name, detail(in.diags[name]))
		return cty.NilVal, false
	}
	return v, !v.IsNull()
}

func (m *module) stringArg(b *builder, in *instance, name string) (string, bool) {
	v, ok := m.value(b, in, name)
	if !ok {
		return "", false
	}
	s, err := asString(v)
	if err != nil {
		b.errorf("%s%s: %s: %v", m.prefix(), in.address, name, err)
		return "", false
	}
	return s, true
}

// props applies the arguments of in to the defaults of kind.
func (m *module) props(b *builder, in *instance, kind broker.EntityKind) (broker.Properties, bool) {
	p := broker.DefaultProperties(kind)
	ok := true
	for name := range in.attrs {
		set, known := properties[name]
		if !known {
			if !ignored[name] {
				b.warnf("%s%s: argument %s is ignored", m.prefix(), in.address, name)
			}
			continue
		}
		v, present := m.value(b, in, name)
		if !present {
			ok = ok && in.attrs[name].IsWhollyKnown()
			continue
		}
		if err := set(&p, v); err != nil {
			b.errorf("%s%s: %s: %v", m.prefix(), in.address, name, err)
			ok = false
		}
	}
	if err := p.Validate(kind); err != nil {
		b.errorf("%s%s: %v", m.prefix(), in.address, err)
		ok = false
	}
	return p, ok
}

// idSegment returns the segment after the last kind in an Azure resource
// id, such as the topic name of ".../topics/orders/subscriptions/audit".
// The last one counts because ids start with /subscriptions/<guid>.
func idSegment(id, kind string) (string, bool) {
	parts := strings.Split(id, "/")
	for i := len(parts) - 2; i >= 0; i-- {
		if strings.EqualFold(parts[i], kind) {
			return parts[i+1], true
		}
	}
	return "", false
}

// parentName finds the name of a parent entity from an id argument, or
// from the name argument of older providers.
func (m *module) parentName(b *builder, in *instance, idArg, nameArg, kind string) (string, bool) {
	if _, ok := in.attrs[idArg]; ok {
		id, ok := m.stringArg(b, in, idArg)
		if !ok {
			return "", false
		}
		name, ok := idSegment(id, kind)
		if !ok {
			b.errorf("%s%s: %s %q has no /%s/ segment", m.prefix(), in.address, idArg, id, kind)
		}
		return name, ok
	}
	if _, ok := in.attrs[nameArg]; ok {
		return m.stringArg(b, in, nameArg)
	}
	b.errorf("%s%s: %s is required", m.prefix(), in.address, idArg)
	return "", false
}

func (m *module) instanceEntity(b *builder, typ string, in *instance) {
	address := m.prefix() + in.address
	if typ == "azurerm_servicebus_namespace_disaster_recovery_config" {
		m.geoDRConfig(b, in, address)
		return
	}
	b.enter(m.namespaceOf(in, typ))
	if typ == "azurerm_role_assignment" {
		m.roleAssignment(b, in, address)
		return
	}
	if typ == "azurerm_management_lock" {
		m.managementLock(b, in, address)
		return
	}
	if typ == "azurerm_servicebus_namespace_network_rule_set" {
		m.networkRules(b, in, address)
		return
	}
	if typ == "azurerm_private_endpoint" {
		m.privateEndpoint(b, in, address)
		return
	}
	name, ok := m.stringArg(b, in, "name")
	if !ok {
		if _, set := in.attrs["name"]; !set {
			b.errorf("%s: name is required", address)
		}
		return
	}
	switch typ {
	case "azurerm_servicebus_namespace":
		if v, ok := m.value(b, in, "premium_messaging_partitions"); ok {
			if n, err := convert.Convert(v, cty.Number); err == nil {
				if i, acc := n.AsBigFloat().Int64(); acc == big.Exact && i > 0 {
					b.partitions = int(i)
				}
			}
		}
		if v, ok := m.value(b, in, "local_auth_enabled"); ok {
			if enabled, err := convert.Convert(v, cty.Bool); err == nil && enabled.False() {
				b.localAuthDisabled = true
			}
		}
		if v, ok := m.value(b, in, "tags"); ok {
			if tags, err := goMap(v); err != nil {
				b.errorf("%s: tags: %v", address, err)
			} else {
				b.tags = map[string]string{}
				for k, val := range tags {
					b.tags[k] = fmt.Sprint(val)
				}
			}
		}
		m.customerManagedKey(b, in, address)
		if v, ok := in.attrs["public_network_access_enabled"]; ok && v.IsKnown() && !v.IsNull() && v.Type() == cty.Bool && v.False() {
			b.leaveOut(unsupported.PrivateLink, address+" public_network_access_enabled = false")
		}
		if hasBlock(m, in, "network_rule_set") {
			m.networkRules(b, in, address)
		}
	case "azurerm_servicebus_namespace_authorization_rule":
		m.policy(b, in, address, name, "")
	case "azurerm_servicebus_queue_authorization_rule":
		if q, ok := m.parentName(b, in, "queue_id", "queue_name", "queues"); ok {
			m.policy(b, in, address, name, q)
		}
	case "azurerm_servicebus_topic_authorization_rule":
		if t, ok := m.parentName(b, in, "topic_id", "topic_name", "topics"); ok {
			m.policy(b, in, address, name, t)
		}
	case "azurerm_servicebus_queue":
		if p, ok := m.props(b, in, broker.KindQueue); ok {
			b.addQueue(address, name, p)
		}
	case "azurerm_servicebus_topic":
		if p, ok := m.props(b, in, broker.KindTopic); ok {
			b.addTopic(address, name, p)
		}
	case "azurerm_servicebus_subscription":
		topic, ok := m.parentName(b, in, "topic_id", "topic_name", "topics")
		if !ok {
			return
		}
		if v, ok := in.attrs["client_scoped_subscription_enabled"]; ok && v.IsKnown() && !v.IsNull() && v.Type() == cty.Bool && v.True() {
			b.leaveOut(unsupported.JMSClientScoped, address+" client_scoped_subscription_enabled = true")
		}
		if hasBlock(m, in, "client_scoped_subscription") {
			b.leaveOut(unsupported.JMSClientScoped, address+" client_scoped_subscription")
		}
		if p, ok := m.props(b, in, broker.KindSubscription); ok {
			b.addSubscription(address, topic, name, p)
		}
	case "azurerm_servicebus_subscription_rule":
		m.ruleEntity(b, in, address, name)
	}
}

// policy declares a shared access policy from the listen, send and manage
// arguments of an authorization rule. Terraform computes its keys; the
// emulator derives them from the scope and name (auth.DeclaredKey).
func (m *module) policy(b *builder, in *instance, address, name, scope string) {
	var rights auth.Right
	for arg, r := range map[string]auth.Right{"listen": auth.Listen, "send": auth.Send, "manage": auth.Manage} {
		if v, ok := m.value(b, in, arg); ok {
			if on, err := convert.Convert(v, cty.Bool); err == nil && on.True() {
				rights |= r
			}
		}
	}
	if rights == 0 {
		b.errorf("%s: set at least one of listen, send and manage", address)
		return
	}
	b.policies = append(b.policies, config.DeclaredPolicy(scope, name, rights.Normalize(), "", ""))
}

// roleAssignment declares the role assignments of azurerm_role_assignment
// resources whose scope is a Service Bus namespace, queue or topic and whose
// role is a Service Bus data role. Others are not about this emulator.
// namespaceOf names the namespace a resource is in: the namespace itself by
// its name, others by the namespace in the id they point to. Resources that
// point to none are in the default namespace.
func (m *module) namespaceOf(in *instance, typ string) string {
	if typ == "azurerm_servicebus_namespace" {
		name, _ := in.knownString("name")
		return name
	}
	for _, arg := range []string{"namespace_id", "queue_id", "topic_id", "subscription_id", "scope"} {
		if id, ok := in.knownString(arg); ok {
			if name, ok := idSegment(id, "namespaces"); ok {
				return name
			}
		}
	}
	return ""
}

// geoDRConfig declares a Geo-DR alias from an
// azurerm_servicebus_namespace_disaster_recovery_config.
func (m *module) geoDRConfig(b *builder, in *instance, address string) {
	alias, ok := m.stringArg(b, in, "name")
	if !ok {
		b.errorf("%s: name is required", address)
		return
	}
	var ends [2]string
	for i, arg := range []string{"primary_namespace_id", "partner_namespace_id"} {
		id, ok := m.stringArg(b, in, arg)
		if !ok {
			b.errorf("%s: %s is required", address, arg)
			return
		}
		name, ok := idSegment(id, "namespaces")
		if !ok {
			b.errorf("%s: %s %q has no /namespaces/ segment", address, arg, id)
			return
		}
		ends[i] = name
	}
	b.geoDR = append(b.geoDR, server.GeoDRConfig{Alias: alias, Primary: ends[0], Secondary: ends[1]})
}

// hasBlock reports whether the resource of in has a block of type typ.
func hasBlock(m *module, in *instance, typ string) bool {
	blocks, _ := m.blocks(in, typ)
	return len(blocks) > 0
}

// networkRules reports the IP rules and virtual network rules of a namespace
// network rule set, in the resource or in the block of the namespace.
func (m *module) networkRules(b *builder, in *instance, address string) {
	typ := "network_rule_set"
	attrs := in.attrs
	if in.address != "" && !strings.HasPrefix(in.address, "azurerm_servicebus_namespace_network_rule_set") {
		// A block of the namespace.
		blocks, _ := m.blocks(in, typ, "ip_rules", "public_network_access_enabled")
		attrs = map[string]cty.Value{}
		if len(blocks) > 0 {
			for name, attr := range blocks[0] {
				if v, diags := attr.Expr.Value(in.ctx); !diags.HasErrors() {
					attrs[name] = v
				}
			}
		}
		if len(nestedBlocks(m, in, typ, "network_rules")) > 0 {
			b.leaveOut(unsupported.VNet, address+" network_rule_set.network_rules")
		}
	} else {
		if v, ok := attrs["public_network_access_enabled"]; ok && v.IsKnown() && !v.IsNull() && v.Type() == cty.Bool && v.False() {
			b.leaveOut(unsupported.PrivateLink, address+" public_network_access_enabled = false")
		}
		if hasBlock(m, in, "network_rules") {
			b.leaveOut(unsupported.VNet, address+" network_rules")
		}
	}
	if v, ok := attrs["ip_rules"]; ok && v.IsKnown() && !v.IsNull() && v.LengthInt() > 0 {
		b.leaveOut(unsupported.IPFilter, address+" ip_rules")
	}
}

// nestedBlocks returns the blocks of type inner inside the block outer.
func nestedBlocks(m *module, in *instance, outer, inner string) []*hcl.Block {
	body := dynblock.Expand(m.resourceOf(in).block.Body, in.ctx)
	content, _, _ := body.PartialContent(&hcl.BodySchema{Blocks: []hcl.BlockHeaderSchema{{Type: outer}}})
	var out []*hcl.Block
	for _, blk := range content.Blocks {
		c, _, _ := blk.Body.PartialContent(&hcl.BodySchema{Blocks: []hcl.BlockHeaderSchema{{Type: inner}}})
		out = append(out, c.Blocks...)
	}
	return out
}

// privateEndpoint reports an azurerm_private_endpoint that connects to a
// Service Bus namespace.
func (m *module) privateEndpoint(b *builder, in *instance, address string) {
	blocks, _ := m.blocks(in, "private_service_connection", "private_connection_resource_id", "subresource_names")
	for _, attrs := range blocks {
		if attr, ok := attrs["private_connection_resource_id"]; ok {
			if v, diags := attr.Expr.Value(in.ctx); !diags.HasErrors() && v.IsKnown() && !v.IsNull() && v.Type() == cty.String &&
				strings.Contains(strings.ToLower(v.AsString()), "microsoft.servicebus") {
				b.leaveOut(unsupported.PrivateLink, address)
			}
		}
	}
}

// managementLock declares a lock from an azurerm_management_lock whose
// scope is a Service Bus namespace, queue, topic or subscription. A lock on
// anything else (a resource group, say) is not about this emulator.
func (m *module) managementLock(b *builder, in *instance, address string) {
	scope, ok := m.stringArg(b, in, "scope")
	if !ok || !strings.Contains(strings.ToLower(scope), "microsoft.servicebus/namespaces/") {
		return
	}
	name, _ := m.stringArg(b, in, "name")
	level, ok := m.stringArg(b, in, "lock_level")
	if !ok {
		b.errorf("%s: lock_level is required", address)
		return
	}
	l := broker.Lock{Name: name}
	switch strings.ToLower(level) {
	case "cannotdelete":
		l.Level = broker.LockCanNotDelete
	case "readonly":
		l.Level = broker.LockReadOnly
	default:
		b.errorf("%s: lock_level %q: want CanNotDelete or ReadOnly", address, level)
		return
	}
	l.Notes, _ = m.stringArg(b, in, "notes")
	if q, ok := idSegment(scope, "queues"); ok {
		l.Scope = broker.NormalizePath(q)
	} else if t, ok := idSegment(scope, "topics"); ok {
		l.Scope = broker.NormalizePath(t)
		if s, ok := idSegment(scope, "subscriptions"); ok {
			l.Scope = broker.SubscriptionPath(t, s)
		}
	}
	b.locks = append(b.locks, l)
}

// blocks returns the blocks of type typ in the resource of in, with its
// dynamic blocks expanded for the instance.
// Each block comes with the arguments in names: the arguments of a
// dynamic block's content only see its iterator when read through a schema.
func (m *module) blocks(in *instance, typ string, names ...string) ([]hcl.Attributes, hcl.Diagnostics) {
	body := dynblock.Expand(m.resourceOf(in).block.Body, in.ctx)
	content, _, diags := body.PartialContent(&hcl.BodySchema{Blocks: []hcl.BlockHeaderSchema{{Type: typ}}})
	schema := &hcl.BodySchema{}
	for _, n := range names {
		schema.Attributes = append(schema.Attributes, hcl.AttributeSchema{Name: n})
	}
	var out []hcl.Attributes
	for _, blk := range content.Blocks {
		c, _, d := blk.Body.PartialContent(schema)
		diags = append(diags, d...)
		out = append(out, c.Attributes)
	}
	return out, diags
}

// customerManagedKey reads the encryption of a namespace from its
// customer_managed_key block.
func (m *module) customerManagedKey(b *builder, in *instance, address string) {
	blocks, diags := m.blocks(in, "customer_managed_key", "key_vault_key_id", "identity_id", "infrastructure_encryption_enabled")
	if diags.HasErrors() {
		b.errorf("%s: customer_managed_key: %v", address, diags)
		return
	}
	if len(blocks) == 0 {
		return
	}
	attrs := blocks[0]
	enc := &broker.Encryption{}
	for name, attr := range attrs {
		v, diags := attr.Expr.Value(in.ctx)
		if diags.HasErrors() || !v.IsWhollyKnown() || v.IsNull() {
			b.errorf("%s: customer_managed_key.%s is not known%s", address, name, detail(diags))
			return
		}
		switch name {
		case "key_vault_key_id":
			enc.KeyVaultKeyIDs = []string{v.AsString()}
		case "identity_id":
			enc.Identity = v.AsString()
		case "infrastructure_encryption_enabled":
			if on, err := convert.Convert(v, cty.Bool); err == nil {
				enc.InfrastructureEncryption = on.True()
			}
		}
	}
	b.encryption = enc
}

func (m *module) roleAssignment(b *builder, in *instance, address string) {
	scope, ok := m.stringArg(b, in, "scope")
	if !ok || !strings.Contains(strings.ToLower(scope), "microsoft.servicebus/namespaces/") {
		return
	}
	roleName, _ := m.stringArg(b, in, "role_definition_name")
	role, err := auth.ParseRole(roleName)
	if err != nil {
		return
	}
	principal, ok := m.stringArg(b, in, "principal_id")
	if !ok {
		b.errorf("%s: principal_id is required", address)
		return
	}
	entity := ""
	if q, ok := idSegment(scope, "queues"); ok {
		entity = q
	} else if t, ok := idSegment(scope, "topics"); ok {
		entity = t
	}
	b.assignments = append(b.assignments, server.Assignment{Principal: principal, Role: role, Scope: strings.ToLower(entity)})
}

func (m *module) ruleEntity(b *builder, in *instance, address, name string) {
	var topic, subName string
	if _, ok := in.attrs["subscription_id"]; ok {
		id, ok := m.stringArg(b, in, "subscription_id")
		if !ok {
			return
		}
		t, ok1 := idSegment(id, "topics")
		s, ok2 := idSegment(id, "subscriptions")
		if !ok1 || !ok2 {
			b.errorf("%s: subscription_id %q has no /topics/ and /subscriptions/ segments", address, id)
			return
		}
		topic, subName = t, s
	} else {
		var ok bool
		if topic, ok = m.stringArg(b, in, "topic_name"); !ok {
			b.errorf("%s: subscription_id is required", address)
			return
		}
		if subName, ok = m.stringArg(b, in, "subscription_name"); !ok {
			b.errorf("%s: subscription_id is required", address)
			return
		}
	}
	s, ok := b.subs[strings.ToLower(topic+"/"+subName)]
	if !ok {
		b.errorf("%s: subscription %q of topic %q is not declared", address, subName, topic)
		return
	}
	if hasRule(s.rules, name) {
		b.errorf("%s: rule %q is declared twice", address, name)
		return
	}

	r := broker.Rule{Name: name}
	kind, _ := m.stringArg(b, in, "filter_type")
	switch strings.ToLower(kind) {
	case "sqlfilter":
		expr, ok := m.stringArg(b, in, "sql_filter")
		if !ok {
			b.errorf("%s: filter_type SqlFilter needs sql_filter", address)
			return
		}
		r.Filter = broker.RuleFilter{Kind: broker.FilterSQL, SQL: expr}
	case "correlationfilter":
		c, ok := m.correlation(b, in, address)
		if !ok {
			return
		}
		r.Filter = broker.RuleFilter{Kind: broker.FilterCorrelation, Correlation: c}
	default:
		b.errorf("%s: filter_type %q: want SqlFilter or CorrelationFilter", address, kind)
		return
	}
	if _, ok := in.attrs["action"]; ok {
		r.Action, _ = m.stringArg(b, in, "action")
	}
	if err := config.ValidateRule(r); err != nil {
		b.errorf("%s: %v", address, err)
		return
	}
	s.rules = append(s.rules, r)
}

// correlation evaluates the correlation_filter block of a rule.
func (m *module) correlation(b *builder, in *instance, address string) (*filter.CorrelationFilter, bool) {
	blocks, diags := m.blocks(in, "correlation_filter", "content_type", "correlation_id", "label", "message_id",
		"properties", "reply_to", "reply_to_session_id", "session_id", "to")
	if diags.HasErrors() {
		b.errorf("%s: correlation_filter: %v", address, diags)
		return nil, false
	}
	if len(blocks) != 1 {
		b.errorf("%s: filter_type CorrelationFilter needs one correlation_filter block", address)
		return nil, false
	}
	attrs := blocks[0]
	c := &filter.CorrelationFilter{}
	fields := map[string]*string{
		"content_type":        &c.ContentType,
		"correlation_id":      &c.CorrelationID,
		"label":               &c.Subject,
		"message_id":          &c.MessageID,
		"reply_to":            &c.ReplyTo,
		"reply_to_session_id": &c.ReplyToSessionID,
		"session_id":          &c.SessionID,
		"to":                  &c.To,
	}
	for name, attr := range attrs {
		v, diags := attr.Expr.Value(in.ctx)
		if diags.HasErrors() || !v.IsWhollyKnown() {
			b.errorf("%s: correlation_filter.%s is not known%s", address, name, detail(diags))
			return nil, false
		}
		if v.IsNull() {
			continue
		}
		if name == "properties" {
			props, err := goMap(v)
			if err != nil {
				b.errorf("%s: correlation_filter.properties: %v", address, err)
				return nil, false
			}
			c.Properties = props
			continue
		}
		dst, ok := fields[name]
		if !ok {
			b.warnf("%s: correlation_filter.%s is ignored", address, name)
			continue
		}
		s, err := asString(v)
		if err != nil {
			b.errorf("%s: correlation_filter.%s: %v", address, name, err)
			return nil, false
		}
		*dst = s
	}
	return c, true
}

func (m *module) resourceOf(in *instance) *resource {
	for _, r := range m.resources {
		for _, i := range r.instances {
			if i == in {
				return r
			}
		}
	}
	panic("terraform: instance without resource")
}

// goMap converts a map of primitive values for a correlation filter.
func goMap(v cty.Value) (map[string]any, error) {
	if !v.Type().IsMapType() && !v.Type().IsObjectType() {
		return nil, fmt.Errorf("want a map, got %s", v.Type().FriendlyName())
	}
	out := map[string]any{}
	for it := v.ElementIterator(); it.Next(); {
		k, e := it.Element()
		switch {
		case e.IsNull():
			continue
		case e.Type() == cty.String:
			out[k.AsString()] = e.AsString()
		case e.Type() == cty.Bool:
			out[k.AsString()] = e.True()
		case e.Type() == cty.Number:
			f := e.AsBigFloat()
			if n, acc := f.Int64(); acc == big.Exact {
				out[k.AsString()] = n
			} else {
				x, _ := f.Float64()
				out[k.AsString()] = x
			}
		default:
			return nil, fmt.Errorf("property %s: want a string, number or bool", k.AsString())
		}
	}
	return out, nil
}
