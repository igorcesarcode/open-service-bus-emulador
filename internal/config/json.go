// Package config loads entity declarations for the emulator.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/unsupported"
)

// Config holds the entities to create at startup.
type Config struct {
	// Name is the name of the namespace the entities are for. Empty means
	// the emulator's default namespace.
	Name string
	// Namespaces holds the namespaces of a document that declares several.
	// Then the other fields of the Config hold nothing of their own:
	// Split picks the default namespace out.
	Namespaces []*Config
	// GeoDR pairs namespaces under aliases.
	GeoDR []server.GeoDRConfig
	// GeoReplication replicates this namespace's data to a second region.
	GeoReplication *server.GeoReplication
	Queues         []server.Queue
	Topics         []server.Topic
	// Policies are shared access policies. A policy declared without keys
	// gets the keys auth.DeclaredKey derives.
	Policies []auth.Policy
	// Assignments are role assignments of Entra ID principals.
	Assignments []server.Assignment
	// Partitions is how many partitions partitioned entities have, from the
	// premium_messaging_partitions of a Terraform namespace. Zero is unset.
	Partitions int
	// LocalAuthDisabled turns off SAS, as local_auth_enabled = false does.
	LocalAuthDisabled bool
	// Tags, Locks and Encryption are the namespace's Azure resource
	// settings.
	Tags       map[string]string
	Locks      []broker.Lock
	Encryption *broker.Encryption
	// Unsupported lists the declared features the emulator leaves out
	// (network rules, private endpoints, ...). They are ignored, and main
	// logs each one.
	Unsupported []unsupported.Finding
}

// LoadJSONFile reads a Config.json file in the format of the official
// emulator. See LoadJSON.
func LoadJSONFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c, err := LoadJSON(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

// LoadJSON parses a Config.json document in the format of the official
// emulator, including its // and /* */ comments. Unlike the official
// emulator, every entity property is accepted, without range limits;
// properties that are not set get the Service Bus defaults. A document
// with one namespace declares the default namespace, and one with several
// declares one namespace each.
func LoadJSON(data []byte) (*Config, error) {
	dec := json.NewDecoder(bytes.NewReader(stripComments(data)))
	dec.DisallowUnknownFields()
	var doc jsonDocument
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("config: unexpected data after the document")
	}

	var all []*Config
	for _, ns := range doc.UserConfig.Namespaces {
		c := &Config{Name: ns.Name}
		if ns.LocalAuthEnabled != nil && !*ns.LocalAuthEnabled {
			c.LocalAuthDisabled = true
		}
		for _, r := range ns.AuthorizationRules {
			p, err := r.policy("")
			if err != nil {
				return nil, fmt.Errorf("config: %w", err)
			}
			c.Policies = append(c.Policies, p)
		}
		for _, a := range ns.RoleAssignments {
			role, err := auth.ParseRole(a.Role)
			if err != nil {
				return nil, fmt.Errorf("config: role assignment of %q: %w", a.Principal, err)
			}
			c.Assignments = append(c.Assignments, server.Assignment{Principal: a.Principal, Role: role, Scope: a.Scope})
		}
		for _, q := range ns.Queues {
			props, err := q.Properties.apply(broker.KindQueue)
			if err != nil {
				return nil, fmt.Errorf("config: queue %q: %w", q.Name, err)
			}
			c.Queues = append(c.Queues, server.Queue{Name: q.Name, Properties: props})
			for _, r := range q.AuthorizationRules {
				p, err := r.policy(q.Name)
				if err != nil {
					return nil, fmt.Errorf("config: queue %q: %w", q.Name, err)
				}
				c.Policies = append(c.Policies, p)
			}
		}
		for _, t := range ns.Topics {
			props, err := t.Properties.apply(broker.KindTopic)
			if err != nil {
				return nil, fmt.Errorf("config: topic %q: %w", t.Name, err)
			}
			topic := server.Topic{Name: t.Name, Properties: props}
			for _, r := range t.AuthorizationRules {
				p, err := r.policy(t.Name)
				if err != nil {
					return nil, fmt.Errorf("config: topic %q: %w", t.Name, err)
				}
				c.Policies = append(c.Policies, p)
			}
			for _, s := range t.Subscriptions {
				props, err := s.Properties.apply(broker.KindSubscription)
				if err != nil {
					return nil, fmt.Errorf("config: subscription %q/%q: %w", t.Name, s.Name, err)
				}
				sub := server.Subscription{Name: s.Name, Properties: props}
				if s.Rules != nil {
					// Declared rules replace $Default, as in the official
					// emulator.
					sub.Rules = []broker.Rule{}
					for _, r := range s.Rules {
						rule, err := r.rule()
						if err != nil {
							return nil, fmt.Errorf("config: subscription %q/%q: rule %q: %w", t.Name, s.Name, r.Name, err)
						}
						sub.Rules = append(sub.Rules, rule)
					}
				}
				topic.Subscriptions = append(topic.Subscriptions, sub)
			}
			c.Topics = append(c.Topics, topic)
		}
		if len(ns.Tags) > 0 {
			c.Tags = ns.Tags
		}
		where := "namespace " + ns.Name
		if r := ns.NetworkRuleSet; r != nil {
			if len(r.IPRules) > 0 {
				c.Unsupported = append(c.Unsupported, unsupported.Finding{Feature: unsupported.IPFilter, Where: where + " NetworkRuleSet.IpRules"})
			}
			if len(r.VirtualNetworkRules) > 0 {
				c.Unsupported = append(c.Unsupported, unsupported.Finding{Feature: unsupported.VNet, Where: where + " NetworkRuleSet.VirtualNetworkRules"})
			}
		}
		if p := ns.PublicNetworkAccess; p != nil && strings.EqualFold(*p, "Disabled") {
			c.Unsupported = append(c.Unsupported, unsupported.Finding{Feature: unsupported.PrivateLink, Where: where + " PublicNetworkAccess = Disabled"})
		}
		if len(ns.PrivateEndpoints) > 0 {
			c.Unsupported = append(c.Unsupported, unsupported.Finding{Feature: unsupported.PrivateLink, Where: where + " PrivateEndpoints"})
		}
		for _, l := range ns.Locks {
			lock, err := l.lock()
			if err != nil {
				return nil, fmt.Errorf("config: namespace %q: %w", ns.Name, err)
			}
			c.Locks = append(c.Locks, lock)
		}
		if e := ns.Encryption; e != nil {
			c.Encryption = &broker.Encryption{KeyVaultKeyIDs: e.KeyVaultKeyIDs, Identity: e.Identity, InfrastructureEncryption: e.InfrastructureEncryption}
		}
		if g := ns.GeoReplication; g != nil {
			rep, err := g.replication()
			if err != nil {
				return nil, fmt.Errorf("config: namespace %q: %w", ns.Name, err)
			}
			c.GeoReplication = rep
		}
		all = append(all, c)
	}
	var out *Config
	switch len(all) {
	case 0:
		out = &Config{}
	case 1:
		// A single namespace is the emulator's namespace.
		out = all[0]
	default:
		out = &Config{Namespaces: all}
	}
	for _, g := range doc.UserConfig.GeoDR {
		out.GeoDR = append(out.GeoDR, server.GeoDRConfig{Alias: g.Alias, Primary: g.Primary, Secondary: g.Secondary})
	}
	return out, nil
}

// jsonGeoDR pairs two namespaces under an alias.
type jsonGeoDR struct{ Alias, Primary, Secondary string }

// jsonGeoReplication configures the replication of a namespace's data.
type jsonGeoReplication struct {
	// Mode is "Synchronous" or "Asynchronous" (the default).
	Mode string
	// MaxReplicationLagSeconds is how far the secondary may lag before the
	// primary throttles, for the asynchronous mode.
	MaxReplicationLagSeconds *float64
	// ReplicationDelayMilliseconds simulates the distance between regions
	// in the asynchronous mode.
	ReplicationDelayMilliseconds *int
	PrimaryRegion                string
	SecondaryRegion              string
}

func (g *jsonGeoReplication) replication() (*server.GeoReplication, error) {
	r := &server.GeoReplication{PrimaryRegion: g.PrimaryRegion, SecondaryRegion: g.SecondaryRegion}
	switch strings.ToLower(g.Mode) {
	case "", "asynchronous", "async":
	case "synchronous", "sync":
		r.Synchronous = true
	default:
		return nil, fmt.Errorf("GeoReplication Mode %q: want Synchronous or Asynchronous", g.Mode)
	}
	if g.MaxReplicationLagSeconds != nil {
		r.MaxLag = time.Duration(*g.MaxReplicationLagSeconds * float64(time.Second))
	}
	if g.ReplicationDelayMilliseconds != nil {
		r.Delay = time.Duration(*g.ReplicationDelayMilliseconds) * time.Millisecond
	}
	return r, nil
}

// jsonLock is a resource lock declared in Config.json. Scope is an entity
// path, or empty for the namespace.
type jsonLock struct {
	Name  string
	Level string
	Scope string
	Notes string
}

func (l jsonLock) lock() (broker.Lock, error) {
	var level broker.LockLevel
	switch strings.ToLower(l.Level) {
	case "cannotdelete":
		level = broker.LockCanNotDelete
	case "readonly":
		level = broker.LockReadOnly
	default:
		return broker.Lock{}, fmt.Errorf("lock %q: level %q must be CanNotDelete or ReadOnly", l.Name, l.Level)
	}
	if l.Name == "" {
		return broker.Lock{}, errors.New("a lock needs a name")
	}
	return broker.Lock{Name: l.Name, Level: level, Scope: broker.NormalizePath(l.Scope), Notes: l.Notes}, nil
}

// jsonEncryption is the encryption of a namespace: customer-managed keys
// and infrastructure encryption.
type jsonEncryption struct {
	KeyVaultKeyIDs           []string `json:"KeyVaultKeyIds"`
	Identity                 string
	InfrastructureEncryption bool
}

type jsonDocument struct {
	UserConfig struct {
		Namespaces []struct {
			Name string
			// AuthorizationRules, RoleAssignments and LocalAuthEnabled
			// extend the official format.
			AuthorizationRules []jsonPolicy
			RoleAssignments    []jsonAssignment
			LocalAuthEnabled   *bool
			GeoReplication     *jsonGeoReplication
			// Tags, Locks and Encryption extend the official format.
			Tags map[string]string
			// NetworkRuleSet, PublicNetworkAccess and PrivateEndpoints are
			// accepted so that a namespace copied from Azure loads, and
			// reported as unsupported.
			NetworkRuleSet *struct {
				IPRules             []json.RawMessage `json:"IpRules"`
				VirtualNetworkRules []json.RawMessage
			}
			PublicNetworkAccess *string
			PrivateEndpoints    []json.RawMessage
			Locks               []jsonLock
			Encryption          *jsonEncryption
			Queues              []struct {
				Name               string
				Properties         jsonProperties
				AuthorizationRules []jsonPolicy
			}
			Topics []struct {
				Name               string
				Properties         jsonProperties
				AuthorizationRules []jsonPolicy
				Subscriptions      []struct {
					Name       string
					Properties jsonProperties
					Rules      []jsonRule
				}
			}
		}
		// GeoDR extends the official format.
		GeoDR   []jsonGeoDR
		Logging json.RawMessage
	}
}

// jsonPolicy is a shared access policy declared in Config.json.
type jsonPolicy struct {
	Name         string
	Rights       []string
	PrimaryKey   string
	SecondaryKey string
}

// jsonAssignment is a role assignment declared in Config.json.
type jsonAssignment struct {
	Principal string
	Role      string
	// Scope is an entity path, or empty for the namespace.
	Scope string
}

func (p jsonPolicy) policy(scope string) (auth.Policy, error) {
	rights, err := auth.ParseRights(p.Rights)
	if err != nil {
		return auth.Policy{}, fmt.Errorf("authorization rule %q: %w", p.Name, err)
	}
	return DeclaredPolicy(scope, p.Name, rights, p.PrimaryKey, p.SecondaryKey), nil
}

// DeclaredPolicy builds a declared policy, filling missing keys with the
// derived ones.
func DeclaredPolicy(scope, name string, rights auth.Right, primary, secondary string) auth.Policy {
	p := auth.Policy{Scope: scope, Name: name, Rights: rights, PrimaryKey: primary, SecondaryKey: secondary}
	if p.PrimaryKey == "" {
		p.PrimaryKey = auth.DeclaredKey(scope, name, false)
	}
	if p.SecondaryKey == "" {
		p.SecondaryKey = auth.DeclaredKey(scope, name, true)
	}
	return p
}

// jsonRule is a rule of the official format. JSON names match fields
// case-insensitively, so "SqlFilter" fills SQLFilter.
type jsonRule struct {
	Name       string
	Properties struct {
		FilterType        string
		CorrelationFilter *struct {
			ContentType      string
			CorrelationID    string
			Label            string
			Subject          string
			MessageID        string
			ReplyTo          string
			ReplyToSessionID string
			SessionID        string
			To               string
			Properties       map[string]any
		}
		SQLFilter *struct{ SQLExpression string }
		Action    *struct{ SQLExpression string }
	}
}

func (j *jsonRule) rule() (broker.Rule, error) {
	r := broker.Rule{Name: j.Name}
	p := j.Properties
	switch strings.ToLower(p.FilterType) {
	case "sql":
		if p.SQLFilter == nil {
			return r, errors.New("FilterType Sql needs a SqlFilter")
		}
		r.Filter = broker.RuleFilter{Kind: broker.FilterSQL, SQL: p.SQLFilter.SQLExpression}
	case "correlation":
		if p.CorrelationFilter == nil {
			return r, errors.New("FilterType Correlation needs a CorrelationFilter")
		}
		c := p.CorrelationFilter
		subject := c.Label
		if subject == "" {
			subject = c.Subject
		}
		r.Filter = broker.RuleFilter{Kind: broker.FilterCorrelation, Correlation: &filter.CorrelationFilter{
			CorrelationID:    c.CorrelationID,
			MessageID:        c.MessageID,
			To:               c.To,
			ReplyTo:          c.ReplyTo,
			Subject:          subject,
			SessionID:        c.SessionID,
			ReplyToSessionID: c.ReplyToSessionID,
			ContentType:      c.ContentType,
			Properties:       c.Properties,
		}}
	case "true":
		r.Filter = broker.RuleFilter{Kind: broker.FilterTrue}
	case "false":
		r.Filter = broker.RuleFilter{Kind: broker.FilterFalse}
	default:
		return r, fmt.Errorf("unknown FilterType %q: want Sql, Correlation, True or False", p.FilterType)
	}
	if p.Action != nil {
		r.Action = p.Action.SQLExpression
	}
	return r, ValidateRule(r)
}

// ValidateRule parses the SQL filter and action of r, so that a bad
// declaration fails at startup.
func ValidateRule(r broker.Rule) error {
	if r.Filter.Kind == broker.FilterSQL {
		if _, err := filter.ParseSQLFilter(r.Filter.SQL); err != nil {
			return err
		}
	}
	if strings.TrimSpace(r.Action) != "" {
		if _, err := filter.ParseSQLAction(r.Action); err != nil {
			return err
		}
	}
	return nil
}

// jsonProperties lists every entity property. The official emulator
// accepts a subset; the rest use the names of the Service Bus ARM API.
type jsonProperties struct {
	LockDuration                              *isoDuration   `json:",omitempty"`
	MaxSizeInMegabytes                        *int64         `json:",omitempty"`
	RequiresDuplicateDetection                *bool          `json:",omitempty"`
	RequiresSession                           *bool          `json:",omitempty"`
	DefaultMessageTimeToLive                  *isoDuration   `json:",omitempty"`
	DeadLetteringOnMessageExpiration          *bool          `json:",omitempty"`
	DeadLetteringOnFilterEvaluationExceptions *bool          `json:",omitempty"`
	DuplicateDetectionHistoryTimeWindow       *isoDuration   `json:",omitempty"`
	MaxDeliveryCount                          *int32         `json:",omitempty"`
	EnableBatchedOperations                   *bool          `json:",omitempty"`
	Status                                    *broker.Status `json:",omitempty"`
	AutoDeleteOnIdle                          *isoDuration   `json:",omitempty"`
	EnablePartitioning                        *bool          `json:",omitempty"`
	EnableExpress                             *bool          `json:",omitempty"`
	SupportOrdering                           *bool          `json:",omitempty"`
	ForwardTo                                 *string        `json:",omitempty"`
	ForwardDeadLetteredMessagesTo             *string        `json:",omitempty"`
	MaxMessageSizeInKilobytes                 *int64         `json:",omitempty"`
	UserMetadata                              *string        `json:",omitempty"`
}

func (j *jsonProperties) apply(kind broker.EntityKind) (broker.Properties, error) {
	p := broker.DefaultProperties(kind)
	setDuration(&p.LockDuration, j.LockDuration)
	set(&p.MaxSizeInMegabytes, j.MaxSizeInMegabytes)
	set(&p.RequiresDuplicateDetection, j.RequiresDuplicateDetection)
	set(&p.RequiresSession, j.RequiresSession)
	setDuration(&p.DefaultMessageTimeToLive, j.DefaultMessageTimeToLive)
	set(&p.DeadLetteringOnMessageExpiration, j.DeadLetteringOnMessageExpiration)
	set(&p.DeadLetteringOnFilterEvaluationExceptions, j.DeadLetteringOnFilterEvaluationExceptions)
	setDuration(&p.DuplicateDetectionHistoryTimeWindow, j.DuplicateDetectionHistoryTimeWindow)
	set(&p.MaxDeliveryCount, j.MaxDeliveryCount)
	set(&p.EnableBatchedOperations, j.EnableBatchedOperations)
	set(&p.Status, j.Status)
	setDuration(&p.AutoDeleteOnIdle, j.AutoDeleteOnIdle)
	set(&p.EnablePartitioning, j.EnablePartitioning)
	set(&p.EnableExpress, j.EnableExpress)
	set(&p.SupportOrdering, j.SupportOrdering)
	set(&p.ForwardTo, j.ForwardTo)
	set(&p.ForwardDeadLetteredMessagesTo, j.ForwardDeadLetteredMessagesTo)
	set(&p.MaxMessageSizeInKilobytes, j.MaxMessageSizeInKilobytes)
	set(&p.UserMetadata, j.UserMetadata)
	if err := p.Validate(kind); err != nil {
		return broker.Properties{}, err
	}
	return p, nil
}

func set[T any](dst *T, v *T) {
	if v != nil {
		*dst = *v
	}
}

func setDuration(dst *time.Duration, v *isoDuration) {
	if v != nil {
		*dst = time.Duration(*v)
	}
}

// isoDuration is an ISO 8601 duration such as "PT1M" or "P14D".
type isoDuration time.Duration

func (d isoDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(duration.Format(time.Duration(d)))
}

func (d *isoDuration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be an ISO 8601 string such as \"PT1M\": %w", err)
	}
	v, err := duration.Parse(s)
	if err != nil {
		return err
	}
	*d = isoDuration(v)
	return nil
}

// stripComments blanks out // and /* */ comments outside JSON strings,
// keeping offsets so decoding errors point at the right place.
func stripComments(data []byte) []byte {
	out := bytes.Clone(data)
	inString := false
	for i := 0; i < len(out); i++ {
		switch c := out[i]; {
		case inString:
			if c == '\\' {
				i++
			} else if c == '"' {
				inString = false
			}
		case c == '"':
			inString = true
		case c == '/' && i+1 < len(out) && out[i+1] == '/':
			for ; i < len(out) && out[i] != '\n'; i++ {
				out[i] = ' '
			}
		case c == '/' && i+1 < len(out) && out[i+1] == '*':
			end := bytes.Index(out[i+2:], []byte("*/"))
			stop := len(out)
			if end >= 0 {
				stop = i + 2 + end + 2
			}
			for ; i < stop; i++ {
				if out[i] != '\n' {
					out[i] = ' '
				}
			}
			i--
		}
	}
	return out
}

// Merge adds the entities of o to c. An entity declared in both takes its
// properties from o; subscriptions of a topic in both are merged the same
// way, keeping their rules unless o declares rules.
func (c *Config) Merge(o *Config) {
	c.GeoDR = append(c.GeoDR, o.GeoDR...)
	c.Unsupported = append(c.Unsupported, o.Unsupported...)
	if len(c.Namespaces) > 0 || len(o.Namespaces) > 0 {
		c.mergeNamespaces(o)
		return
	}
	if c.Name == "" {
		c.Name = o.Name
	}
	if o.GeoReplication != nil {
		c.GeoReplication = o.GeoReplication
	}
	if len(o.Tags) > 0 {
		if c.Tags == nil {
			c.Tags = map[string]string{}
		}
		for k, v := range o.Tags {
			c.Tags[k] = v
		}
	}
	if o.Encryption != nil {
		c.Encryption = o.Encryption
	}
	for _, l := range o.Locks {
		replaced := false
		for i, x := range c.Locks {
			if x.Name == l.Name {
				c.Locks[i], replaced = l, true
			}
		}
		if !replaced {
			c.Locks = append(c.Locks, l)
		}
	}
	c.LocalAuthDisabled = c.LocalAuthDisabled || o.LocalAuthDisabled
	if o.Partitions > 0 {
		c.Partitions = o.Partitions
	}
	for _, p := range o.Policies {
		replaced := false
		for i, x := range c.Policies {
			if x.Scope == p.Scope && x.Name == p.Name {
				c.Policies[i], replaced = p, true
			}
		}
		if !replaced {
			c.Policies = append(c.Policies, p)
		}
	}
	c.Assignments = append(c.Assignments, o.Assignments...)
	for _, q := range o.Queues {
		if i := index(c.Queues, q.Name, func(q server.Queue) string { return q.Name }); i >= 0 {
			c.Queues[i] = q
		} else {
			c.Queues = append(c.Queues, q)
		}
	}
	for _, t := range o.Topics {
		i := index(c.Topics, t.Name, func(t server.Topic) string { return t.Name })
		if i < 0 {
			c.Topics = append(c.Topics, t)
			continue
		}
		dst := &c.Topics[i]
		dst.Properties = t.Properties
		for _, s := range t.Subscriptions {
			if j := index(dst.Subscriptions, s.Name, func(s server.Subscription) string { return s.Name }); j >= 0 {
				if s.Rules == nil {
					s.Rules = dst.Subscriptions[j].Rules
				}
				dst.Subscriptions[j] = s
			} else {
				dst.Subscriptions = append(dst.Subscriptions, s)
			}
		}
	}
}

// index finds an entity by name, case-insensitively as Service Bus does.
func index[T any](items []T, name string, nameOf func(T) string) int {
	for i, it := range items {
		if strings.EqualFold(nameOf(it), name) {
			return i
		}
	}
	return -1
}

// entries returns the namespaces a Config declares: its Namespaces, or
// itself as the only one.
func (c *Config) entries() []*Config {
	if len(c.Namespaces) > 0 {
		return c.Namespaces
	}
	self := *c
	self.GeoDR = nil
	if self.Name == "" && len(self.Queues)+len(self.Topics)+len(self.Policies)+len(self.Assignments) == 0 &&
		!self.LocalAuthDisabled && self.Partitions == 0 && self.GeoReplication == nil && len(self.Unsupported) == 0 &&
		len(self.Tags)+len(self.Locks) == 0 && self.Encryption == nil {
		return nil
	}
	return []*Config{&self}
}

// mergeNamespaces merges o into c by namespace name.
func (c *Config) mergeNamespaces(o *Config) {
	byName := map[string]*Config{}
	var order []*Config
	for _, list := range [][]*Config{c.entries(), o.entries()} {
		for _, ns := range list {
			key := strings.ToLower(ns.Name)
			if prev, ok := byName[key]; ok {
				prev.Merge(ns)
				continue
			}
			cp := *ns
			byName[key] = &cp
			order = append(order, &cp)
		}
	}
	geo := c.GeoDR
	*c = Config{Namespaces: order, GeoDR: geo}
}

// Split separates the default namespace from the others in a Config that
// declares several. The default is the one named defaultName, or the first
// named one; entities declared without a namespace belong to it. A Config
// with a single namespace is returned as it is.
func (c *Config) Split(defaultName string) (def *Config, others []*Config) {
	if len(c.Namespaces) == 0 {
		return c, nil
	}
	pick := defaultName
	if pick == "" {
		for _, ns := range c.Namespaces {
			if ns.Name != "" {
				pick = ns.Name
				break
			}
		}
	}
	def = &Config{Name: pick, GeoDR: c.GeoDR}
	for _, ns := range c.Namespaces {
		if ns.Name == "" || strings.EqualFold(ns.Name, pick) {
			cp := *ns
			cp.Name = pick
			def.Merge(&cp)
			continue
		}
		others = append(others, ns)
	}
	return def, others
}
