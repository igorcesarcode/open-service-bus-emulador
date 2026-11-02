package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// DefaultRuleName is the rule every new subscription starts with.
const DefaultRuleName = "$Default"

// ErrRuleNotFound reports an unknown rule.
var ErrRuleNotFound = errors.New("broker: rule not found")

// FilterKind is the type of a rule filter.
type FilterKind string

const (
	FilterSQL         FilterKind = "Sql"
	FilterCorrelation FilterKind = "Correlation"
	FilterTrue        FilterKind = "True"
	FilterFalse       FilterKind = "False"
)

// RuleFilter selects the messages a rule copies into its subscription.
type RuleFilter struct {
	Kind        FilterKind                `json:"kind"`
	SQL         string                    `json:"sql,omitempty"`
	Correlation *filter.CorrelationFilter `json:"correlation,omitempty"`
	// Parameters are the values of @name references in SQL.
	Parameters filter.Values `json:"parameters,omitempty"`
}

// Rule is a subscription rule: a filter and an optional SQL action.
type Rule struct {
	Name   string     `json:"-"`
	Filter RuleFilter `json:"filter"`
	// Action is a SQL rule action; empty means none.
	Action           string        `json:"action,omitempty"`
	ActionParameters filter.Values `json:"actionParameters,omitempty"`
	CreatedAt        time.Time     `json:"-"`
}

// TrueRule returns a rule that selects every message.
func TrueRule(name string) Rule {
	return Rule{Name: name, Filter: RuleFilter{Kind: FilterTrue}}
}

// compiledRule is a validated rule ready for evaluation.
type compiledRule struct {
	Rule
	id     int64
	sql    *filter.SQLFilter
	action *filter.SQLAction
}

// validateRuleName checks a rule name. Unlike entity names, rule names may
// start with $, as $Default does.
func validateRuleName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: empty rule name", ErrInvalidOperation)
	case len(name) > 50:
		return fmt.Errorf("%w: rule name %q is longer than 50 characters", ErrInvalidOperation, name)
	case strings.ContainsAny(name, "/\\?#"):
		return fmt.Errorf("%w: invalid character in rule name %q", ErrInvalidOperation, name)
	}
	return nil
}

func compileRule(r Rule) (*compiledRule, error) {
	if err := validateRuleName(r.Name); err != nil {
		return nil, err
	}
	c := &compiledRule{Rule: r}
	var err error
	switch r.Filter.Kind {
	case FilterTrue, FilterFalse:
	case FilterSQL:
		if c.sql, err = filter.ParseSQLFilter(r.Filter.SQL); err != nil {
			return nil, fmt.Errorf("%w: rule %s: %v", ErrInvalidOperation, r.Name, err)
		}
		c.sql = c.sql.WithParameters(r.Filter.Parameters)
	case FilterCorrelation:
		if r.Filter.Correlation == nil {
			c.Filter.Correlation = &filter.CorrelationFilter{}
		}
	default:
		return nil, fmt.Errorf("%w: rule %s: unknown filter type %q", ErrInvalidOperation, r.Name, r.Filter.Kind)
	}
	if strings.TrimSpace(r.Action) != "" {
		if c.action, err = filter.ParseSQLAction(r.Action); err != nil {
			return nil, fmt.Errorf("%w: rule %s: %v", ErrInvalidOperation, r.Name, err)
		}
		c.action = c.action.WithParameters(r.ActionParameters)
	}
	return c, nil
}

// matchesAll reports whether the rule copies every message unchanged.
func (c *compiledRule) matchesAll() bool {
	return c.Filter.Kind == FilterTrue && c.action == nil
}

func (c *compiledRule) match(m filter.Message) (bool, error) {
	switch c.Filter.Kind {
	case FilterTrue:
		return true, nil
	case FilterFalse:
		return false, nil
	case FilterCorrelation:
		return c.Filter.Correlation.Match(m), nil
	}
	return c.sql.Match(m)
}

func (c *compiledRule) row(subscriptionID int64) (*store.Rule, error) {
	props, err := json.Marshal(c.Rule)
	if err != nil {
		return nil, err
	}
	return &store.Rule{ID: c.id, SubscriptionID: subscriptionID, Name: c.Name, Props: props, CreatedAt: c.CreatedAt.UnixMilli()}, nil
}

func ruleFromRow(r *store.Rule) (*compiledRule, error) {
	var rule Rule
	if err := json.Unmarshal(r.Props, &rule); err != nil {
		return nil, fmt.Errorf("broker: rule %s: decoding: %w", r.Name, err)
	}
	rule.Name = r.Name
	rule.CreatedAt = time.UnixMilli(r.CreatedAt).UTC()
	c, err := compileRule(rule)
	if err != nil {
		return nil, err
	}
	c.id = r.ID
	return c, nil
}

func (b *Broker) subscription(path string) (*Entity, error) {
	b.mu.RLock()
	e := b.entities[NormalizePath(path)]
	b.mu.RUnlock()
	if e == nil || e.Kind != KindSubscription {
		return nil, fmt.Errorf("%w: subscription %s", ErrEntityNotFound, path)
	}
	return e, nil
}

// Rules returns the rules of the subscription at path, in creation order.
func (b *Broker) Rules(path string) ([]Rule, error) {
	e, err := b.subscription(path)
	if err != nil {
		return nil, err
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	var out []Rule
	for _, r := range b.rules[e.id] {
		out = append(out, r.Rule)
	}
	return out, nil
}

// AddRule adds a rule to the subscription at path. It returns
// ErrEntityExists if a rule of that name exists.
func (b *Broker) AddRule(ctx context.Context, path string, r Rule) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.add_rule", telemetry.Entity(path))
	defer done(&err)
	if err := b.checkWritable(ctx); err != nil {
		return err
	}
	if err := b.checkLocks(ctx, opWrite, path); err != nil {
		return err
	}
	err = b.addRule(ctx, path, r)
	b.audit(ctx, "CreateRule", ruleEntity(path, r.Name), err, "")
	return err
}

func (b *Broker) addRule(ctx context.Context, path string, r Rule) error {
	c, err := compileRule(r)
	if err != nil {
		return err
	}
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	e, err := b.subscription(path)
	if err != nil {
		return err
	}
	b.mu.RLock()
	existing := b.rules[e.id]
	b.mu.RUnlock()
	for _, x := range existing {
		if strings.EqualFold(x.Name, c.Name) {
			return fmt.Errorf("%w: rule %s on %s", ErrEntityExists, c.Name, e.DisplayPath())
		}
	}
	c.CreatedAt = b.now().UTC().Truncate(time.Millisecond)
	row, err := c.row(e.id)
	if err != nil {
		return err
	}
	if err := b.store.Write(ctx, func(tx *store.Tx) error {
		if err := tx.InsertRule(row); err != nil {
			return err
		}
		return touchTx(tx, e, b.nowMillis())
	}); err != nil {
		return err
	}
	c.id = row.ID
	b.mu.Lock()
	b.rules[e.id] = append(append([]*compiledRule(nil), existing...), c)
	b.publishLocked()
	b.mu.Unlock()
	return nil
}

// UpdateRule replaces the filter and action of an existing rule of the
// subscription at path, keeping its position and creation time.
func (b *Broker) UpdateRule(ctx context.Context, path string, r Rule) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.update_rule", telemetry.Entity(path))
	defer done(&err)
	if err := b.checkWritable(ctx); err != nil {
		return err
	}
	if err := b.checkLocks(ctx, opWrite, path); err != nil {
		return err
	}
	err = b.updateRule(ctx, path, r)
	b.audit(ctx, "UpdateRule", ruleEntity(path, r.Name), err, "")
	return err
}

func (b *Broker) updateRule(ctx context.Context, path string, r Rule) error {
	c, err := compileRule(r)
	if err != nil {
		return err
	}
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	e, err := b.subscription(path)
	if err != nil {
		return err
	}
	b.mu.RLock()
	existing := b.rules[e.id]
	b.mu.RUnlock()
	for i, x := range existing {
		if !strings.EqualFold(x.Name, c.Name) {
			continue
		}
		c.id, c.Name, c.CreatedAt = x.id, x.Name, x.CreatedAt
		row, err := c.row(e.id)
		if err != nil {
			return err
		}
		if err := b.store.Write(ctx, func(tx *store.Tx) error {
			if err := tx.UpdateRule(row); err != nil {
				return err
			}
			return touchTx(tx, e, b.nowMillis())
		}); err != nil {
			return err
		}
		rules := append([]*compiledRule(nil), existing...)
		rules[i] = c
		b.mu.Lock()
		b.rules[e.id] = rules
		b.publishLocked()
		b.mu.Unlock()
		return nil
	}
	return fmt.Errorf("%w: The messaging entity '%s/Rules/%s' could not be found.", ErrRuleNotFound, e.DisplayPath(), r.Name)
}

// DeleteRule removes a rule from the subscription at path.
func (b *Broker) DeleteRule(ctx context.Context, path, name string) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.delete_rule")
	defer done(&err)
	if err := b.checkWritable(ctx); err != nil {
		return err
	}
	if err := b.checkLocks(ctx, opWrite, path); err != nil {
		return err
	}
	err = b.deleteRule(ctx, path, name)
	b.audit(ctx, "DeleteRule", ruleEntity(path, name), err, "")
	return err
}

func (b *Broker) deleteRule(ctx context.Context, path, name string) error {
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	e, err := b.subscription(path)
	if err != nil {
		return err
	}
	b.mu.RLock()
	existing := b.rules[e.id]
	b.mu.RUnlock()
	for i, x := range existing {
		if !strings.EqualFold(x.Name, name) {
			continue
		}
		if err := b.store.Write(ctx, func(tx *store.Tx) error {
			if err := tx.DeleteRule(x.id); err != nil {
				return err
			}
			return touchTx(tx, e, b.nowMillis())
		}); err != nil {
			return err
		}
		rest := append(append([]*compiledRule(nil), existing[:i]...), existing[i+1:]...)
		b.mu.Lock()
		b.rules[e.id] = rest
		b.publishLocked()
		b.mu.Unlock()
		return nil
	}
	// Clients recognize a missing entity by Service Bus's wording.
	return fmt.Errorf("%w: The messaging entity '%s/Rules/%s' could not be found.", ErrRuleNotFound, e.DisplayPath(), name)
}

// SetRules replaces every rule of the subscription at path.
func (b *Broker) SetRules(ctx context.Context, path string, rules []Rule) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.set_rules", telemetry.Entity(path))
	defer done(&err)
	if err := b.checkWritable(ctx); err != nil {
		return err
	}
	if err := b.checkLocks(ctx, opWrite, path); err != nil {
		return err
	}
	changed, err := b.setRules(ctx, path, rules)
	if changed || err != nil {
		b.audit(ctx, "SetRules", path, err, "")
	}
	return err
}

// setRules reports whether it changed anything: replacing the rules with the
// same ones is a no-op.
func (b *Broker) setRules(ctx context.Context, path string, rules []Rule) (bool, error) {
	compiled := make([]*compiledRule, len(rules))
	seen := map[string]bool{}
	now := b.now().UTC().Truncate(time.Millisecond)
	for i, r := range rules {
		c, err := compileRule(r)
		if err != nil {
			return false, err
		}
		if seen[strings.ToLower(c.Name)] {
			return false, fmt.Errorf("%w: duplicate rule %s", ErrInvalidOperation, c.Name)
		}
		seen[strings.ToLower(c.Name)] = true
		c.CreatedAt = now
		compiled[i] = c
	}
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	e, err := b.subscription(path)
	if err != nil {
		return false, err
	}
	b.mu.RLock()
	existing := b.rules[e.id]
	b.mu.RUnlock()
	if sameRules(existing, compiled) {
		return false, nil
	}
	err = b.store.Write(ctx, func(tx *store.Tx) error {
		for _, x := range existing {
			if err := tx.DeleteRule(x.id); err != nil {
				return err
			}
		}
		for _, c := range compiled {
			row, err := c.row(e.id)
			if err != nil {
				return err
			}
			row.ID = 0
			if err := tx.InsertRule(row); err != nil {
				return err
			}
			c.id = row.ID
		}
		return touchTx(tx, e, b.nowMillis())
	})
	if err != nil {
		return false, err
	}
	b.mu.Lock()
	b.rules[e.id] = compiled
	b.publishLocked()
	b.mu.Unlock()
	return true, nil
}

// sameRules reports whether a and b hold the same rules in the same order,
// ignoring when they were created.
func sameRules(a, b []*compiledRule) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i].Rule, b[i].Rule
		x.CreatedAt, y.CreatedAt = time.Time{}, time.Time{}
		if !reflect.DeepEqual(x, y) {
			return false
		}
	}
	return true
}

// envMessage exposes an encoded message to filters and actions.
type envMessage struct {
	env      *frames.Envelope
	seq      int64
	enqueued time.Time
	size     int
}

func (m *envMessage) Property(scope filter.Scope, name string) (any, bool) {
	if scope == filter.User {
		i := findProperty(m.env.ApplicationProperties, name)
		if i < 0 {
			return nil, false
		}
		return filterValue(m.env.ApplicationProperties[i].Value), true
	}
	p := m.env.Properties
	if p == nil {
		p = &frames.Properties{}
	}
	str := func(s string) (any, bool) { return s, s != "" }
	switch name {
	case "MessageId":
		return str(MessageIDString(p.MessageID))
	case "CorrelationId":
		return str(MessageIDString(p.CorrelationID))
	case "To":
		return str(p.To)
	case "ReplyTo":
		return str(p.ReplyTo)
	case "Label":
		return str(p.Subject)
	case "SessionId":
		return str(p.GroupID)
	case "ReplyToSessionId":
		return str(p.ReplyToGroupID)
	case "ContentType":
		return str(string(p.ContentType))
	case "SequenceNumber", "EnqueuedSequenceNumber":
		return m.seq, true
	case "EnqueuedTimeUtc":
		return m.enqueued, true
	case "Size":
		return int64(m.size), true
	case "DeliveryCount":
		return int64(0), true
	case "ScheduledEnqueueTimeUtc":
		if v, ok := m.env.MessageAnnotations.Get(encoding.Symbol("x-opt-scheduled-enqueue-time")); ok {
			return v, true
		}
	case "TimeToLive":
		if h := m.env.Header; h != nil && h.TTL != nil {
			return *h.TTL, true
		}
	case "ExpiresAtUtc":
		if !p.AbsoluteExpiryTime.IsZero() {
			return p.AbsoluteExpiryTime, true
		}
	}
	return nil, false
}

func (m *envMessage) SetProperty(scope filter.Scope, name string, v any) error {
	if scope == filter.User {
		props := m.env.ApplicationProperties
		if i := findProperty(props, name); i >= 0 {
			props = append(props[:i:i], props[i+1:]...)
		}
		m.env.ApplicationProperties = append(props, encoding.MapEntry{Key: name, Value: amqpValue(v)})
		m.env.ApplicationPropertiesModified = true
		return nil
	}
	if m.env.Properties == nil {
		m.env.Properties = &frames.Properties{}
	}
	p := m.env.Properties
	s, _ := v.(string)
	switch name {
	case "MessageId":
		p.MessageID = nilIfEmpty(s)
	case "CorrelationId":
		p.CorrelationID = nilIfEmpty(s)
	case "To":
		p.To = s
	case "ReplyTo":
		p.ReplyTo = s
	case "Label":
		p.Subject = s
	case "SessionId":
		p.GroupID = s
	case "ReplyToSessionId":
		p.ReplyToGroupID = s
	case "ContentType":
		p.ContentType = encoding.Symbol(s)
	default:
		return fmt.Errorf("broker: cannot set system property %s", name)
	}
	m.env.PropertiesModified = true
	return nil
}

func (m *envMessage) RemoveProperty(name string) {
	props := m.env.ApplicationProperties
	if i := findProperty(props, name); i >= 0 {
		m.env.ApplicationProperties = append(props[:i:i], props[i+1:]...)
		m.env.ApplicationPropertiesModified = true
	}
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// findProperty finds an application property by name, preferring an exact
// match; property names in filters are case-insensitive.
func findProperty(props encoding.Map, name string) int {
	fold := -1
	for i, e := range props {
		k, ok := e.Key.(string)
		if !ok {
			continue
		}
		if k == name {
			return i
		}
		if fold < 0 && strings.EqualFold(k, name) {
			fold = i
		}
	}
	return fold
}

// filterValue converts an AMQP value to the types filters understand.
func filterValue(v any) any {
	switch v := v.(type) {
	case encoding.Symbol:
		return string(v)
	case encoding.UUID:
		return filter.GUID(v)
	case encoding.Char:
		return string(rune(v))
	}
	return v
}

// amqpValue converts a value produced by an action to its AMQP encoding.
func amqpValue(v any) any {
	if g, ok := v.(filter.GUID); ok {
		return encoding.UUID(g)
	}
	return v
}
