package amqpsb

import (
	"context"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
)

// Descriptors of the rule types in $management rule operations.
const (
	descRuleDescription   = uint64(0x0000013700000004)
	descEmptyRuleAction   = uint64(0x0000013700000005)
	descSQLRuleAction     = uint64(0x0000013700000006)
	descSQLFilter         = uint64(0x0000001370000006)
	descTrueFilter        = uint64(0x0000001370000007)
	descFalseFilter       = uint64(0x0000001370000008)
	descCorrelationFilter = uint64(0x0000001370000009)
)

// sqlCompatibilityLevel is the compatibility level Service Bus reports for
// SQL filters and actions.
const sqlCompatibilityLevel = int32(20)

func (m *management) addRule(ctx context.Context, req *request) (any, int32, error) {
	name, ok := getString(req.body, "rule-name")
	if !ok || name == "" {
		return nil, 0, badRequest("rule-name is required")
	}
	v, _ := get(req.body, "rule-description")
	desc, ok := v.(encoding.Map)
	if !ok {
		return nil, 0, badRequest("rule-description is %T, want map", v)
	}
	r := broker.Rule{Name: name, Filter: broker.RuleFilter{Kind: broker.FilterTrue}}
	if v, ok := get(desc, "sql-filter"); ok && v != nil {
		f, ok := v.(encoding.Map)
		if !ok {
			return nil, 0, badRequest("sql-filter is %T, want map", v)
		}
		expr, _ := getString(f, "expression")
		r.Filter = broker.RuleFilter{Kind: broker.FilterSQL, SQL: expr}
	} else if v, ok := get(desc, "correlation-filter"); ok && v != nil {
		f, ok := v.(encoding.Map)
		if !ok {
			return nil, 0, badRequest("correlation-filter is %T, want map", v)
		}
		c := &filter.CorrelationFilter{}
		c.CorrelationID, _ = getString(f, "correlation-id")
		c.MessageID, _ = getString(f, "message-id")
		c.To, _ = getString(f, "to")
		c.ReplyTo, _ = getString(f, "reply-to")
		c.Subject, _ = getString(f, "label")
		c.SessionID, _ = getString(f, "session-id")
		c.ReplyToSessionID, _ = getString(f, "reply-to-session-id")
		c.ContentType, _ = getString(f, "content-type")
		if v, ok := get(f, "properties"); ok && v != nil {
			props, ok := v.(encoding.Map)
			if !ok {
				return nil, 0, badRequest("correlation-filter properties is %T, want map", v)
			}
			c.Properties = map[string]any{}
			for _, e := range stringKeyed(props) {
				c.Properties[e.Key.(string)] = e.Value
			}
		}
		r.Filter = broker.RuleFilter{Kind: broker.FilterCorrelation, Correlation: c}
	}
	if v, ok := get(desc, "sql-rule-action"); ok && v != nil {
		a, ok := v.(encoding.Map)
		if !ok {
			return nil, 0, badRequest("sql-rule-action is %T, want map", v)
		}
		r.Action, _ = getString(a, "expression")
	}
	if err := m.h.Broker.AddRule(broker.WithActor(ctx, "management"), m.addr.Entity.Path, r); err != nil {
		return nil, 0, err
	}
	return nil, statusOK, nil
}

func (m *management) removeRule(ctx context.Context, req *request) (any, int32, error) {
	name, ok := getString(req.body, "rule-name")
	if !ok || name == "" {
		return nil, 0, badRequest("rule-name is required")
	}
	if err := m.h.Broker.DeleteRule(broker.WithActor(ctx, "management"), m.addr.Entity.Path, name); err != nil {
		return nil, 0, err
	}
	return nil, statusOK, nil
}

func (m *management) enumerateRules(ctx context.Context, req *request) (any, int32, error) {
	rules, err := m.h.Broker.Rules(m.addr.Entity.Path)
	if err != nil {
		return nil, 0, err
	}
	skip, _ := getInt64(req.body, "skip")
	top, ok := getInt64(req.body, "top")
	if !ok || top <= 0 {
		top = int64(len(rules))
	}
	rules = rules[min(int(max(skip, 0)), len(rules)):]
	rules = rules[:min(int(top), len(rules))]
	if len(rules) == 0 {
		return nil, statusNoContent, nil
	}
	list := make([]any, len(rules))
	for i, r := range rules {
		list[i] = encoding.Map{{Key: "rule-description", Value: ruleDescription(r)}}
	}
	return encoding.Map{{Key: "rules", Value: list}}, statusOK, nil
}

// ruleDescription encodes a rule as the described list Service Bus uses:
// filter, action, name and creation time.
func ruleDescription(r broker.Rule) encoding.Described {
	var f encoding.Described
	switch r.Filter.Kind {
	case broker.FilterSQL:
		f = encoding.Described{Descriptor: descSQLFilter, Value: []any{r.Filter.SQL, sqlCompatibilityLevel}}
	case broker.FilterFalse:
		f = encoding.Described{Descriptor: descFalseFilter, Value: []any{}}
	case broker.FilterCorrelation:
		c := r.Filter.Correlation
		var props encoding.Map
		for k, v := range c.Properties {
			props = append(props, encoding.MapEntry{Key: k, Value: v})
		}
		f = encoding.Described{Descriptor: descCorrelationFilter, Value: []any{
			nullable(c.CorrelationID), nullable(c.MessageID), nullable(c.To), nullable(c.ReplyTo),
			nullable(c.Subject), nullable(c.SessionID), nullable(c.ReplyToSessionID), nullable(c.ContentType),
			props,
		}}
	default:
		f = encoding.Described{Descriptor: descTrueFilter, Value: []any{}}
	}
	// Clients read the action's descriptor without a nil check, so a rule
	// without action gets the empty action.
	action := encoding.Described{Descriptor: descEmptyRuleAction, Value: []any{}}
	if r.Action != "" {
		action = encoding.Described{Descriptor: descSQLRuleAction, Value: []any{r.Action, sqlCompatibilityLevel}}
	}
	return encoding.Described{Descriptor: descRuleDescription, Value: []any{f, action, r.Name, r.CreatedAt.UTC().Truncate(time.Millisecond)}}
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
