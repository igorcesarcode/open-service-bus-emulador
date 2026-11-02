package mcp

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
)

type (
	correlationIn struct {
		CorrelationID    string         `json:"correlationId,omitempty"`
		MessageID        string         `json:"messageId,omitempty"`
		To               string         `json:"to,omitempty"`
		ReplyTo          string         `json:"replyTo,omitempty"`
		Subject          string         `json:"subject,omitempty" desc:"The label of the message."`
		SessionID        string         `json:"sessionId,omitempty"`
		ReplyToSessionID string         `json:"replyToSessionId,omitempty"`
		ContentType      string         `json:"contentType,omitempty"`
		Properties       map[string]any `json:"properties,omitempty" desc:"Application properties that must be equal."`
	}
	ruleIn struct {
		Name             string         `json:"name" desc:"Rule name." required:"true"`
		Filter           string         `json:"filter,omitempty" desc:"Filter type. Left out, it is Sql when sql is set, Correlation when correlation is set, otherwise True." enum:"Sql,Correlation,True,False"`
		SQL              string         `json:"sql,omitempty" desc:"SQL filter expression, such as \"color = 'red' AND qty > 3\"."`
		Parameters       map[string]any `json:"parameters,omitempty" desc:"Values of the @name references of the SQL filter."`
		Correlation      *correlationIn `json:"correlation,omitempty" desc:"Correlation filter: every field set must be equal."`
		Action           string         `json:"action,omitempty" desc:"SQL action run on matching messages, such as \"SET quality = 'fast'\"."`
		ActionParameters map[string]any `json:"actionParameters,omitempty" desc:"Values of the @name references of the action."`
	}
	subscriptionIn struct {
		Common
		Path string `json:"path" desc:"Subscription path: topic/Subscriptions/name." required:"true"`
	}
	ruleNameIn struct {
		Common
		Path string `json:"path" desc:"Subscription path: topic/Subscriptions/name." required:"true"`
		Name string `json:"name" desc:"Rule name." required:"true"`
	}
	ruleSaveIn struct {
		Common
		Path string `json:"path" desc:"Subscription path: topic/Subscriptions/name." required:"true"`
		Rule ruleIn `json:"rule" required:"true"`
	}
	deleteRuleIn struct {
		ruleNameIn
	}
	setRulesIn struct {
		Common
		Path  string   `json:"path" desc:"Subscription path: topic/Subscriptions/name." required:"true"`
		Rules []ruleIn `json:"rules" desc:"The complete list; it replaces every current rule, $Default included." required:"true"`
	}
	testRuleIn struct {
		Common
		Path       string         `json:"path,omitempty" desc:"Subscription whose current rules are tried."`
		Rules      []ruleIn       `json:"rules,omitempty" desc:"Rules to try instead of a subscription's."`
		System     map[string]any `json:"system,omitempty" desc:"System properties of the sample message: MessageId, Label (or Subject), CorrelationId, To, ReplyTo, SessionId, ContentType..."`
		Properties map[string]any `json:"properties,omitempty" desc:"Application properties of the sample message."`
	}
)

func (s *Server) registerRuleTools() {
	add(s, "list_rules", "Lists the rules of a subscription.", read,
		func(ctx context.Context, in subscriptionIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			rules, err := n.Broker().Rules(in.Path)
			if err != nil {
				return nil, err
			}
			items := make([]any, 0, len(rules))
			for _, r := range rules {
				items = append(items, ruleView(r))
			}
			return map[string]any{"items": items}, nil
		})

	add(s, "get_rule", "Returns one rule of a subscription.", read,
		func(ctx context.Context, in ruleNameIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			rules, err := n.Broker().Rules(in.Path)
			if err != nil {
				return nil, err
			}
			for _, r := range rules {
				if strings.EqualFold(r.Name, in.Name) {
					return ruleView(r), nil
				}
			}
			return nil, fmt.Errorf("rule %q not found in %s", in.Name, in.Path)
		})

	add(s, "add_rule", "Adds a rule to a subscription. A subscription made with create_subscription has a $Default rule that selects everything; delete it, or messages match through it whatever the new rule says.", write,
		func(ctx context.Context, in ruleSaveIn) (any, error) {
			return s.saveRule(ctx, in, (*broker.Broker).AddRule)
		})
	add(s, "update_rule", "Replaces the filter and action of an existing rule.", write,
		func(ctx context.Context, in ruleSaveIn) (any, error) {
			return s.saveRule(ctx, in, (*broker.Broker).UpdateRule)
		})
	add(s, "delete_rule", "Deletes a rule of a subscription. Without rules, a subscription receives nothing.", write,
		func(ctx context.Context, in deleteRuleIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			if err := n.Broker().DeleteRule(ctx, in.Path, in.Name); err != nil {
				return nil, err
			}
			return map[string]any{"deleted": in.Name}, nil
		})
	add(s, "set_rules", "Replaces all the rules of a subscription with the given list.", write,
		func(ctx context.Context, in setRulesIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			rules, err := rulesOf(in.Rules)
			if err != nil {
				return nil, err
			}
			if err := n.Broker().SetRules(ctx, in.Path, rules); err != nil {
				return nil, err
			}
			return map[string]any{"rules": len(rules)}, nil
		})

	add(s, "test_rule", "Tries rules on a sample message without sending anything: tells whether each rule selects it and what the message looks like after the rule's action. Use path to try a subscription's rules, or rules to try new ones.", read,
		func(ctx context.Context, in testRuleIn) (any, error) {
			var rules []broker.Rule
			var err error
			switch {
			case in.Path != "" && len(in.Rules) > 0:
				return nil, fmt.Errorf("give path or rules, not both")
			case in.Path != "":
				n, err := s.namespace(in.Namespace)
				if err != nil {
					return nil, err
				}
				if rules, err = n.Broker().Rules(in.Path); err != nil {
					return nil, err
				}
			case len(in.Rules) > 0:
				if rules, err = rulesOf(in.Rules); err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("give the path of a subscription or the rules to try")
			}
			sample := broker.SampleMessage{System: map[string]any{}, User: map[string]any{}}
			for k, v := range in.System {
				canon, ok := filter.SystemProperty(k)
				if !ok {
					return nil, fmt.Errorf("unknown system property %q", k)
				}
				sample.System[canon] = jsonValue(v)
			}
			for k, v := range in.Properties {
				sample.User[k] = jsonValue(v)
			}
			var out []any
			for _, r := range broker.TestRules(rules, sample) {
				v := map[string]any{"rule": r.Name, "matched": r.Matched}
				if r.Error != "" {
					v["error"] = r.Error
				}
				if r.After != nil {
					v["after"] = map[string]any{"system": r.After.System, "properties": r.After.User}
				}
				out = append(out, v)
			}
			return map[string]any{"results": out}, nil
		})
}

func (s *Server) saveRule(ctx context.Context, in ruleSaveIn, op func(*broker.Broker, context.Context, string, broker.Rule) error) (any, error) {
	n, err := s.namespace(in.Namespace)
	if err != nil {
		return nil, err
	}
	r, err := ruleOf(in.Rule)
	if err != nil {
		return nil, err
	}
	if err := op(n.Broker(), ctx, in.Path, r); err != nil {
		return nil, err
	}
	return ruleView(r), nil
}

func rulesOf(in []ruleIn) ([]broker.Rule, error) {
	rules := make([]broker.Rule, 0, len(in))
	for _, r := range in {
		x, err := ruleOf(r)
		if err != nil {
			return nil, err
		}
		rules = append(rules, x)
	}
	return rules, nil
}

// ruleOf builds a rule from its input and checks that it compiles.
func ruleOf(in ruleIn) (broker.Rule, error) {
	r := broker.Rule{Name: in.Name, Action: in.Action}
	kind := broker.FilterKind(in.Filter)
	if kind == "" {
		switch {
		case in.SQL != "":
			kind = broker.FilterSQL
		case in.Correlation != nil:
			kind = broker.FilterCorrelation
		default:
			kind = broker.FilterTrue
		}
	}
	switch strings.ToLower(string(kind)) {
	case "sql":
		r.Filter = broker.RuleFilter{Kind: broker.FilterSQL, SQL: in.SQL, Parameters: valuesOf(in.Parameters)}
	case "correlation":
		if in.Correlation == nil {
			return r, fmt.Errorf("rule %s: a Correlation filter needs correlation", in.Name)
		}
		c := in.Correlation
		r.Filter = broker.RuleFilter{Kind: broker.FilterCorrelation, Correlation: &filter.CorrelationFilter{
			CorrelationID: c.CorrelationID, MessageID: c.MessageID, To: c.To, ReplyTo: c.ReplyTo, Subject: c.Subject,
			SessionID: c.SessionID, ReplyToSessionID: c.ReplyToSessionID, ContentType: c.ContentType,
			Properties: valuesOf(c.Properties),
		}}
	case "true":
		r.Filter = broker.RuleFilter{Kind: broker.FilterTrue}
	case "false":
		r.Filter = broker.RuleFilter{Kind: broker.FilterFalse}
	default:
		return r, fmt.Errorf("rule %s: unknown filter %q: want Sql, Correlation, True or False", in.Name, in.Filter)
	}
	r.ActionParameters = valuesOf(in.ActionParameters)
	if err := broker.ValidateRule(r); err != nil {
		return r, fmt.Errorf("rule %s: %w", in.Name, err)
	}
	return r, nil
}

// valuesOf converts JSON values to the typed values filters compare.
func valuesOf(m map[string]any) filter.Values {
	if len(m) == 0 {
		return nil
	}
	out := make(filter.Values, len(m))
	for k, v := range m {
		out[k] = jsonValue(v)
	}
	return out
}

// jsonValue turns a whole JSON number into an int64, as filters keep
// integers apart from floating point numbers.
func jsonValue(v any) any {
	if f, ok := v.(float64); ok && f == math.Trunc(f) && math.Abs(f) < 1<<53 {
		return int64(f)
	}
	return v
}

func ruleView(r broker.Rule) map[string]any {
	v := map[string]any{"name": r.Name, "filter": r.Filter.Kind}
	switch r.Filter.Kind {
	case broker.FilterSQL:
		v["sql"] = r.Filter.SQL
		if len(r.Filter.Parameters) > 0 {
			v["parameters"] = r.Filter.Parameters
		}
	case broker.FilterCorrelation:
		v["correlation"] = r.Filter.Correlation
	}
	if r.Action != "" {
		v["action"] = r.Action
		if len(r.ActionParameters) > 0 {
			v["actionParameters"] = r.ActionParameters
		}
	}
	return v
}
