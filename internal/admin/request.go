package admin

import (
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
)

const xsiNamespace = "http://www.w3.org/2001/XMLSchema-instance"

// requestEntry is an ATOM entry sent to create or update an entity.
// Elements match by local name, in any order and under any namespace
// prefix, since the SDKs differ in both.
type requestEntry struct {
	Content struct {
		Queue        *description `xml:"QueueDescription"`
		Topic        *description `xml:"TopicDescription"`
		Subscription *description `xml:"SubscriptionDescription"`
		Rule         *ruleXML     `xml:"RuleDescription"`
	} `xml:"content"`
}

// description holds the elements of queue, topic and subscription
// descriptions. Each is nil when absent, which keeps the current value.
type description struct {
	LockDuration                              *string
	MaxSizeInMegabytes                        *string
	RequiresDuplicateDetection                *string
	RequiresSession                           *string
	DefaultMessageTimeToLive                  *string
	DeadLetteringOnMessageExpiration          *string
	DeadLetteringOnFilterEvaluationExceptions *string
	DuplicateDetectionHistoryTimeWindow       *string
	MaxDeliveryCount                          *string
	EnableBatchedOperations                   *string
	Status                                    *string
	AutoDeleteOnIdle                          *string
	EnablePartitioning                        *string
	EnableExpress                             *string
	SupportOrdering                           *string
	ForwardTo                                 *string
	ForwardDeadLetteredMessagesTo             *string
	UserMetadata                              *string
	MaxMessageSizeInKilobytes                 *string
	DefaultRuleDescription                    *ruleXML
	AuthorizationRules                        *authorizationRules
	// IsClientAffine and ClientAffineProperties describe JMS client-scoped
	// subscriptions, which are not supported.
	IsClientAffine         *string
	ClientAffineProperties *struct{ ClientId *string }
}

// authorizationRules are the shared access policies of a queue or topic.
type authorizationRules struct {
	AuthorizationRule []authorizationRule
}

type authorizationRule struct {
	KeyName      string
	PrimaryKey   string
	SecondaryKey string
	Rights       struct{ AccessRights []string }
}

type ruleXML struct {
	Filter *filterXML
	Action *actionXML
	Name   *string
}

type filterXML struct {
	Type             string `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
	SqlExpression    *string
	Parameters       *keyValues
	CorrelationId    *string
	MessageId        *string
	To               *string
	ReplyTo          *string
	Label            *string
	SessionId        *string
	ReplyToSessionId *string
	ContentType      *string
	Properties       *keyValues
}

type actionXML struct {
	Type          string `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
	SqlExpression *string
	Parameters    *keyValues
}

type keyValues struct {
	Items []struct {
		Key   string
		Value struct {
			Type string `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
			Nil  string `xml:"http://www.w3.org/2001/XMLSchema-instance nil,attr"`
			Text string `xml:",chardata"`
		}
	} `xml:"KeyValueOfstringanyType"`
}

func decodeEntry(r io.Reader) (*requestEntry, error) {
	var e requestEntry
	if err := xml.NewDecoder(r).Decode(&e); err != nil {
		return nil, badRequest("invalid ATOM entry: %v", err)
	}
	return &e, nil
}

// apply merges the elements present in d into p.
func (d *description) apply(p *broker.Properties) error {
	var errs []error
	durationField := func(dst *time.Duration, v *string, name string) {
		if v == nil {
			return
		}
		x, err := duration.Parse(strings.TrimSpace(*v))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			return
		}
		*dst = x
	}
	boolField := func(dst *bool, v *string, name string) {
		if v == nil {
			return
		}
		x, err := strconv.ParseBool(strings.ToLower(strings.TrimSpace(*v)))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: invalid boolean %q", name, *v))
			return
		}
		*dst = x
	}
	intField := func(set func(int64), v *string, name string) {
		if v == nil {
			return
		}
		x, err := strconv.ParseInt(strings.TrimSpace(*v), 10, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: invalid integer %q", name, *v))
			return
		}
		set(x)
	}
	durationField(&p.LockDuration, d.LockDuration, "LockDuration")
	intField(func(x int64) { p.MaxSizeInMegabytes = x }, d.MaxSizeInMegabytes, "MaxSizeInMegabytes")
	boolField(&p.RequiresDuplicateDetection, d.RequiresDuplicateDetection, "RequiresDuplicateDetection")
	boolField(&p.RequiresSession, d.RequiresSession, "RequiresSession")
	durationField(&p.DefaultMessageTimeToLive, d.DefaultMessageTimeToLive, "DefaultMessageTimeToLive")
	boolField(&p.DeadLetteringOnMessageExpiration, d.DeadLetteringOnMessageExpiration, "DeadLetteringOnMessageExpiration")
	boolField(&p.DeadLetteringOnFilterEvaluationExceptions, d.DeadLetteringOnFilterEvaluationExceptions, "DeadLetteringOnFilterEvaluationExceptions")
	durationField(&p.DuplicateDetectionHistoryTimeWindow, d.DuplicateDetectionHistoryTimeWindow, "DuplicateDetectionHistoryTimeWindow")
	intField(func(x int64) { p.MaxDeliveryCount = int32(x) }, d.MaxDeliveryCount, "MaxDeliveryCount")
	boolField(&p.EnableBatchedOperations, d.EnableBatchedOperations, "EnableBatchedOperations")
	if d.Status != nil {
		p.Status = broker.Status(strings.TrimSpace(*d.Status))
	}
	durationField(&p.AutoDeleteOnIdle, d.AutoDeleteOnIdle, "AutoDeleteOnIdle")
	boolField(&p.EnablePartitioning, d.EnablePartitioning, "EnablePartitioning")
	boolField(&p.EnableExpress, d.EnableExpress, "EnableExpress")
	boolField(&p.SupportOrdering, d.SupportOrdering, "SupportOrdering")
	if d.ForwardTo != nil {
		p.ForwardTo = strings.TrimSpace(*d.ForwardTo)
	}
	if d.ForwardDeadLetteredMessagesTo != nil {
		p.ForwardDeadLetteredMessagesTo = strings.TrimSpace(*d.ForwardDeadLetteredMessagesTo)
	}
	if d.UserMetadata != nil {
		p.UserMetadata = *d.UserMetadata
	}
	intField(func(x int64) { p.MaxMessageSizeInKilobytes = x }, d.MaxMessageSizeInKilobytes, "MaxMessageSizeInKilobytes")
	if len(errs) > 0 {
		return badRequest("%v", errs[0])
	}
	return nil
}

// rule converts a rule description. A missing filter is TrueFilter and a
// missing action is EmptyRuleAction.
func (r *ruleXML) rule(name string) (broker.Rule, error) {
	if name == "" && r.Name != nil {
		name = strings.TrimSpace(*r.Name)
	}
	if name == "" {
		name = broker.DefaultRuleName
	}
	rule := broker.Rule{Name: name, Filter: broker.RuleFilter{Kind: broker.FilterTrue}}
	if f := r.Filter; f != nil {
		params, err := f.Parameters.values()
		if err != nil {
			return rule, err
		}
		switch localType(f.Type) {
		case "TrueFilter":
		case "FalseFilter":
			rule.Filter.Kind = broker.FilterFalse
		case "CorrelationFilter":
			props, err := f.Properties.values()
			if err != nil {
				return rule, err
			}
			rule.Filter = broker.RuleFilter{Kind: broker.FilterCorrelation, Correlation: &filter.CorrelationFilter{
				CorrelationID:    deref(f.CorrelationId),
				MessageID:        deref(f.MessageId),
				To:               deref(f.To),
				ReplyTo:          deref(f.ReplyTo),
				Subject:          deref(f.Label),
				SessionID:        deref(f.SessionId),
				ReplyToSessionID: deref(f.ReplyToSessionId),
				ContentType:      deref(f.ContentType),
				Properties:       props,
			}}
		case "SqlFilter", "":
			if f.SqlExpression == nil {
				return rule, badRequest("rule %s: SqlFilter without SqlExpression", name)
			}
			rule.Filter = broker.RuleFilter{Kind: broker.FilterSQL, SQL: *f.SqlExpression, Parameters: params}
		default:
			return rule, badRequest("rule %s: unsupported filter type %q", name, f.Type)
		}
	}
	if a := r.Action; a != nil {
		switch localType(a.Type) {
		case "EmptyRuleAction", "":
		case "SqlRuleAction":
			params, err := a.Parameters.values()
			if err != nil {
				return rule, err
			}
			rule.Action = deref(a.SqlExpression)
			rule.ActionParameters = params
		default:
			return rule, badRequest("rule %s: unsupported action type %q", name, a.Type)
		}
	}
	return rule, nil
}

// localType strips the namespace prefix of an xsi:type value.
func localType(t string) string {
	if _, local, ok := strings.Cut(t, ":"); ok {
		return local
	}
	return t
}

// values converts typed key/value pairs. Numbers keep their kind: int and
// long as int64, double and decimal as float64.
func (kv *keyValues) values() (filter.Values, error) {
	if kv == nil || len(kv.Items) == 0 {
		return nil, nil
	}
	out := filter.Values{}
	for _, it := range kv.Items {
		v := it.Value
		if v.Nil == "true" {
			out[it.Key] = nil
			continue
		}
		var (
			x   any
			err error
		)
		switch localType(v.Type) {
		case "int", "long", "short", "byte", "unsignedInt", "unsignedShort", "unsignedByte":
			x, err = strconv.ParseInt(strings.TrimSpace(v.Text), 10, 64)
		case "unsignedLong":
			var u uint64
			u, err = strconv.ParseUint(strings.TrimSpace(v.Text), 10, 64)
			x = u
		case "double", "float", "decimal":
			x, err = strconv.ParseFloat(strings.TrimSpace(v.Text), 64)
		case "boolean":
			x, err = strconv.ParseBool(strings.TrimSpace(v.Text))
		case "dateTime":
			x, err = time.Parse(time.RFC3339Nano, strings.TrimSpace(v.Text))
		case "duration":
			x, err = duration.Parse(strings.TrimSpace(v.Text))
		default:
			x = v.Text
		}
		if err != nil {
			return nil, badRequest("value %s: invalid %s %q", it.Key, localType(v.Type), v.Text)
		}
		out[it.Key] = x
	}
	return out, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
