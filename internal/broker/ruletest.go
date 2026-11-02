package broker

import (
	"fmt"
	"maps"
	"strings"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
)

// SampleMessage is a message described by its properties, for trying rules
// out without sending anything. System keys are canonical names such as
// "MessageId" or "Label"; user keys are matched case-insensitively.
type SampleMessage struct {
	System map[string]any
	User   map[string]any
}

func (m *SampleMessage) clone() *SampleMessage {
	return &SampleMessage{System: maps.Clone(m.System), User: maps.Clone(m.User)}
}

func (m *SampleMessage) userKey(name string) (string, bool) {
	if _, ok := m.User[name]; ok {
		return name, true
	}
	for k := range m.User {
		if strings.EqualFold(k, name) {
			return k, true
		}
	}
	return "", false
}

// Property implements filter.Message.
func (m *SampleMessage) Property(scope filter.Scope, name string) (any, bool) {
	if scope == filter.System {
		v, ok := m.System[name]
		return v, ok
	}
	k, ok := m.userKey(name)
	if !ok {
		return nil, false
	}
	return m.User[k], true
}

// SetProperty implements filter.MutableMessage.
func (m *SampleMessage) SetProperty(scope filter.Scope, name string, v any) error {
	if scope == filter.System {
		if m.System == nil {
			m.System = map[string]any{}
		}
		m.System[name] = v
		return nil
	}
	if m.User == nil {
		m.User = map[string]any{}
	}
	if k, ok := m.userKey(name); ok {
		delete(m.User, k)
	}
	m.User[name] = v
	return nil
}

// RemoveProperty implements filter.MutableMessage.
func (m *SampleMessage) RemoveProperty(name string) {
	if k, ok := m.userKey(name); ok {
		delete(m.User, k)
	}
}

// RuleResult is what one rule does to a sample message.
type RuleResult struct {
	Name string
	// Matched reports whether the filter selects the message.
	Matched bool
	// Error is set when the filter or action cannot be evaluated; the
	// message is then not delivered through this rule (or, on a
	// subscription that dead-letters filter exceptions, is dead-lettered).
	Error string
	// After is the message the subscription would receive, with the action
	// applied. Nil unless Matched.
	After *SampleMessage
}

// TestRules evaluates each rule against m, as a subscription does when a
// message reaches its topic. A rule that does not compile reports its error.
func TestRules(rules []Rule, m SampleMessage) []RuleResult {
	out := make([]RuleResult, len(rules))
	for i, r := range rules {
		res := RuleResult{Name: r.Name}
		c, err := compileRule(r)
		if err != nil {
			res.Error = strings.TrimPrefix(err.Error(), ErrInvalidOperation.Error()+": ")
			out[i] = res
			continue
		}
		ok, err := c.match(m.clone())
		switch {
		case err != nil:
			res.Error = err.Error()
		case ok:
			res.Matched = true
			after := m.clone()
			if c.action != nil {
				if err := c.action.Apply(after); err != nil {
					res.Matched = false
					res.Error = fmt.Sprintf("action: %v", err)
					out[i] = res
					continue
				}
			}
			res.After = after
		}
		out[i] = res
	}
	return out
}

// ValidateRule checks that r compiles, reporting the error a client would
// see when creating it.
func ValidateRule(r Rule) error {
	_, err := compileRule(r)
	return err
}
