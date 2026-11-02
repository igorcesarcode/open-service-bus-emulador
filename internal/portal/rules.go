package portal

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

func (h *Handler) registerRules() {
	h.mux.HandleFunc("GET "+Prefix+"rules", h.rulesPage)
	h.mux.HandleFunc("GET "+Prefix+"rules/new", h.ruleForm(false))
	h.mux.HandleFunc("GET "+Prefix+"rules/edit", h.ruleForm(true))
	h.mux.HandleFunc("POST "+Prefix+"rules/save", h.saveRule)
	h.mux.HandleFunc("POST "+Prefix+"rules/delete", h.deleteRule)
	h.mux.HandleFunc("POST "+Prefix+"rules/validate", h.validateRule)
	h.mux.HandleFunc("POST "+Prefix+"rules/test", h.testRules)
}

const defaultSample = "sys.Label = order.created\nquantity = 20\nregion = us"

func (h *Handler) subscriptionFromRequest(w http.ResponseWriter, r *http.Request) *broker.Entity {
	e := h.entityFromRequest(w, r)
	if e != nil && e.Kind != broker.KindSubscription {
		http.Error(w, "rules belong to subscriptions", http.StatusBadRequest)
		return nil
	}
	return e
}

func ruleType(r broker.Rule) string {
	switch r.Filter.Kind {
	case broker.FilterSQL:
		return "SQL filter"
	case broker.FilterCorrelation:
		return "Correlation filter"
	case broker.FilterFalse:
		return "False filter"
	}
	return "True filter"
}

func ruleFilterText(r broker.Rule) string {
	switch r.Filter.Kind {
	case broker.FilterSQL:
		return r.Filter.SQL
	case broker.FilterCorrelation:
		c := r.Filter.Correlation
		if c == nil {
			return ""
		}
		var parts []string
		for _, f := range correlationFields(c) {
			if *f.value != "" {
				parts = append(parts, f.name+" = "+*f.value)
			}
		}
		keys := make([]string, 0, len(c.Properties))
		for k := range c.Properties {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s = %v", k, c.Properties[k]))
		}
		return strings.Join(parts, ", ")
	case broker.FilterFalse:
		return "1=0"
	}
	return "1=1"
}

type corrField struct {
	name  string // the form field and the label
	value *string
}

func correlationFields(c *filter.CorrelationFilter) []corrField {
	return []corrField{
		{"correlationId", &c.CorrelationID}, {"messageId", &c.MessageID}, {"to", &c.To}, {"replyTo", &c.ReplyTo},
		{"subject", &c.Subject}, {"sessionId", &c.SessionID}, {"replyToSessionId", &c.ReplyToSessionID},
		{"contentType", &c.ContentType},
	}
}

func (h *Handler) rulesPage(w http.ResponseWriter, r *http.Request) {
	e := h.subscriptionFromRequest(w, r)
	if e == nil {
		return
	}
	h.renderRules(w, r, e, defaultSample, nil)
}

func (h *Handler) renderRules(w http.ResponseWriter, r *http.Request, e *broker.Entity, sample string, results []broker.RuleResult) {
	rules, err := h.opts.Broker.Rules(e.Path)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	p := views.RulesPage{Path: e.Path, Name: e.DisplayPath(), TopicName: e.Topic.Name, Sample: sample}
	for _, ru := range rules {
		p.Rules = append(p.Rules, views.RuleRow{Name: ru.Name, Type: ruleType(ru), Filter: ruleFilterText(ru), Action: ru.Action})
	}
	if results != nil {
		p.Tested = true
		in, _ := parseSample(sample)
		for _, res := range results {
			p.Results = append(p.Results, views.RuleResultView{
				Name: res.Name, Matched: res.Matched, Error: res.Error, Changes: sampleDiff(in, res.After),
			})
		}
	}
	if r.Header.Get("HX-Request") == "true" && r.Method == http.MethodPost {
		h.render(w, r, http.StatusOK, views.RuleResults(p))
		return
	}
	h.render(w, r, http.StatusOK, views.RulesPageView(h.layout("Rules of "+e.DisplayPath(), views.NavTopics), p))
}

// parseTyped reads "name = value" lines. Values that look like numbers or
// booleans are typed; anything else, or a quoted value, is text.
func parseTyped(text string) (map[string]any, error) {
	out := map[string]any{}
	for i, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		name = strings.TrimSpace(name)
		if !ok || name == "" {
			return nil, fmt.Errorf("line %d: want name = value, got %q", i+1, line)
		}
		out[name] = typedValue(strings.TrimSpace(value))
	}
	return out, nil
}

func typedValue(v string) any {
	if len(v) >= 2 && (v[0] == '"' && v[len(v)-1] == '"' || v[0] == '\'' && v[len(v)-1] == '\'') {
		return v[1 : len(v)-1]
	}
	switch strings.ToLower(v) {
	case "true":
		return true
	case "false":
		return false
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return v
}

// parseSample reads the tester's message description.
func parseSample(text string) (broker.SampleMessage, error) {
	all, err := parseTyped(text)
	if err != nil {
		return broker.SampleMessage{}, err
	}
	m := broker.SampleMessage{System: map[string]any{}, User: map[string]any{}}
	for name, v := range all {
		if rest, ok := cutPrefixFold(name, "sys."); ok {
			canon, ok := filter.SystemProperty(rest)
			if !ok {
				return m, fmt.Errorf("unknown system property %q", name)
			}
			m.System[canon] = v
			continue
		}
		m.User[name] = v
	}
	return m, nil
}

func cutPrefixFold(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix) {
		return s[len(prefix):], true
	}
	return s, false
}

// sampleDiff lists what an action changed.
func sampleDiff(before broker.SampleMessage, after *broker.SampleMessage) []string {
	if after == nil {
		return nil
	}
	var out []string
	diff := func(prefix string, a, b map[string]any) {
		keys := map[string]bool{}
		for k := range a {
			keys[k] = true
		}
		for k := range b {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			old, hadOld := a[k]
			cur, hasCur := b[k]
			switch {
			case !hadOld && hasCur:
				out = append(out, fmt.Sprintf("+ %s%s = %v", prefix, k, cur))
			case hadOld && !hasCur:
				out = append(out, fmt.Sprintf("- %s%s", prefix, k))
			case fmt.Sprint(old) != fmt.Sprint(cur):
				out = append(out, fmt.Sprintf("~ %s%s: %v -> %v", prefix, k, old, cur))
			}
		}
	}
	diff("sys.", before.System, after.System)
	diff("", before.User, after.User)
	return out
}

func (h *Handler) testRules(w http.ResponseWriter, r *http.Request) {
	e := h.subscriptionFromRequest(w, r)
	if e == nil {
		return
	}
	sample := r.PostFormValue("sample")
	msg, err := parseSample(sample)
	if err != nil {
		http.Error(w, "Message: "+err.Error(), http.StatusUnprocessableEntity)
		return
	}
	rules, err := h.opts.Broker.Rules(e.Path)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.renderRules(w, r, e, sample, append([]broker.RuleResult{}, broker.TestRules(rules, msg)...))
}

var filterKinds = []string{string(broker.FilterSQL), string(broker.FilterCorrelation), string(broker.FilterTrue), string(broker.FilterFalse)}

// ruleFormFor builds the form that edits rule, which may be zero.
func ruleFormFor(e *broker.Entity, rule broker.Rule, edit bool, formErr string) views.RuleForm {
	f := views.RuleForm{
		Path: e.Path, SubName: e.DisplayPath(), Edit: edit, Error: formErr, Name: rule.Name,
		Kind: string(rule.Filter.Kind), Kinds: filterKinds, SQL: rule.Filter.SQL, Action: rule.Action,
		Parameters: typedLines(rule.Filter.Parameters),
	}
	if f.Kind == "" {
		f.Kind = string(broker.FilterSQL)
	}
	c := rule.Filter.Correlation
	if c == nil {
		c = &filter.CorrelationFilter{}
	}
	for _, cf := range correlationFields(c) {
		f.Correlation = append(f.Correlation, views.Field{Name: cf.name, Label: cf.name, Value: *cf.value})
	}
	f.CorrProps = typedLines(c.Properties)
	return f
}

func typedLines(v filter.Values) string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		if s, ok := v[k].(string); ok && typedValue(s) != any(s) {
			// Text that looks like a number or boolean keeps its quotes.
			fmt.Fprintf(&b, "%s = %q\n", k, s)
			continue
		}
		fmt.Fprintf(&b, "%s = %v\n", k, v[k])
	}
	return b.String()
}

func (h *Handler) ruleForm(edit bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		e := h.subscriptionFromRequest(w, r)
		if e == nil {
			return
		}
		var rule broker.Rule
		if edit {
			rules, _ := h.opts.Broker.Rules(e.Path)
			found := false
			for _, ru := range rules {
				if strings.EqualFold(ru.Name, r.URL.Query().Get("name")) {
					rule, found = ru, true
				}
			}
			if !found {
				http.NotFound(w, r)
				return
			}
		}
		h.render(w, r, http.StatusOK, views.RuleFormPage(h.layout("Rule", views.NavTopics), ruleFormFor(e, rule, edit, "")))
	}
}

// ruleFromForm reads the posted rule.
func ruleFromForm(r *http.Request) (broker.Rule, error) {
	rule := broker.Rule{
		Name:   strings.TrimSpace(r.PostFormValue("name")),
		Action: strings.TrimSpace(r.PostFormValue("action")),
	}
	rule.Filter.Kind = broker.FilterKind(r.PostFormValue("kind"))
	var errs []error
	switch rule.Filter.Kind {
	case broker.FilterSQL:
		rule.Filter.SQL = strings.TrimSpace(r.PostFormValue("sql"))
		params, err := parseTyped(r.PostFormValue("parameters"))
		if err != nil {
			errs = append(errs, fmt.Errorf("SQL filter parameters: %w", err))
		}
		if len(params) > 0 {
			rule.Filter.Parameters = params
		}
	case broker.FilterCorrelation:
		c := &filter.CorrelationFilter{}
		for _, cf := range correlationFields(c) {
			*cf.value = strings.TrimSpace(r.PostFormValue(cf.name))
		}
		props, err := parseTyped(r.PostFormValue("corrprops"))
		if err != nil {
			errs = append(errs, fmt.Errorf("Custom properties: %w", err))
		}
		if len(props) > 0 {
			c.Properties = props
		}
		rule.Filter.Correlation = c
	}
	return rule, errors.Join(errs...)
}

func (h *Handler) saveRule(w http.ResponseWriter, r *http.Request) {
	e := h.subscriptionFromRequest(w, r)
	if e == nil {
		return
	}
	edit := r.PostFormValue("edit") != ""
	rule, err := ruleFromForm(r)
	if err == nil {
		ctx := broker.WithActor(r.Context(), Actor)
		if edit {
			err = h.opts.Broker.UpdateRule(ctx, e.Path, rule)
		} else {
			err = h.opts.Broker.AddRule(ctx, e.Path, rule)
		}
	}
	if err != nil {
		msg := strings.TrimPrefix(err.Error(), "broker: ")
		h.render(w, r, http.StatusUnprocessableEntity, views.RuleFormPage(h.layout("Rule", views.NavTopics), ruleFormFor(e, rule, edit, msg)))
		return
	}
	http.Redirect(w, r, Prefix+"rules?path="+urlQuery(e.Path), http.StatusSeeOther)
}

func (h *Handler) deleteRule(w http.ResponseWriter, r *http.Request) {
	e := h.subscriptionFromRequest(w, r)
	if e == nil {
		return
	}
	err := h.opts.Broker.DeleteRule(broker.WithActor(r.Context(), Actor), e.Path, r.PostFormValue("name"))
	if err != nil && !errors.Is(err, broker.ErrRuleNotFound) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// validateRule answers the form's live check with a fragment.
func (h *Handler) validateRule(w http.ResponseWriter, r *http.Request) {
	rule, err := ruleFromForm(r)
	if rule.Name == "" {
		rule.Name = "rule" // The name is checked on save.
	}
	if err == nil {
		err = broker.ValidateRule(rule)
	}
	msg := ""
	if err != nil {
		msg = strings.TrimPrefix(strings.TrimPrefix(err.Error(), "broker: "), "invalid operation: ")
	}
	h.render(w, r, http.StatusOK, views.Validation(err == nil, msg))
}
