// Package admin serves the Service Bus management API: the ATOM feeds the
// SDK administration clients use to manage queues, topics, subscriptions
// and rules.
package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/unsupported"
)

// Handler serves the management API of a broker. Authorization headers
// are accepted without validation.
type Handler struct {
	broker  *broker.Broker
	log     *slog.Logger
	started time.Time
	// Auth, when it is enabled, requires an Authorization header whose
	// token gives Manage on the entity of the request.
	Auth *auth.Authority
}

// NewHandler returns a handler for the management API of b.
func NewHandler(b *broker.Broker, log *slog.Logger) *Handler {
	return &Handler{broker: b, log: log, started: time.Now().UTC()}
}

// errBadRequest marks malformed requests.
var errBadRequest = errors.New("bad request")

func badRequest(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errBadRequest, fmt.Sprintf(format, args...))
}

// statusError is an error with its HTTP status.
type statusError struct {
	code int
	msg  string
}

func (e *statusError) Error() string { return e.msg }

// request is a parsed management request.
type request struct {
	r    *http.Request
	base string // https://host/
	// update is set by If-Match on a PUT.
	update bool
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req := &request{r: r, base: "https://" + r.Host + "/", update: r.Header.Get("If-Match") != ""}
	var (
		status int
		body   string
		err    error
	)
	if err = h.authorize(r); err == nil {
		status, body, err = h.route(broker.WithActor(r.Context(), "atom"), req)
	}
	if err != nil {
		status = errorStatus(err)
		var xw xmlWriter
		writeError(&xw, status, err.Error())
		body = xw.b.String()
		h.log.Debug("management request failed", "method", r.Method, "path", r.URL.Path, "status", status, "err", err)
	}
	ctype := "application/atom+xml;type=entry;charset=utf-8"
	if strings.HasPrefix(body, "<feed") {
		ctype = "application/atom+xml;type=feed;charset=utf-8"
	} else if strings.HasPrefix(body, "<Error") {
		ctype = "application/xml;charset=utf-8"
	}
	w.Header().Set("Content-Type", ctype)
	w.WriteHeader(status)
	w.Write([]byte(body))
}

// authorize checks the Authorization header of a request, when
// authentication is on. Every management request needs Manage on the entity
// it names, or on the namespace for requests that name none.
func (h *Handler) authorize(r *http.Request) error {
	if err := h.Auth.AuthorizeRequest(r, authScope(r.URL.Path), auth.Manage); err != nil {
		return &statusError{http.StatusUnauthorized, "401: " + strings.TrimPrefix(err.Error(), "unauthorized: ")}
	}
	return nil
}

// authScope maps a request path to the scope its authorization is checked
// against: the entity, or "" for namespace requests such as $Resources.
func authScope(path string) string {
	p := strings.ToLower(strings.Trim(path, "/"))
	if strings.HasPrefix(p, "$") {
		return ""
	}
	if i := strings.Index(p, "/rules"); i >= 0 {
		p = p[:i]
	}
	return p
}

func errorStatus(err error) int {
	var se *statusError
	switch {
	case errors.As(err, &se):
		return se.code
	case errors.Is(err, broker.ErrEntityNotFound), errors.Is(err, broker.ErrRuleNotFound):
		return http.StatusNotFound
	case errors.Is(err, broker.ErrEntityExists), errors.Is(err, broker.ErrLocked):
		return http.StatusConflict
	case errors.Is(err, broker.ErrReadOnly):
		return http.StatusForbidden
	case errors.Is(err, store.ErrThrottled):
		return http.StatusServiceUnavailable
	case errors.Is(err, broker.ErrInvalidOperation), errors.Is(err, errBadRequest):
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

// route dispatches by path. Entity names are not escaped by the SDKs and
// queue and topic names may contain slashes, so a path is a subscription
// or rule path only when it has a Subscriptions segment.
func (h *Handler) route(ctx context.Context, req *request) (int, string, error) {
	path := strings.TrimPrefix(req.r.URL.Path, "/")
	method := req.r.Method
	switch {
	case path == "":
		return 0, "", &statusError{http.StatusNotFound, "no entity in the request path"}
	case strings.EqualFold(path, "$namespaceinfo"):
		if method != http.MethodGet {
			return 0, "", methodNotAllowed(method)
		}
		return h.namespaceInfo(req)
	case strings.EqualFold(path, "$Resources/Queues"):
		return h.list(ctx, req, broker.KindQueue, "", "Queues")
	case strings.EqualFold(path, "$Resources/Topics"):
		return h.list(ctx, req, broker.KindTopic, "", "Topics")
	}

	topic, rest, ok := cutSegment(path, "Subscriptions")
	if !ok {
		return h.entity(ctx, req, path)
	}
	sub, rest, _ := strings.Cut(rest, "/")
	if sub == "" {
		if method != http.MethodGet {
			return 0, "", methodNotAllowed(method)
		}
		return h.list(ctx, req, broker.KindSubscription, topic, "Subscriptions")
	}
	if rest == "" {
		return h.subscription(ctx, req, topic, sub)
	}
	rules, rule, _ := strings.Cut(rest, "/")
	if !strings.EqualFold(rules, "Rules") {
		return 0, "", &statusError{http.StatusNotFound, fmt.Sprintf("unknown path %s", path)}
	}
	if rule == "" && method == http.MethodGet {
		return h.listRules(ctx, req, topic, sub)
	}
	return h.rule(ctx, req, topic, sub, rule)
}

// cutSegment splits path around its first /segment, matched
// case-insensitively, returning what follows it without a leading slash.
func cutSegment(path, segment string) (before, after string, ok bool) {
	parts := strings.Split(path, "/")
	for i := 1; i < len(parts); i++ {
		if strings.EqualFold(parts[i], segment) {
			return strings.Join(parts[:i], "/"), strings.Join(parts[i+1:], "/"), true
		}
	}
	return "", "", false
}

func methodNotAllowed(method string) error {
	return &statusError{http.StatusMethodNotAllowed, fmt.Sprintf("method %s is not allowed", method)}
}

func notFound(kind, path string) error {
	return &statusError{http.StatusNotFound, fmt.Sprintf("The messaging entity '%s' could not be found.", path)}
}

func (h *Handler) namespaceInfo(req *request) (int, string, error) {
	name, _, _ := strings.Cut(req.r.Host, ":")
	var w xmlWriter
	writeEntry(&w, req.base, "$namespaceinfo", name, h.started, h.started, func(w *xmlWriter) {
		openDescription(w, "NamespaceInfo")
		w.time("CreatedTime", h.started)
		w.el("MessagingSKU", "Standard")
		w.time("ModifiedTime", h.started)
		w.el("Name", name)
		w.el("NamespaceType", "Messaging")
		w.raw(`</NamespaceInfo>`)
	})
	return http.StatusOK, w.b.String(), nil
}

// entity serves a queue or topic path.
func (h *Handler) entity(ctx context.Context, req *request, path string) (int, string, error) {
	switch req.r.Method {
	case http.MethodGet:
		e, err := h.broker.Entity(path)
		if err != nil || e.Kind == broker.KindSubscription {
			return 0, "", notFound("entity", path)
		}
		return h.entityResponse(ctx, req, e, http.StatusOK)
	case http.MethodDelete:
		if e, err := h.broker.Entity(path); err != nil || e.Kind == broker.KindSubscription {
			return 0, "", notFound("entity", path)
		}
		if err := h.broker.DeleteEntity(ctx, path); err != nil {
			return 0, "", err
		}
		return http.StatusOK, "", nil
	case http.MethodPut:
		entry, err := decodeEntry(req.r.Body)
		if err != nil {
			return 0, "", err
		}
		kind, desc := broker.KindQueue, entry.Content.Queue
		if desc == nil {
			kind, desc = broker.KindTopic, entry.Content.Topic
		}
		if desc == nil {
			return 0, "", badRequest("the entry has no QueueDescription or TopicDescription")
		}
		return h.put(ctx, req, kind, path, "", desc)
	}
	return 0, "", methodNotAllowed(req.r.Method)
}

// put creates an entity, or updates it when the request has If-Match.
// An update replaces the description, as in Service Bus: an element left
// out goes back to its default, except the properties that cannot change,
// which keep their value, and a request that sets one of those to another
// value is refused.
func (h *Handler) put(ctx context.Context, req *request, kind broker.EntityKind, path, topic string, desc *description) (int, string, error) {
	if err := h.refuseUnsupported(ctx, kind, path, desc); err != nil {
		return 0, "", err
	}
	existing, err := h.broker.Entity(path)
	exists := err == nil
	if req.update {
		if !exists {
			return 0, "", notFound(string(kind), path)
		}
		if existing.Kind != kind {
			return 0, "", badRequest("%s is a %s, not a %s", path, existing.Kind, kind)
		}
		props := broker.DefaultProperties(kind)
		keepImmutable(&props, &existing.Properties)
		if err := desc.apply(&props); err != nil {
			return 0, "", err
		}
		if err := checkImmutable(kind, &existing.Properties, &props); err != nil {
			h.broker.RecordFailure(ctx, "Update", kind, path, err)
			return 0, "", err
		}
		e, err := h.broker.UpdateEntity(ctx, path, props)
		if err != nil {
			return 0, "", err
		}
		if err := h.applyRules(ctx, e.Path, desc.AuthorizationRules); err != nil {
			return 0, "", err
		}
		return h.entityResponse(ctx, req, e, http.StatusOK)
	}
	if exists {
		err := &statusError{http.StatusConflict, fmt.Sprintf("The messaging entity '%s' already exists.", path)}
		h.broker.RecordFailure(ctx, "Create", kind, path, err)
		return 0, "", err
	}
	props := broker.DefaultProperties(kind)
	if err := desc.apply(&props); err != nil {
		return 0, "", err
	}
	var e *broker.Entity
	switch kind {
	case broker.KindQueue:
		e, err = h.broker.CreateQueue(ctx, path, props)
	case broker.KindTopic:
		e, err = h.broker.CreateTopic(ctx, path, props)
	case broker.KindSubscription:
		_, name, _ := cutSegment(path, "Subscriptions")
		e, err = h.broker.CreateSubscription(ctx, topic, name, props)
		if err == nil && desc.DefaultRuleDescription != nil {
			err = h.setDefaultRule(ctx, e, desc.DefaultRuleDescription)
		}
	}
	if err != nil {
		return 0, "", err
	}
	if err := h.applyRules(ctx, e.Path, desc.AuthorizationRules); err != nil {
		return 0, "", err
	}
	return h.entityResponse(ctx, req, e, http.StatusCreated)
}

// refuseUnsupported refuses a description that asks for a feature the
// emulator leaves out, instead of creating something that behaves otherwise:
// a JMS client-scoped subscription.
func (h *Handler) refuseUnsupported(ctx context.Context, kind broker.EntityKind, path string, desc *description) error {
	affine := desc.ClientAffineProperties != nil
	if v := desc.IsClientAffine; v != nil && strings.EqualFold(strings.TrimSpace(*v), "true") {
		affine = true
	}
	if !affine || kind != broker.KindSubscription {
		return nil
	}
	f := unsupported.Finding{Feature: unsupported.JMSClientScoped, Where: "subscription " + path}
	unsupported.LogRefused(h.log, f)
	err := badRequest("%s are not supported by open-service-bus (IsClientAffine must be false)", f.Feature.Name)
	h.broker.RecordFailure(ctx, "Create", kind, path, err)
	return err
}

// keepImmutable copies the properties that cannot change from cur into p, so
// that an update which leaves them out keeps them.
func keepImmutable(p, cur *broker.Properties) {
	p.RequiresSession = cur.RequiresSession
	p.RequiresDuplicateDetection = cur.RequiresDuplicateDetection
	p.EnablePartitioning = cur.EnablePartitioning
}

// checkImmutable refuses an update that changes RequiresSession,
// RequiresDuplicateDetection or EnablePartitioning (a subscription has only
// the first), with the messages of the page "Service Bus Resource Manager
// exceptions".
func checkImmutable(kind broker.EntityKind, cur, next *broker.Properties) error {
	const code = "Sub code=40000. "
	switch {
	case cur.RequiresSession != next.RequiresSession:
		if kind == broker.KindTopic {
			break
		}
		return badRequest("%sThe value for RequiresSession property of an existing %s can't be changed.", code, titleKind(kind))
	}
	if kind != broker.KindSubscription {
		if cur.RequiresDuplicateDetection != next.RequiresDuplicateDetection {
			return badRequest("%sThe value for the 'requiresDuplicateDetection' property of an existing %s can't be changed.", code, titleKind(kind))
		}
		if cur.EnablePartitioning != next.EnablePartitioning {
			return badRequest("%sPartitioning can't be changed for Queue/Topic.", code)
		}
	}
	return nil
}

func titleKind(k broker.EntityKind) string {
	s := string(k)
	return strings.ToUpper(s[:1]) + s[1:]
}

// setDefaultRule replaces the $Default rule of a new subscription with the
// rule of its DefaultRuleDescription.
func (h *Handler) setDefaultRule(ctx context.Context, e *broker.Entity, r *ruleXML) error {
	rule, err := r.rule("")
	if err == nil {
		err = h.broker.SetRules(ctx, e.Path, []broker.Rule{rule})
	}
	if err != nil {
		if derr := h.broker.DeleteEntity(ctx, e.Path); derr != nil {
			h.log.Warn("removing a subscription with an invalid default rule", "path", e.Path, "err", derr)
		}
	}
	return err
}

func (h *Handler) subscription(ctx context.Context, req *request, topic, sub string) (int, string, error) {
	path := broker.SubscriptionPath(topic, sub)
	display := topic + "/Subscriptions/" + sub
	switch req.r.Method {
	case http.MethodGet:
		e, err := h.broker.Entity(path)
		if err != nil || e.Kind != broker.KindSubscription {
			return 0, "", notFound("subscription", display)
		}
		return h.entityResponse(ctx, req, e, http.StatusOK)
	case http.MethodDelete:
		if e, err := h.broker.Entity(path); err != nil || e.Kind != broker.KindSubscription {
			return 0, "", notFound("subscription", display)
		}
		if err := h.broker.DeleteEntity(ctx, path); err != nil {
			return 0, "", err
		}
		return http.StatusOK, "", nil
	case http.MethodPut:
		entry, err := decodeEntry(req.r.Body)
		if err != nil {
			return 0, "", err
		}
		desc := entry.Content.Subscription
		if desc == nil {
			return 0, "", badRequest("the entry has no SubscriptionDescription")
		}
		if t, err := h.broker.Entity(topic); err != nil || t.Kind != broker.KindTopic {
			return 0, "", notFound("topic", topic)
		}
		return h.put(ctx, req, broker.KindSubscription, path, topic, desc)
	}
	return 0, "", methodNotAllowed(req.r.Method)
}

// entityResponse writes the entry of e with its runtime properties: the
// SDKs read descriptions and runtime properties from the same URL.
func (h *Handler) entityResponse(ctx context.Context, req *request, e *broker.Entity, status int) (int, string, error) {
	var w xmlWriter
	if err := h.writeEntityEntry(ctx, &w, req.base, e); err != nil {
		return 0, "", err
	}
	return status, w.b.String(), nil
}

func (h *Handler) writeEntityEntry(ctx context.Context, w *xmlWriter, base string, e *broker.Entity) error {
	counts, err := h.broker.Counts(ctx, e.Path)
	if err != nil {
		return err
	}
	path := e.DisplayPath()
	switch e.Kind {
	case broker.KindQueue:
		writeEntry(w, base, path, e.Name, e.CreatedAt, e.UpdatedAt, func(w *xmlWriter) { writeQueue(w, e, counts, h.policies(e.Path)) })
	case broker.KindTopic:
		stats := topicStats{counts: counts}
		for _, s := range h.broker.Entities(broker.KindSubscription, e.Path) {
			stats.subscriptions++
			rules, err := h.broker.Rules(s.Path)
			if err != nil {
				return err
			}
			for _, r := range rules {
				if r.Filter.Kind == broker.FilterCorrelation {
					stats.correlationFilter++
				} else {
					stats.sqlFilters++
				}
			}
		}
		writeEntry(w, base, path, e.Name, e.CreatedAt, e.UpdatedAt, func(w *xmlWriter) { writeTopic(w, e, stats, h.policies(e.Path)) })
	case broker.KindSubscription:
		writeEntry(w, base, path, e.Name, e.CreatedAt, e.UpdatedAt, func(w *xmlWriter) { writeSubscription(w, e, counts) })
	}
	return nil
}

// page reads $skip and $top. A missing $top means every entry.
func page(r *http.Request) (skip, top int, err error) {
	q := r.URL.Query()
	if v := q.Get("$skip"); v != "" {
		if skip, err = strconv.Atoi(v); err != nil || skip < 0 {
			return 0, 0, badRequest("invalid $skip %q", v)
		}
	}
	if v := q.Get("$top"); v != "" {
		if top, err = strconv.Atoi(v); err != nil || top < 0 {
			return 0, 0, badRequest("invalid $top %q", v)
		}
	}
	return skip, top, nil
}

func pageSlice[T any](items []T, skip, top int) []T {
	items = items[min(skip, len(items)):]
	if top > 0 {
		items = items[:min(top, len(items))]
	}
	return items
}

func (h *Handler) list(ctx context.Context, req *request, kind broker.EntityKind, topic, title string) (int, string, error) {
	if req.r.Method != http.MethodGet {
		return 0, "", methodNotAllowed(req.r.Method)
	}
	feedPath := "$Resources/" + title
	if kind == broker.KindSubscription {
		t, err := h.broker.Entity(topic)
		if err != nil || t.Kind != broker.KindTopic {
			return 0, "", notFound("topic", topic)
		}
		feedPath = topic + "/Subscriptions"
	}
	skip, top, err := page(req.r)
	if err != nil {
		return 0, "", err
	}
	all := h.broker.Entities(kind, topic)
	items := pageSlice(all, skip, top)
	var (
		w       xmlWriter
		werr    error
		entries strings.Builder
	)
	for _, e := range items {
		var ew xmlWriter
		if werr = h.writeEntityEntry(ctx, &ew, req.base, e); werr != nil {
			return 0, "", werr
		}
		entries.WriteString(ew.b.String())
	}
	writeFeed(&w, feedPage{title: title, base: req.base, path: feedPath, skip: skip, top: top, total: len(all)}, func(w *xmlWriter) {
		w.raw(entries.String())
	})
	return http.StatusOK, w.b.String(), nil
}

func (h *Handler) listRules(ctx context.Context, req *request, topic, sub string) (int, string, error) {
	path := broker.SubscriptionPath(topic, sub)
	rules, err := h.broker.Rules(path)
	if err != nil {
		return 0, "", notFound("subscription", topic+"/Subscriptions/"+sub)
	}
	skip, top, err := page(req.r)
	if err != nil {
		return 0, "", err
	}
	items := pageSlice(rules, skip, top)
	base := topic + "/Subscriptions/" + sub + "/Rules"
	var w xmlWriter
	writeFeed(&w, feedPage{title: "Rules", base: req.base, path: base, skip: skip, top: top, total: len(rules)}, func(w *xmlWriter) {
		for _, r := range items {
			writeRuleEntry(w, req.base, base, r)
		}
	})
	return http.StatusOK, w.b.String(), nil
}

func writeRuleEntry(w *xmlWriter, base, rulesPath string, r broker.Rule) {
	writeEntry(w, base, rulesPath+"/"+r.Name, r.Name, r.CreatedAt, r.CreatedAt, func(w *xmlWriter) { writeRule(w, r) })
}

// rule serves a rule path. A PUT with no name in the path, which the Go SDK
// sends for rules without a name, creates the rule named in the body.
func (h *Handler) rule(ctx context.Context, req *request, topic, sub, name string) (int, string, error) {
	path := broker.SubscriptionPath(topic, sub)
	rulesPath := topic + "/Subscriptions/" + sub + "/Rules"
	if _, err := h.broker.Rules(path); err != nil {
		return 0, "", notFound("subscription", topic+"/Subscriptions/"+sub)
	}
	find := func(name string) (broker.Rule, bool) {
		rules, _ := h.broker.Rules(path)
		for _, r := range rules {
			if strings.EqualFold(r.Name, name) {
				return r, true
			}
		}
		return broker.Rule{}, false
	}
	switch req.r.Method {
	case http.MethodGet:
		r, ok := find(name)
		if !ok {
			return 0, "", notFound("rule", rulesPath+"/"+name)
		}
		var w xmlWriter
		writeRuleEntry(&w, req.base, rulesPath, r)
		return http.StatusOK, w.b.String(), nil
	case http.MethodDelete:
		if err := h.broker.DeleteRule(ctx, path, name); err != nil {
			return 0, "", err
		}
		return http.StatusOK, "", nil
	case http.MethodPut:
		entry, err := decodeEntry(req.r.Body)
		if err != nil {
			return 0, "", err
		}
		if entry.Content.Rule == nil {
			return 0, "", badRequest("the entry has no RuleDescription")
		}
		rule, err := entry.Content.Rule.rule(name)
		if err != nil {
			return 0, "", err
		}
		status := http.StatusCreated
		if req.update {
			status = http.StatusOK
			err = h.broker.UpdateRule(ctx, path, rule)
		} else {
			err = h.broker.AddRule(ctx, path, rule)
		}
		if err != nil {
			return 0, "", err
		}
		r, _ := find(rule.Name)
		var w xmlWriter
		writeRuleEntry(&w, req.base, rulesPath, r)
		return status, w.b.String(), nil
	}
	return 0, "", methodNotAllowed(req.r.Method)
}

func sortStrings(s []string) { sort.Strings(s) }

// policies returns the shared access policies of the entity at path.
func (h *Handler) policies(path string) []auth.Policy {
	if h.Auth == nil {
		return nil
	}
	return h.Auth.Policies(path, false)
}

// applyRules makes the policies of a queue or topic the ones its
// description lists, as Service Bus does when it saves a description: the
// listed policies are saved, empty keys are generated, and the ones left
// out are removed.
func (h *Handler) applyRules(ctx context.Context, path string, rules *authorizationRules) error {
	if h.Auth == nil || rules == nil || strings.Contains(path, "/subscriptions/") {
		return nil
	}
	keep := map[string]bool{}
	for _, r := range rules.AuthorizationRule {
		rights, err := auth.ParseRights(r.Rights.AccessRights)
		if err != nil {
			return badRequest("authorization rule %q: %v", r.KeyName, err)
		}
		if r.KeyName == "" {
			return badRequest("an authorization rule needs a KeyName")
		}
		keep[r.KeyName] = true
		if err := h.Auth.SavePolicy(ctx, auth.Policy{Scope: path, Name: r.KeyName, Rights: rights, PrimaryKey: r.PrimaryKey, SecondaryKey: r.SecondaryKey}); err != nil {
			return err
		}
	}
	for _, p := range h.Auth.Policies(path, false) {
		if !keep[p.Name] {
			if err := h.Auth.DeletePolicy(ctx, path, p.Name); err != nil {
				return err
			}
		}
	}
	return nil
}
