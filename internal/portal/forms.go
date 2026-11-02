package portal

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

// textField is a property edited as text in the creation form.
type textField struct {
	name, label, help string
	// kinds has a letter for each entity kind with the property: q for
	// queues, t for topics and s for subscriptions.
	kinds string
	get   func(p *broker.Properties) string
	set   func(p *broker.Properties, v string) error
}

func durationField(name, label, help, kinds string, f func(p *broker.Properties) *time.Duration) textField {
	return textField{
		name: name, label: label, help: help + " ISO 8601, such as PT30S or P14D.", kinds: kinds,
		get: func(p *broker.Properties) string { return duration.Format(*f(p)) },
		set: func(p *broker.Properties, v string) error {
			d, err := duration.Parse(strings.TrimSpace(v))
			if err != nil {
				return fmt.Errorf("%s: %w", label, err)
			}
			*f(p) = d
			return nil
		},
	}
}

func intField(name, label, help, kinds string, f func(p *broker.Properties) *int64) textField {
	return textField{
		name: name, label: label, help: help, kinds: kinds,
		get: func(p *broker.Properties) string { return fmt.Sprint(*f(p)) },
		set: func(p *broker.Properties, v string) error {
			n, err := atoi(v)
			if err != nil || n < 0 {
				return fmt.Errorf("%s: want a whole number, got %q", label, v)
			}
			*f(p) = n
			return nil
		},
	}
}

func stringField(name, label, help, kinds string, f func(p *broker.Properties) *string) textField {
	return textField{
		name: name, label: label, help: help, kinds: kinds,
		get: func(p *broker.Properties) string { return *f(p) },
		set: func(p *broker.Properties, v string) error { *f(p) = strings.TrimSpace(v); return nil },
	}
}

// Every property has a field; none is capped.
var textFields = []textField{
	intField("maxSizeInMegabytes", "Max size (MB)", "", "qt", func(p *broker.Properties) *int64 { return &p.MaxSizeInMegabytes }),
	durationField("defaultMessageTimeToLive", "Message time to live", "", "qts", func(p *broker.Properties) *time.Duration { return &p.DefaultMessageTimeToLive }),
	durationField("lockDuration", "Lock duration", "", "qs", func(p *broker.Properties) *time.Duration { return &p.LockDuration }),
	{
		name: "maxDeliveryCount", label: "Max delivery count", kinds: "qs",
		get: func(p *broker.Properties) string { return fmt.Sprint(p.MaxDeliveryCount) },
		set: func(p *broker.Properties, v string) error {
			n, err := atoi(v)
			if err != nil || n < 1 || n > 1<<31-1 {
				return fmt.Errorf("Max delivery count: want a whole number of at least 1, got %q", v)
			}
			p.MaxDeliveryCount = int32(n)
			return nil
		},
	},
	durationField("duplicateDetectionHistoryTimeWindow", "Duplicate detection window", "Used when duplicate detection is on.", "qt", func(p *broker.Properties) *time.Duration { return &p.DuplicateDetectionHistoryTimeWindow }),
	durationField("autoDeleteOnIdle", "Auto-delete on idle", "", "qts", func(p *broker.Properties) *time.Duration { return &p.AutoDeleteOnIdle }),
	intField("maxMessageSizeInKilobytes", "Max message size (KB)", "", "qt", func(p *broker.Properties) *int64 { return &p.MaxMessageSizeInKilobytes }),
	stringField("forwardTo", "Forward to", "Name of a queue or topic.", "qs", func(p *broker.Properties) *string { return &p.ForwardTo }),
	stringField("forwardDeadLetteredMessagesTo", "Forward dead-lettered messages to", "Name of a queue or topic.", "qs", func(p *broker.Properties) *string { return &p.ForwardDeadLetteredMessagesTo }),
	stringField("userMetadata", "User metadata", "", "qts", func(p *broker.Properties) *string { return &p.UserMetadata }),
}

// checkField is a boolean property.
type checkField struct {
	name, label, kinds string
	// immutable properties cannot change once the entity exists.
	immutable bool
	f         func(p *broker.Properties) *bool
}

var checkFields = []checkField{
	{"requiresSession", "Enable sessions", "qs", true, func(p *broker.Properties) *bool { return &p.RequiresSession }},
	{"requiresDuplicateDetection", "Enable duplicate detection", "qt", true, func(p *broker.Properties) *bool { return &p.RequiresDuplicateDetection }},
	{"deadLetteringOnMessageExpiration", "Dead-letter on message expiration", "qs", false, func(p *broker.Properties) *bool { return &p.DeadLetteringOnMessageExpiration }},
	{"deadLetteringOnFilterEvaluationExceptions", "Dead-letter on filter evaluation exceptions", "s", false, func(p *broker.Properties) *bool { return &p.DeadLetteringOnFilterEvaluationExceptions }},
	{"enableBatchedOperations", "Enable batched operations", "qts", false, func(p *broker.Properties) *bool { return &p.EnableBatchedOperations }},
	{"enableExpress", "Enable express", "qt", false, func(p *broker.Properties) *bool { return &p.EnableExpress }},
	{"enablePartitioning", "Enable partitioning", "qt", true, func(p *broker.Properties) *bool { return &p.EnablePartitioning }},
	{"supportOrdering", "Support ordering", "t", false, func(p *broker.Properties) *bool { return &p.SupportOrdering }},
}

var statuses = []string{
	string(broker.StatusActive), string(broker.StatusDisabled),
	string(broker.StatusSendDisabled), string(broker.StatusReceiveDisabled),
}

func appliesTo(kind broker.EntityKind, kinds string) bool {
	return strings.Contains(kinds, string(kind[0]))
}

// formMode says whether a form creates an entity or edits one.
type formMode int

const (
	modeCreate formMode = iota
	modeEdit
)

// formFor builds the form of kind showing props. topic is the parent of a
// subscription, and path the entity being edited.
func formFor(kind broker.EntityKind, mode formMode, name, topic string, props broker.Properties, formErr string) views.EntityForm {
	f := views.EntityForm{
		Kind: string(kind), Name: name, Topic: topic, Error: formErr, Edit: mode == modeEdit,
		Status: string(props.Status), Statuses: statuses,
	}
	switch {
	case mode == modeEdit:
		f.Path = entityPath(kind, topic, name)
		f.Action = Prefix + "entity/edit"
		f.Cancel = EntityURL(f.Path)
	case kind == broker.KindSubscription:
		f.Action = Prefix + "subscriptions"
		f.Cancel = EntityURL(topic)
	default:
		f.Action = Prefix + string(kind) + "s"
		f.Cancel = Prefix + string(kind) + "s"
	}
	for _, t := range textFields {
		if appliesTo(kind, t.kinds) {
			f.Text = append(f.Text, views.Field{Name: t.name, Label: t.label, Help: t.help, Value: t.get(&props)})
		}
	}
	for _, c := range checkFields {
		if appliesTo(kind, c.kinds) {
			f.Checks = append(f.Checks, views.Check{Name: c.name, Label: c.label, On: *c.f(&props), Disabled: mode == modeEdit && c.immutable})
		}
	}
	return f
}

func entityPath(kind broker.EntityKind, topic, name string) string {
	if kind == broker.KindSubscription {
		return broker.SubscriptionPath(topic, name)
	}
	return broker.NormalizePath(name)
}

// EntityURL is the address of the overview page of the entity at path.
func EntityURL(path string) string { return Prefix + "entity?path=" + url.QueryEscape(path) }

// parseProperties applies the posted form to base. Blank numeric and
// duration fields keep the value of base; blank text fields clear it.
// Immutable checkboxes are only read when creating.
func parseProperties(kind broker.EntityKind, mode formMode, base broker.Properties, r *http.Request) (broker.Properties, error) {
	props := base
	var errs []error
	for _, t := range textFields {
		if !appliesTo(kind, t.kinds) {
			continue
		}
		v := r.PostFormValue(t.name)
		if strings.TrimSpace(v) == "" && t.name != "forwardTo" && t.name != "forwardDeadLetteredMessagesTo" && t.name != "userMetadata" {
			continue
		}
		if err := t.set(&props, v); err != nil {
			errs = append(errs, err)
		}
	}
	for _, c := range checkFields {
		if appliesTo(kind, c.kinds) && !(mode == modeEdit && c.immutable) {
			*c.f(&props) = r.PostFormValue(c.name) == "true"
		}
	}
	if s := r.PostFormValue("status"); s != "" {
		props.Status = broker.Status(s)
	}
	return props, errors.Join(errs...)
}

func (h *Handler) newForm(kind broker.EntityKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		f := formFor(kind, modeCreate, "", topic, broker.DefaultProperties(kind), "")
		h.render(w, r, http.StatusOK, views.EntityFormPage(h.layout("Create "+string(kind), navFor(kind)), f))
	}
}

func navFor(kind broker.EntityKind) views.Nav {
	if kind == broker.KindTopic {
		return views.NavTopics
	}
	return views.NavQueues
}

// retype shows what was typed in the fields of f, not what failed to parse.
func retype(f *views.EntityForm, r *http.Request) {
	for i, t := range f.Text {
		if v, ok := r.PostForm[t.Name]; ok && len(v) > 0 {
			f.Text[i].Value = v[0]
		}
	}
}

func (h *Handler) create(kind broker.EntityKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimSpace(r.PostFormValue("name"))
		topic := r.PostFormValue("topic")
		props, err := parseProperties(kind, modeCreate, broker.DefaultProperties(kind), r)
		if err == nil {
			ctx := broker.WithActor(r.Context(), Actor)
			switch kind {
			case broker.KindTopic:
				_, err = h.opts.Broker.CreateTopic(ctx, name, props)
			case broker.KindSubscription:
				_, err = h.opts.Broker.CreateSubscription(ctx, topic, name, props)
			default:
				_, err = h.opts.Broker.CreateQueue(ctx, name, props)
			}
		}
		if err != nil {
			f := formFor(kind, modeCreate, name, topic, props, strings.TrimPrefix(err.Error(), "broker: "))
			retype(&f, r)
			h.render(w, r, http.StatusUnprocessableEntity, views.EntityFormPage(h.layout("Create "+string(kind), navFor(kind)), f))
			return
		}
		if kind == broker.KindSubscription {
			http.Redirect(w, r, EntityURL(entityPath(kind, topic, name)), http.StatusSeeOther)
			return
		}
		http.Redirect(w, r, Prefix+string(kind)+"s", http.StatusSeeOther)
	}
}

// editForm shows the properties of the entity at ?path=.
func (h *Handler) editForm(w http.ResponseWriter, r *http.Request) {
	e, err := h.opts.Broker.Entity(r.URL.Query().Get("path"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	f := formFor(e.Kind, modeEdit, e.Name, topicPath(e), e.Properties, "")
	h.render(w, r, http.StatusOK, views.EntityFormPage(h.layout("Properties of "+e.DisplayPath(), navFor(e.Kind)), f))
}

func topicPath(e *broker.Entity) string {
	if e.Topic != nil {
		return e.Topic.Path
	}
	return ""
}

// update saves the posted properties of an entity, keeping the ones the
// form does not carry, as the management API's update does.
func (h *Handler) update(w http.ResponseWriter, r *http.Request) {
	path := r.PostFormValue("path")
	e, err := h.opts.Broker.Entity(path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	props, err := parseProperties(e.Kind, modeEdit, e.Properties, r)
	if err == nil {
		_, err = h.opts.Broker.UpdateEntity(broker.WithActor(r.Context(), Actor), path, props)
	}
	if err != nil {
		f := formFor(e.Kind, modeEdit, e.Name, topicPath(e), props, strings.TrimPrefix(err.Error(), "broker: "))
		retype(&f, r)
		h.render(w, r, http.StatusUnprocessableEntity, views.EntityFormPage(h.layout("Properties of "+e.DisplayPath(), navFor(e.Kind)), f))
		return
	}
	http.Redirect(w, r, EntityURL(path), http.StatusSeeOther)
}
