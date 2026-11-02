package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// Change is one step of an Apply: what was done to an entity, or, in a dry
// run, what would be.
type Change struct {
	// Action is create, update, delete or unchanged.
	Action string
	Kind   broker.EntityKind
	// Path is the entity path with the original casing, such as
	// "orders" or "events/Subscriptions/billing".
	Path string
	// Detail names what differs, for an update.
	Detail string
}

// Apply makes the namespace hold the declared queues and topics: it creates
// what is missing and updates what differs, leaving everything else alone.
// With prune it also deletes the queues, topics and subscriptions that are
// not declared. With dryRun it changes nothing and reports what it would do.
// Apply stops at the first failure and returns the changes made until then.
func (n *Namespace) Apply(ctx context.Context, queues []Queue, topics []Topic, prune, dryRun bool) ([]Change, error) {
	var out []Change
	b := n.broker
	declared := map[string]bool{}

	// plan reports how the entity of kind at path becomes props.
	plan := func(kind broker.EntityKind, name, topic string, props broker.Properties) (Change, error) {
		if props == (broker.Properties{}) {
			props = broker.DefaultProperties(kind)
		}
		path := name
		display := name
		if kind == broker.KindSubscription {
			path = broker.SubscriptionPath(topic, name)
			display = topic + "/Subscriptions/" + name
		}
		declared[broker.NormalizePath(path)] = true
		c := Change{Kind: kind, Path: display}
		e, err := b.Entity(path)
		switch {
		case err != nil:
			c.Action = "create"
			if dryRun {
				return c, nil
			}
			switch kind {
			case broker.KindQueue:
				_, err = b.CreateQueue(ctx, name, props)
			case broker.KindTopic:
				_, err = b.CreateTopic(ctx, name, props)
			default:
				_, err = b.CreateSubscription(ctx, topic, name, props)
			}
			return c, err
		case e.Kind != kind:
			return c, fmt.Errorf("%s %s already exists as a %s", kind, name, e.Kind)
		case reflect.DeepEqual(e.Properties, props):
			c.Action = "unchanged"
			return c, nil
		}
		c.Action, c.Detail = "update", differing(e.Properties, props)
		if dryRun {
			return c, nil
		}
		_, err = b.UpdateEntity(ctx, path, props)
		return c, err
	}

	for _, q := range queues {
		c, err := plan(broker.KindQueue, q.Name, "", q.Properties)
		out = append(out, c)
		if err != nil {
			return out, fmt.Errorf("queue %s: %w", q.Name, err)
		}
	}
	for _, t := range topics {
		c, err := plan(broker.KindTopic, t.Name, "", t.Properties)
		out = append(out, c)
		if err != nil {
			return out, fmt.Errorf("topic %s: %w", t.Name, err)
		}
		for _, s := range t.Subscriptions {
			c, err := plan(broker.KindSubscription, s.Name, t.Name, s.Properties)
			out = append(out, c)
			if err != nil {
				return out, fmt.Errorf("subscription %s/%s: %w", t.Name, s.Name, err)
			}
			if s.Rules == nil {
				continue
			}
			path := broker.SubscriptionPath(t.Name, s.Name)
			cur, err := b.Rules(path)
			if err != nil && !dryRun {
				return out, err
			}
			if err == nil && sameRules(cur, s.Rules) {
				continue
			}
			out = append(out, Change{Action: "update", Kind: broker.KindSubscription, Path: c.Path, Detail: "rules"})
			if !dryRun {
				if err := b.SetRules(ctx, path, s.Rules); err != nil {
					return out, fmt.Errorf("subscription %s/%s: %w", t.Name, s.Name, err)
				}
			}
		}
	}
	if !prune {
		return out, nil
	}
	// Subscriptions go before their topics: deleting a topic takes them
	// along, and then they would be deleted twice.
	for _, kind := range []broker.EntityKind{broker.KindSubscription, broker.KindQueue, broker.KindTopic} {
		for _, e := range b.Entities(kind, "") {
			if declared[e.Path] {
				continue
			}
			c := Change{Action: "delete", Kind: kind, Path: e.DisplayPath()}
			out = append(out, c)
			if dryRun {
				continue
			}
			if err := b.DeleteEntity(ctx, e.Path); err != nil && !errors.Is(err, broker.ErrEntityNotFound) {
				return out, fmt.Errorf("%s %s: %w", kind, e.DisplayPath(), err)
			}
		}
	}
	return out, nil
}

// sameRules reports whether two rule lists hold the same rules in the same
// order.
func sameRules(a, b []broker.Rule) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		x.CreatedAt, y.CreatedAt = time.Time{}, time.Time{}
		if !reflect.DeepEqual(x, y) {
			return false
		}
	}
	return true
}

// differing names the properties in which a and b differ.
func differing(a, b broker.Properties) string {
	var names []string
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	for i := range va.NumField() {
		if !va.Field(i).Equal(vb.Field(i)) {
			names = append(names, va.Type().Field(i).Name)
		}
	}
	return strings.Join(names, ", ")
}

// Reset deletes every queue, topic and subscription, and with them their
// messages, then declares again what the server started with. It stops at
// the first entity a lock protects. Metrics and the activity log stay.
func (n *Namespace) Reset(ctx context.Context, restore bool) error {
	for _, kind := range []broker.EntityKind{broker.KindQueue, broker.KindTopic} {
		for _, e := range n.broker.Entities(kind, "") {
			if err := n.broker.DeleteEntity(ctx, e.Path); err != nil {
				return fmt.Errorf("%s %s: %w", kind, e.DisplayPath(), err)
			}
		}
	}
	if !restore {
		return nil
	}
	if err := n.declare(ctx); err != nil {
		return err
	}
	// Deleting an entity deletes the policies scoped to it.
	for _, p := range n.opts.Policies {
		if err := n.auth.Declare(ctx, p); err != nil {
			return fmt.Errorf("policy %s: %w", p.Name, err)
		}
	}
	return nil
}

// DBPath returns the SQLite file of the namespace, or "in memory".
func (n *Namespace) DBPath() string {
	if n.dbPath == store.Memory {
		return "in memory"
	}
	return n.dbPath
}

// Version returns the version the server reports.
func (s *Server) Version() string {
	if s.opts.Version == "" {
		return "dev"
	}
	return s.opts.Version
}

// Started returns when the server was created.
func (s *Server) Started() time.Time { return s.started }

// ConnectionStringFor returns the development connection string of the
// namespace, for the SDKs.
func (s *Server) ConnectionStringFor(n *Namespace) string {
	return s.connectionStringFor(n, s.Addr())
}
