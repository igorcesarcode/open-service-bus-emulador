package mcp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

const propsDesc = "Entity properties by their JSON names, for example lockDuration (ISO 8601 such as PT1M, or seconds), maxDeliveryCount, " +
	"defaultMessageTimeToLive, requiresSession, requiresDuplicateDetection, deadLetteringOnMessageExpiration, forwardTo, status. " +
	"Nothing is limited to the ranges of the official emulator. Properties left out keep the Service Bus defaults (create) or their current value (update)."

type (
	listEntitiesIn struct {
		Common
		Kind  string `json:"kind,omitempty" desc:"Only this kind of entity." enum:"queue,topic,subscription"`
		Topic string `json:"topic,omitempty" desc:"Only the subscriptions of this topic."`
		Query string `json:"query,omitempty" desc:"Only entities whose path contains this text."`
	}
	pathIn struct {
		Common
		Path string `json:"path" desc:"Entity path: a queue or topic name, or topic/Subscriptions/name." required:"true"`
	}
	createQueueIn struct {
		Common
		Name        string         `json:"name" desc:"Queue name." required:"true"`
		Properties  map[string]any `json:"properties,omitempty" desc:"Queue properties. See the tool description."`
		IfNotExists bool           `json:"if_not_exists,omitempty" desc:"Succeed without changing anything when the queue exists."`
	}
	createTopicIn struct {
		Common
		Name        string         `json:"name" desc:"Topic name." required:"true"`
		Properties  map[string]any `json:"properties,omitempty" desc:"Topic properties. See the tool description."`
		IfNotExists bool           `json:"if_not_exists,omitempty" desc:"Succeed without changing anything when the topic exists."`
	}
	createSubscriptionIn struct {
		Common
		Topic       string         `json:"topic" desc:"Topic the subscription belongs to." required:"true"`
		Name        string         `json:"name" desc:"Subscription name." required:"true"`
		Properties  map[string]any `json:"properties,omitempty" desc:"Subscription properties. See the tool description."`
		IfNotExists bool           `json:"if_not_exists,omitempty" desc:"Succeed without changing anything when the subscription exists."`
	}
	updateEntityIn struct {
		Common
		Path       string         `json:"path" desc:"Entity path." required:"true"`
		Properties map[string]any `json:"properties" desc:"Properties to change; the others keep their value." required:"true"`
	}
	deleteEntityIn struct {
		Common
		Confirm
		Path string `json:"path" desc:"Entity path. Deleting a topic deletes its subscriptions, and every message of what is deleted goes with it." required:"true"`
	}
)

func (s *Server) registerEntityTools() {
	add(s, "list_entities", "Lists queues, topics and subscriptions with their message counts and status.", read,
		func(ctx context.Context, in listEntitiesIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			out := []any{}
			for _, kind := range []broker.EntityKind{broker.KindQueue, broker.KindTopic, broker.KindSubscription} {
				if in.Kind != "" && in.Kind != string(kind) {
					continue
				}
				for _, e := range n.Broker().Entities(kind, "") {
					if in.Topic != "" && (e.Topic == nil || !strings.EqualFold(e.Topic.Name, in.Topic)) {
						continue
					}
					if in.Query != "" && !strings.Contains(e.Path, strings.ToLower(in.Query)) {
						continue
					}
					v, err := s.entityView(ctx, n, e, false)
					if err != nil {
						return nil, err
					}
					out = append(out, v)
				}
			}
			return map[string]any{"namespace": n.Name(), "count": len(out), "items": out}, nil
		})

	add(s, "get_entity", "Returns every property of a queue, topic or subscription, with its message counts and, for a subscription, its rules.", read,
		func(ctx context.Context, in pathIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			e, err := n.Broker().Entity(in.Path)
			if err != nil {
				return nil, err
			}
			return s.entityView(ctx, n, e, true)
		})

	add(s, "create_queue", "Creates a queue. "+propsDesc, write,
		func(ctx context.Context, in createQueueIn) (any, error) {
			return s.create(ctx, in.Namespace, broker.KindQueue, "", in.Name, in.Properties, in.IfNotExists)
		})
	add(s, "create_topic", "Creates a topic. "+propsDesc, write,
		func(ctx context.Context, in createTopicIn) (any, error) {
			return s.create(ctx, in.Namespace, broker.KindTopic, "", in.Name, in.Properties, in.IfNotExists)
		})
	add(s, "create_subscription", "Creates a subscription of a topic, with the $Default rule that selects every message. "+propsDesc, write,
		func(ctx context.Context, in createSubscriptionIn) (any, error) {
			return s.create(ctx, in.Namespace, broker.KindSubscription, in.Topic, in.Name, in.Properties, in.IfNotExists)
		})

	add(s, "update_entity", "Changes properties of a queue, topic or subscription. "+propsDesc, write,
		func(ctx context.Context, in updateEntityIn) (any, error) {
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			e, err := n.Broker().Entity(in.Path)
			if err != nil {
				return nil, err
			}
			props := e.Properties
			if err := applyProps(&props, in.Properties); err != nil {
				return nil, err
			}
			if _, err := n.Broker().UpdateEntity(ctx, in.Path, props); err != nil {
				return nil, err
			}
			e, err = n.Broker().Entity(in.Path)
			if err != nil {
				return nil, err
			}
			return s.entityView(ctx, n, e, true)
		})

	add(s, "delete_entity", "Deletes a queue, topic or subscription and all its messages. Resource locks of the namespace apply.", destroy,
		func(ctx context.Context, in deleteEntityIn) (any, error) {
			if err := in.check(); err != nil {
				return nil, err
			}
			n, err := s.namespace(in.Namespace)
			if err != nil {
				return nil, err
			}
			e, err := n.Broker().Entity(in.Path)
			if err != nil {
				return nil, err
			}
			deleted := []string{e.DisplayPath()}
			if e.Kind == broker.KindTopic {
				for _, sub := range n.Broker().Entities(broker.KindSubscription, e.Name) {
					deleted = append(deleted, sub.DisplayPath())
				}
			}
			if err := n.Broker().DeleteEntity(ctx, in.Path); err != nil {
				return nil, err
			}
			return map[string]any{"deleted": deleted}, nil
		})
}

func (s *Server) create(ctx context.Context, ns string, kind broker.EntityKind, topic, name string, in map[string]any, ifNotExists bool) (any, error) {
	n, err := s.namespace(ns)
	if err != nil {
		return nil, err
	}
	b := n.Broker()
	path := name
	if kind == broker.KindSubscription {
		path = broker.SubscriptionPath(topic, name)
	}
	if e, err := b.Entity(path); err == nil && ifNotExists {
		if e.Kind != kind {
			return nil, fmt.Errorf("%s already exists as a %s", name, e.Kind)
		}
		return s.entityView(ctx, n, e, true)
	}
	props := broker.DefaultProperties(kind)
	if err := applyProps(&props, in); err != nil {
		return nil, err
	}
	switch kind {
	case broker.KindQueue:
		_, err = b.CreateQueue(ctx, name, props)
	case broker.KindTopic:
		_, err = b.CreateTopic(ctx, name, props)
	default:
		_, err = b.CreateSubscription(ctx, topic, name, props)
	}
	if err != nil {
		return nil, err
	}
	e, err := b.Entity(path)
	if err != nil {
		return nil, err
	}
	return s.entityView(ctx, n, e, true)
}

// entityView describes an entity. detail adds the properties and rules.
func (s *Server) entityView(ctx context.Context, n *server.Namespace, e *broker.Entity, detail bool) (map[string]any, error) {
	b := n.Broker()
	v := map[string]any{
		"path": e.DisplayPath(), "kind": e.Kind, "status": e.Properties.Status,
		"createdAt": e.CreatedAt.UTC().Format(time.RFC3339), "updatedAt": e.UpdatedAt.UTC().Format(time.RFC3339),
	}
	c, err := b.Counts(ctx, e.Path)
	if err != nil {
		return nil, err
	}
	v["counts"] = map[string]any{
		"active": c.Active, "deadLetter": c.DeadLetter, "transferDeadLetter": c.TransferDeadLetter,
		"scheduled": c.Scheduled, "deferred": c.Deferred, "sizeBytes": c.SizeBytes,
	}
	if e.Kind == broker.KindTopic {
		v["subscriptions"] = len(b.Entities(broker.KindSubscription, e.Name))
	}
	if !detail {
		return v, nil
	}
	v["properties"] = propsView(e.Properties, e.Kind)
	if e.Kind == broker.KindSubscription {
		rules, err := b.Rules(e.Path)
		if err != nil {
			return nil, err
		}
		views := make([]any, 0, len(rules))
		for _, r := range rules {
			views = append(views, ruleView(r))
		}
		v["rules"] = views
	}
	return v, nil
}
