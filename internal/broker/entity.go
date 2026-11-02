package broker

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// Errors reported by broker operations. Protocol layers map them to wire
// error conditions.
var (
	ErrEntityNotFound   = errors.New("broker: entity not found")
	ErrEntityExists     = errors.New("broker: entity already exists")
	ErrEntityDisabled   = errors.New("broker: entity is disabled")
	ErrInvalidOperation = errors.New("broker: invalid operation")
	ErrLockLost         = errors.New("broker: lock lost")
	ErrMessageNotFound  = errors.New("broker: message not found")
	ErrSessionRequired  = errors.New("broker: session id required")
	ErrQuotaExceeded    = errors.New("broker: entity size quota exceeded")
	ErrMessageTooLarge  = errors.New("broker: message too large")
)

// touchTx records activity on e and, for a subscription, on its topic:
// operations on a subscription keep the topic alive too.
func touchTx(tx *store.Tx, e *Entity, at int64) error {
	if err := tx.TouchEntity(e.id, at); err != nil {
		return err
	}
	if e.Topic != nil {
		return tx.TouchEntity(e.Topic.id, at)
	}
	return nil
}

// entityGone maps a write to a concurrently deleted entity to
// ErrEntityNotFound.
func entityGone(err error, e *Entity) error {
	if errors.Is(err, store.ErrEntityGone) {
		return fmt.Errorf("%w: %s", ErrEntityNotFound, e.DisplayPath())
	}
	return err
}

// Entity is a queue, topic or subscription.
type Entity struct {
	id         int64
	Kind       EntityKind
	Path       string // normalized: lower case, "topic/subscriptions/sub"
	Name       string // as created; for subscriptions, the subscription name
	Topic      *Entity
	Properties Properties
	CreatedAt  time.Time
	UpdatedAt  time.Time
	AccessedAt time.Time
}

// ID returns the store identifier of the entity.
func (e *Entity) ID() int64 { return e.id }

// DisplayPath returns the path with the original casing of the names.
func (e *Entity) DisplayPath() string {
	if e.Topic != nil {
		return e.Topic.Name + "/Subscriptions/" + e.Name
	}
	return e.Name
}

func (e *Entity) clone() *Entity {
	c := *e
	return &c
}

// Subqueue selects the active messages or a dead-letter queue.
type Subqueue int

const (
	SubqueueActive      Subqueue = store.SubqueueActive
	SubqueueDeadLetter  Subqueue = store.SubqueueDeadLetter
	SubqueueTransferDLQ Subqueue = store.SubqueueTransferDLQ
)

// Address is a resolved link address: an entity and one of its sub-queues.
type Address struct {
	Entity   *Entity
	Subqueue Subqueue
}

func (a Address) String() string {
	switch a.Subqueue {
	case SubqueueDeadLetter:
		return a.Entity.DisplayPath() + "/$DeadLetterQueue"
	case SubqueueTransferDLQ:
		return a.Entity.DisplayPath() + "/$Transfer/$DeadLetterQueue"
	}
	return a.Entity.DisplayPath()
}

const (
	dlqSuffix         = "/$deadletterqueue"
	transferDLQSuffix = "/$transfer/$deadletterqueue"
	subscriptionsPart = "/subscriptions/"
)

// NormalizePath returns the canonical form of an entity path. Service Bus
// entity names are case-insensitive.
func NormalizePath(p string) string {
	return strings.ToLower(strings.Trim(p, "/"))
}

// SubscriptionPath returns the normalized path of a subscription.
func SubscriptionPath(topic, sub string) string {
	return NormalizePath(topic) + subscriptionsPart + NormalizePath(sub)
}

// splitAddress separates the entity path from a sub-queue suffix.
func splitAddress(address string) (string, Subqueue) {
	p := NormalizePath(address)
	if s, ok := strings.CutSuffix(p, transferDLQSuffix); ok {
		return s, SubqueueTransferDLQ
	}
	if s, ok := strings.CutSuffix(p, dlqSuffix); ok {
		return s, SubqueueDeadLetter
	}
	return p, SubqueueActive
}

// validateName checks an entity or subscription name.
func validateName(name string, allowSlash bool) error {
	if name == "" {
		return fmt.Errorf("%w: empty name", ErrInvalidOperation)
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.' || r == '~':
		case r == '/' && allowSlash:
		default:
			return fmt.Errorf("%w: invalid character %q in name %q", ErrInvalidOperation, r, name)
		}
	}
	if strings.HasPrefix(name, "$") || strings.Contains(strings.ToLower(name), "/subscriptions/") {
		return fmt.Errorf("%w: reserved name %q", ErrInvalidOperation, name)
	}
	return nil
}
