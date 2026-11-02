package broker

import (
	"fmt"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
)

// Infinite is the duration Service Bus reports as TimeSpan.MaxValue: a time
// to live or idle interval that never elapses.
const Infinite = duration.Infinite

// EntityKind distinguishes queues, topics and subscriptions.
type EntityKind string

const (
	KindQueue        EntityKind = "queue"
	KindTopic        EntityKind = "topic"
	KindSubscription EntityKind = "subscription"
)

// Status is the availability of an entity.
type Status string

const (
	StatusActive          Status = "Active"
	StatusDisabled        Status = "Disabled"
	StatusSendDisabled    Status = "SendDisabled"
	StatusReceiveDisabled Status = "ReceiveDisabled"
)

// Properties holds every configurable property of a queue, topic or
// subscription. Each kind uses the subset Service Bus defines for it.
// Unlike the official emulator, no value is capped: any duration or count
// that Service Bus accepts is accepted here.
type Properties struct {
	LockDuration                              time.Duration `json:"lockDuration"`
	MaxSizeInMegabytes                        int64         `json:"maxSizeInMegabytes"`
	RequiresDuplicateDetection                bool          `json:"requiresDuplicateDetection"`
	RequiresSession                           bool          `json:"requiresSession"`
	DefaultMessageTimeToLive                  time.Duration `json:"defaultMessageTimeToLive"`
	DeadLetteringOnMessageExpiration          bool          `json:"deadLetteringOnMessageExpiration"`
	DeadLetteringOnFilterEvaluationExceptions bool          `json:"deadLetteringOnFilterEvaluationExceptions"`
	DuplicateDetectionHistoryTimeWindow       time.Duration `json:"duplicateDetectionHistoryTimeWindow"`
	MaxDeliveryCount                          int32         `json:"maxDeliveryCount"`
	EnableBatchedOperations                   bool          `json:"enableBatchedOperations"`
	Status                                    Status        `json:"status"`
	AutoDeleteOnIdle                          time.Duration `json:"autoDeleteOnIdle"`
	EnablePartitioning                        bool          `json:"enablePartitioning"`
	EnableExpress                             bool          `json:"enableExpress"`
	SupportOrdering                           bool          `json:"supportOrdering"`
	ForwardTo                                 string        `json:"forwardTo,omitempty"`
	ForwardDeadLetteredMessagesTo             string        `json:"forwardDeadLetteredMessagesTo,omitempty"`
	MaxMessageSizeInKilobytes                 int64         `json:"maxMessageSizeInKilobytes"`
	UserMetadata                              string        `json:"userMetadata,omitempty"`
}

// DefaultProperties returns the defaults Service Bus applies to a new entity
// of the given kind.
func DefaultProperties(kind EntityKind) Properties {
	p := Properties{
		LockDuration:                        time.Minute,
		MaxSizeInMegabytes:                  1024,
		DefaultMessageTimeToLive:            Infinite,
		DuplicateDetectionHistoryTimeWindow: 10 * time.Minute,
		MaxDeliveryCount:                    10,
		EnableBatchedOperations:             true,
		Status:                              StatusActive,
		AutoDeleteOnIdle:                    Infinite,
		MaxMessageSizeInKilobytes:           1024,
	}
	switch kind {
	case KindTopic:
		p.SupportOrdering = true
	case KindSubscription:
		p.DeadLetteringOnFilterEvaluationExceptions = true
	}
	return p
}

// Validate reports properties Service Bus itself would reject. It does not
// impose the official emulator's limits.
func (p *Properties) Validate(kind EntityKind) error {
	switch {
	case kind != KindTopic && p.LockDuration <= 0:
		return fmt.Errorf("lock duration must be positive, got %v", p.LockDuration)
	case p.DefaultMessageTimeToLive <= 0:
		return fmt.Errorf("default message time to live must be positive, got %v", p.DefaultMessageTimeToLive)
	case kind != KindTopic && p.MaxDeliveryCount < 1:
		return fmt.Errorf("max delivery count must be at least 1, got %d", p.MaxDeliveryCount)
	case p.RequiresDuplicateDetection && p.DuplicateDetectionHistoryTimeWindow <= 0:
		return fmt.Errorf("duplicate detection window must be positive, got %v", p.DuplicateDetectionHistoryTimeWindow)
	case p.EnableExpress && p.RequiresDuplicateDetection && kind != KindSubscription:
		return fmt.Errorf("Sub code=40000. Both DelayedPersistence and RequiresDuplicateDetection property can't be enabled together.")
	case p.AutoDeleteOnIdle <= 0:
		return fmt.Errorf("auto delete on idle must be positive, got %v", p.AutoDeleteOnIdle)
	}
	if p.RequiresSession && p.ForwardTo != "" {
		return fmt.Errorf("an entity that requires sessions cannot forward messages")
	}
	switch p.Status {
	case StatusActive, StatusDisabled, StatusSendDisabled, StatusReceiveDisabled:
	default:
		return fmt.Errorf("unknown entity status %q", p.Status)
	}
	return nil
}
