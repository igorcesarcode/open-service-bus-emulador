package amqp

import (
	"context"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
)

// Handler decides how links attached by peers behave.
//
// Both methods run on the connection's frame loop, so they must not block
// for long; slow work belongs in the returned MessageHandler or in
// goroutines that call SenderLink.Send.
type Handler interface {
	// AttachReceiver is called when the peer attaches a link to send
	// messages to this server. Returning an error refuses the link; a
	// *frames.Error controls the condition reported to the peer.
	AttachReceiver(ctx context.Context, l *ReceiverLink) (MessageHandler, error)

	// AttachSender is called when the peer attaches a link to receive
	// messages from this server. The handler may adjust l.Source before
	// returning; the adjusted terminus is echoed to the peer. Returning an
	// error refuses the link. Returning ErrAttachPending defers the answer:
	// the handler later calls l.CompleteAttach from another goroutine,
	// while the connection keeps serving other links.
	AttachSender(ctx context.Context, l *SenderLink) error
}

// MessageHandler processes the deliveries of one receiving link. Calls for
// a link are sequential and in delivery order. The returned state is sent
// to the peer as the delivery outcome unless the peer settled it already.
type MessageHandler interface {
	HandleMessage(ctx context.Context, d *Delivery) frames.DeliveryState
}

// MessageHandlerFunc adapts a function to MessageHandler.
type MessageHandlerFunc func(ctx context.Context, d *Delivery) frames.DeliveryState

// HandleMessage calls f(ctx, d).
func (f MessageHandlerFunc) HandleMessage(ctx context.Context, d *Delivery) frames.DeliveryState {
	return f(ctx, d)
}

// Delivery is a complete message received on a link.
type Delivery struct {
	ID      uint32
	Tag     []byte
	Format  uint32
	Settled bool
	// State is the delivery state the peer sent with the transfer: a
	// *frames.TransactionalState for a delivery that belongs to a
	// transaction, or nil.
	State frames.DeliveryState
	// Payload holds the encoded message: the concatenation of the payloads
	// of every transfer frame of the delivery.
	Payload []byte

	link *ReceiverLink
	// oversized marks a delivery past the link's maximum message size,
	// whose payload is dropped.
	oversized bool
}

// Link returns the link the delivery arrived on.
func (d *Delivery) Link() *ReceiverLink { return d.link }

// Message decodes the payload.
func (d *Delivery) Message() (*frames.Message, error) {
	return frames.UnmarshalMessage(d.Payload)
}
