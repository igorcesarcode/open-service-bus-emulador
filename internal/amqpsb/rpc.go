package amqpsb

import (
	"context"
	"log/slog"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
)

// rpcNode serves a request/response node such as $cbs. Requests arrive on
// the link the client sends on; each reply goes to the client's receiving
// link whose target address equals the request's reply-to, with the
// request's message-id as correlation-id.
type rpcNode struct {
	name string
	// handle serves a request. reply is the link the response goes to; its
	// source carries the session filter of session receivers.
	handle func(ctx context.Context, req *frames.Message, reply *amqp.SenderLink) *frames.Message
	log    *slog.Logger
}

// HandleMessage implements amqp.MessageHandler.
func (n *rpcNode) HandleMessage(ctx context.Context, d *amqp.Delivery) frames.DeliveryState {
	req, err := d.Message()
	if err != nil {
		return &frames.Rejected{Error: amqp.NewError(amqp.CondDecodeError, err.Error())}
	}
	if req.Properties == nil || req.Properties.ReplyTo == "" {
		return &frames.Rejected{Error: amqp.NewError(amqp.CondInvalidField, "request has no reply-to")}
	}
	reply := d.Link().Session().ReplyLink(req.Properties.ReplyTo)
	if reply == nil {
		return &frames.Rejected{Error: amqp.NewError(amqp.CondNotFound,
			"no link attached for reply-to "+req.Properties.ReplyTo)}
	}

	resp := n.handle(ctx, req, reply)
	if resp.Properties == nil {
		resp.Properties = &frames.Properties{}
	}
	resp.Properties.CorrelationID = req.Properties.MessageID
	payload, err := frames.MarshalMessage(resp)
	if err != nil {
		n.log.Error("encoding rpc reply", "node", n.name, "err", err)
		return &frames.Rejected{Error: amqp.NewError(amqp.CondInternalError, err.Error())}
	}
	if err := reply.Send(ctx, payload, amqp.SendOptions{Settled: true}); err != nil {
		n.log.Debug("sending rpc reply", "node", n.name, "err", err)
		return &frames.Released{}
	}
	return &frames.Accepted{}
}
