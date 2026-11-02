// Package amqpsb maps AMQP links onto Service Bus: entity links go to the
// broker, and the $cbs and $management request/response nodes are served
// here.
package amqpsb

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

const (
	cbsAddress       = "$cbs"
	managementSuffix = "/$management"
)

// Service Bus specific error conditions.
const (
	condEntityDisabled = encoding.Symbol("com.microsoft:entity-disabled")
	condLockLost       = encoding.Symbol("com.microsoft:message-lock-lost")
	condMessageMissing = encoding.Symbol("com.microsoft:message-not-found")
	condDeadLetter     = encoding.Symbol("com.microsoft:dead-letter")
	condSessionLocked  = encoding.Symbol("com.microsoft:session-cannot-be-locked")
	condSessionLost    = encoding.Symbol("com.microsoft:session-lock-lost")
	condTimeout        = encoding.Symbol("com.microsoft:timeout")
	condServerBusy     = encoding.Symbol("com.microsoft:server-busy")
	condEntityExists   = encoding.Symbol("com.microsoft:entity-already-exists")
)

// Handler implements amqp.Handler for a Service Bus namespace.
type Handler struct {
	Broker *broker.Broker
	// MaxBatchSize is advertised to senders as
	// com.microsoft:max-message-batch-size. Zero omits the property.
	MaxBatchSize uint64
	// Auth checks CBS tokens and what they allow. Nil, or an authority
	// whose mode is none, accepts every client.
	Auth *auth.Authority
	// TxnTimeout is how long a transaction may stay open. Zero means
	// DefaultTxnTimeout.
	TxnTimeout time.Duration
	// Logger receives diagnostics. Nil disables logging.
	Logger *slog.Logger

	// linkSessions maps the names of session receiver links to their
	// session id. Management requests name the receiver link they act for
	// in associated-link-name.
	linkSessions sync.Map
}

var _ amqp.Handler = (*Handler)(nil)

func (h *Handler) logger() *slog.Logger {
	if h.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return h.Logger
}

// AttachReceiver implements amqp.Handler for links on which clients send.
func (h *Handler) AttachReceiver(ctx context.Context, l *amqp.ReceiverLink) (_ amqp.MessageHandler, err error) {
	ctx, done := telemetry.Operation(ctx, "amqp.attach", attribute.String("osb.link.role", "sender-to-broker"), telemetry.Entity(l.Address()))
	defer func() { done(attachErr(err)) }()
	if l.Coordinator() != nil {
		return &coordinator{h: h}, nil
	}
	addr := l.Address()
	switch {
	case addr == cbsAddress:
		return &rpcNode{name: cbsAddress, handle: func(ctx context.Context, req *frames.Message, _ *amqp.SenderLink) *frames.Message {
			return h.handleCBS(ctx, req)
		}, log: h.logger()}, nil
	case strings.HasSuffix(addr, managementSuffix):
		target, err := h.Broker.Resolve(strings.TrimSuffix(addr, managementSuffix))
		if err != nil {
			return nil, amqpError(err)
		}
		if !h.authorizedAny(ctx, target.Entity.Path) {
			return nil, unauthorized(target.Entity.DisplayPath(), "Listen, Send or Manage")
		}
		m := &management{h: h, addr: target}
		return &rpcNode{name: addr, handle: m.handle, log: h.logger()}, nil
	}
	target, err := h.Broker.Resolve(addr)
	if err != nil {
		return nil, amqpError(err)
	}
	if !h.authorized(ctx, target.Entity.Path, auth.Send) {
		return nil, unauthorized(target.Entity.DisplayPath(), "Send")
	}
	if target.Subqueue != broker.SubqueueActive || target.Entity.Kind == broker.KindSubscription {
		return nil, amqp.NewError(amqp.CondNotAllowed, fmt.Sprintf("cannot send to %s", target))
	}
	// The SDKs size messages and batches from these limits, so they follow
	// the entity's MaxMessageSizeInKilobytes when it is lower.
	batch := h.MaxBatchSize
	if kb := target.Entity.Properties.MaxMessageSizeInKilobytes; kb > 0 {
		if entityLimit := uint64(kb) * 1024; batch == 0 || entityLimit < batch {
			batch = entityLimit
			l.MaxMessageSize = entityLimit
		}
	}
	if batch > 0 {
		l.Properties = encoding.Map{{Key: encoding.Symbol("com.microsoft:max-message-batch-size"), Value: batch}}
	}
	// A send-via link: the messages are for the destination the link names,
	// and go through the entity the link is attached to.
	via := ""
	if v, ok := l.RemoteAttach().Properties.Get(transferDestination); ok {
		if dest, _ := v.(string); dest != "" {
			via, addr = target.Entity.Path, dest
			if !h.authorized(ctx, auth.EntityFromResource(dest), auth.Send) {
				return nil, unauthorized(dest, "Send")
			}
		}
	}
	return amqp.MessageHandlerFunc(func(ctx context.Context, d *amqp.Delivery) frames.DeliveryState {
		return h.enqueue(ctx, addr, via, d)
	}), nil
}

// AttachSender implements amqp.Handler for links on which clients receive.
func (h *Handler) AttachSender(ctx context.Context, l *amqp.SenderLink) (err error) {
	ctx, done := telemetry.Operation(ctx, "amqp.attach", attribute.String("osb.link.role", "receiver-from-broker"), telemetry.Entity(l.Address()))
	defer func() { done(attachErr(err)) }()
	addr := l.Address()
	switch {
	case addr == cbsAddress:
		return nil
	case strings.HasSuffix(addr, managementSuffix):
		if _, err := h.Broker.Resolve(strings.TrimSuffix(addr, managementSuffix)); err != nil {
			return amqpError(err)
		}
		return nil
	}
	target, err := h.Broker.Resolve(addr)
	if err != nil {
		return amqpError(err)
	}
	if !h.authorized(ctx, target.Entity.Path, auth.Listen) {
		return unauthorized(target.Entity.DisplayPath(), "Listen")
	}
	if target.Entity.Kind == broker.KindTopic {
		return amqp.NewError(amqp.CondNotAllowed, fmt.Sprintf("cannot receive from topic %s; receive from a subscription", target))
	}
	if fwd := target.Entity.Properties.ForwardTo; fwd != "" && target.Subqueue == broker.SubqueueActive {
		return amqp.NewError(amqp.CondNotAllowed, fmt.Sprintf(
			"cannot receive from %s, which forwards its messages to %s", target, fwd))
	}
	mode := broker.PeekLock
	remote := l.RemoteAttach()
	if remote.ReceiverSettle() == frames.ReceiverSettleModeFirst && remote.SenderSettle() == frames.SenderSettleModeSettled {
		mode = broker.ReceiveAndDelete
	}
	l.ManualDrain = true
	r := &receiver{h: h, link: l, addr: target, mode: mode}

	sessionID, sessionful := sessionFilter(l.Source())
	sessionEntity := target.Entity.Properties.RequiresSession && target.Subqueue == broker.SubqueueActive
	switch {
	case sessionful && !sessionEntity:
		return amqp.NewError(amqp.CondNotAllowed, fmt.Sprintf(
			"It is not possible for an entity that does not require sessions to create a sessionful message receiver. Entity: %s", target))
	case !sessionful && sessionEntity:
		return amqp.NewError(amqp.CondNotAllowed, fmt.Sprintf(
			"It is not possible for an entity that requires sessions to create a non-sessionful message receiver. Entity: %s", target))
	case sessionful:
		// Locking a session may have to wait for one to become available,
		// so the attach completes in the background.
		go r.acceptSession(sessionID, acceptTimeout(remote))
		return amqp.ErrAttachPending
	}
	go r.run(l.Context())
	return nil
}

const (
	sessionFilterName = encoding.Symbol("com.microsoft:session-filter")
	sessionFilterCode = uint64(0x00000137000000C)
)

// sessionFilter reports whether src requests a session receiver and which
// session; an empty id asks for the next available session.
func sessionFilter(src *frames.Source) (string, bool) {
	if src == nil {
		return "", false
	}
	v, ok := src.Filter.Get(sessionFilterName)
	if !ok {
		return "", false
	}
	if d, ok := v.(encoding.Described); ok {
		v = d.Value
	}
	id, _ := v.(string)
	return id, true
}

// setSessionFilter reports the locked session in src, in the encoding the
// client used: the Go SDK sends a described value and requires one back,
// while the JavaScript SDK sends and compares a plain string.
func setSessionFilter(src *frames.Source, id string) {
	v, _ := src.Filter.Get(sessionFilterName)
	if _, described := v.(encoding.Described); described {
		src.Filter.Set(sessionFilterName, encoding.Described{Descriptor: sessionFilterCode, Value: id})
		return
	}
	src.Filter.Set(sessionFilterName, id)
}

// defaultAcceptTimeout is how long AcceptNextSession waits when the client
// sets no timeout, matching Service Bus.
const defaultAcceptTimeout = time.Minute

// acceptTimeout returns how long to wait for a session, from the
// com.microsoft:timeout link property. Clients derive that property from
// their own deadline, so the server answers a little earlier: a reply
// arriving after the deadline would be lost.
func acceptTimeout(a *frames.Attach) time.Duration {
	v, ok := a.Properties.Get(encoding.Symbol("com.microsoft:timeout"))
	if !ok {
		return defaultAcceptTimeout
	}
	var ms int64
	switch v := v.(type) {
	case uint32:
		ms = int64(v)
	case uint64:
		ms = int64(min(v, 1<<40))
	case int32:
		ms = int64(v)
	case int64:
		ms = v
	default:
		return defaultAcceptTimeout
	}
	d := time.Duration(ms) * time.Millisecond
	return d - min(d/10, time.Second)
}

// enqueue is the transfer of a message (or a batch) from a client: it is
// the span that the broker's send hangs from.
func (h *Handler) enqueue(ctx context.Context, addr, via string, d *amqp.Delivery) (state frames.DeliveryState) {
	if telemetry.Enabled() {
		// A client that traces its sends puts the trace context in the
		// message (Azure SDKs: Diagnostic-Id); continue that trace.
		ctx = telemetry.ExtractTraceparent(ctx, diagnosticID(d))
	}
	ctx, done := telemetry.Operation(ctx, "amqp.transfer", telemetry.Entity(addr),
		attribute.Int("osb.bytes", len(d.Payload)), attribute.Bool("osb.batch", d.Format == frames.MessageFormatBatch))
	var terr error
	defer func() {
		if r, ok := state.(*frames.Rejected); ok && r.Error != nil {
			terr = r.Error
		}
		done(&terr)
	}()
	payloads := [][]byte{d.Payload}
	if d.Format == frames.MessageFormatBatch {
		// Each data section of a batch holds one complete encoded message.
		batch, err := frames.UnmarshalMessage(d.Payload)
		if err != nil {
			return &frames.Rejected{Error: amqp.NewError(amqp.CondDecodeError, err.Error())}
		}
		payloads = batch.Data
	}
	msgs := make([]broker.IncomingMessage, 0, len(payloads))
	for _, p := range payloads {
		im, err := incomingMessage(p)
		if err != nil {
			return &frames.Rejected{Error: amqp.NewError(amqp.CondDecodeError, err.Error())}
		}
		msgs = append(msgs, im)
	}
	if state, ok := d.State.(*frames.TransactionalState); ok {
		// The messages wait in the transaction until it commits.
		target, err := h.Broker.Resolve(addr)
		if err != nil {
			return &frames.Rejected{Error: amqpError(err)}
		}
		return h.enlistSend(ctx, state, target.Entity, via, msgs)
	}
	if via != "" {
		return &frames.Rejected{Error: amqp.NewError(amqp.CondNotAllowed, "a send-via link sends only inside a transaction")}
	}
	if _, err := h.Broker.Send(ctx, addr, msgs); err != nil {
		return &frames.Rejected{Error: amqpError(err)}
	}
	return &frames.Accepted{}
}

// amqpError maps broker errors to AMQP error conditions.
func amqpError(err error) *frames.Error {
	var fe *frames.Error
	if errors.As(err, &fe) {
		return fe
	}
	cond := amqp.CondInternalError
	switch {
	case errors.Is(err, broker.ErrEntityNotFound), errors.Is(err, broker.ErrRuleNotFound):
		cond = amqp.CondNotFound
	case errors.Is(err, broker.ErrEntityExists):
		cond = condEntityExists
	case errors.Is(err, broker.ErrEntityDisabled):
		cond = condEntityDisabled
	case errors.Is(err, broker.ErrLockLost):
		cond = condLockLost
	case errors.Is(err, broker.ErrMessageNotFound):
		cond = condMessageMissing
	case errors.Is(err, broker.ErrSessionCannotBeLocked):
		cond = condSessionLocked
	case errors.Is(err, broker.ErrSessionLockLost):
		cond = condSessionLost
	case errors.Is(err, broker.ErrNoSessionAvailable):
		cond = condTimeout
	case errors.Is(err, broker.ErrSessionRequired), errors.Is(err, broker.ErrInvalidOperation):
		cond = amqp.CondNotAllowed
	case errors.Is(err, broker.ErrQuotaExceeded):
		cond = amqp.CondResourceLimitExceeded
	case errors.Is(err, broker.ErrMessageTooLarge):
		cond = amqp.CondMessageSizeExceeded
	case errors.Is(err, store.ErrThrottled):
		cond = condServerBusy
	}
	return amqp.NewError(cond, err.Error())
}

// Status codes used in request/response replies.
const (
	statusOK           int32 = 200
	statusAccepted     int32 = 202
	statusNoContent    int32 = 204
	statusBadRequest   int32 = 400
	statusUnauthorized int32 = 401
	statusNotFound     int32 = 404
	statusForbidden    int32 = 403
	statusConflict     int32 = 409
	statusTooLarge     int32 = 413
	statusServerBusy   int32 = 503
	statusGone         int32 = 410
	statusInternal     int32 = 500
	statusNotImpl      int32 = 501
)

// rpcStatus maps a broker error to a request/response status code.
func rpcStatus(err error) int32 {
	switch {
	case errors.Is(err, broker.ErrLockLost), errors.Is(err, broker.ErrSessionLockLost),
		errors.Is(err, broker.ErrSessionCannotBeLocked):
		return statusGone
	case errors.Is(err, broker.ErrEntityNotFound), errors.Is(err, broker.ErrMessageNotFound),
		errors.Is(err, broker.ErrRuleNotFound):
		return statusNotFound
	case errors.Is(err, broker.ErrEntityExists):
		return statusConflict
	case errors.Is(err, broker.ErrQuotaExceeded), errors.Is(err, broker.ErrReadOnly):
		return statusForbidden
	case errors.Is(err, broker.ErrMessageTooLarge):
		return statusTooLarge
	case errors.Is(err, store.ErrThrottled):
		return statusServerBusy
	case errors.Is(err, broker.ErrInvalidOperation), errors.Is(err, broker.ErrSessionRequired),
		errors.Is(err, broker.ErrEntityDisabled), errors.Is(err, errBadRequest):
		return statusBadRequest
	}
	return statusInternal
}

// handleCBS serves the claims-based security node.
func (h *Handler) handleCBS(ctx context.Context, req *frames.Message) *frames.Message {
	op, _ := appProperty(req, "operation")
	if op != "put-token" {
		return statusReply(statusBadRequest, fmt.Sprintf("unsupported $cbs operation %q", op))
	}
	tokenType, _ := appProperty(req, "type")
	audience, _ := appProperty(req, "name")
	// Like Service Bus, reject tokens for entities that do not exist; this is
	// how clients learn that a queue or topic is missing.
	if entity := audienceEntity(audience); entity != "" && !h.Broker.Exists(entity) {
		return statusReply(statusNotFound, fmt.Sprintf("The messaging entity '%s' could not be found.", entity))
	}
	if h.Auth.Enabled() {
		claims, err := h.Auth.Authenticate(ctx, tokenType, audience, req.Value)
		if err != nil {
			return statusReply(statusUnauthorized, err.Error())
		}
		h.claimsOf(ctx).put(audience, claims)
	}
	h.logger().Debug("cbs token accepted", "audience", audience, "type", tokenType)
	return statusReply(statusAccepted, "Accepted")
}

// audienceEntity extracts the entity path from a CBS audience such as
// "amqp://host/queue" or "sb://host/topic/$management". It returns "" for
// namespace-wide audiences.
func audienceEntity(audience string) string {
	path := audience
	if i := strings.Index(path, "://"); i >= 0 {
		path = path[i+3:]
		if j := strings.IndexByte(path, '/'); j >= 0 {
			path = path[j+1:]
		} else {
			path = ""
		}
	}
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	path = strings.TrimSuffix(strings.Trim(path, "/"), managementSuffix)
	return strings.Trim(path, "/")
}

func appProperty(m *frames.Message, name string) (string, bool) {
	v, ok := m.ApplicationProperties.Get(name)
	if !ok {
		return "", false
	}
	switch v := v.(type) {
	case string:
		return v, true
	case encoding.Symbol:
		return string(v), true
	}
	return "", false
}

// statusReply is a $cbs response.
func statusReply(code int32, description string) *frames.Message {
	return &frames.Message{
		ApplicationProperties: encoding.Map{
			{Key: "status-code", Value: code},
			{Key: "status-description", Value: description},
		},
	}
}

// managementReply is a $management response. Service Bus names its status
// properties in camel case there, and the JavaScript SDK reads them by that
// name to recognize a 204 with no body.
func managementReply(code int32, description string) *frames.Message {
	return &frames.Message{
		ApplicationProperties: encoding.Map{
			{Key: "statusCode", Value: code},
			{Key: "statusDescription", Value: description},
		},
	}
}

// attachErr is the error to report for an attach: a pending attach, which
// completes in the background, is not a failure.
func attachErr(err error) *error {
	if errors.Is(err, amqp.ErrAttachPending) {
		err = nil
	}
	return &err
}

func init() {
	// A peer that went away is not a failure of the emulator.
	telemetry.AddClassifier(func(err error) string {
		if errors.Is(err, amqp.ErrLinkDetached) || errors.Is(err, amqp.ErrConnClosed) || errors.Is(err, amqp.ErrServerClosed) {
			return "canceled"
		}
		return ""
	})
}

// diagnosticID returns the trace context a client put in the first message of
// a transfer, or "".
func diagnosticID(d *amqp.Delivery) string {
	payload := d.Payload
	if d.Format == frames.MessageFormatBatch {
		batch, err := frames.UnmarshalMessage(payload)
		if err != nil || len(batch.Data) == 0 {
			return ""
		}
		payload = batch.Data[0]
	}
	m, err := frames.UnmarshalMessage(payload)
	if err != nil || m.ApplicationProperties == nil {
		return ""
	}
	for _, name := range []string{"Diagnostic-Id", "traceparent"} {
		if v, ok := appProperty(m, name); ok {
			return v
		}
	}
	return ""
}
