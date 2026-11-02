package amqpsb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
)

// errBadRequest marks malformed management requests.
var errBadRequest = errors.New("bad request")

func badRequest(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errBadRequest, fmt.Sprintf(format, args...))
}

// management serves the $management node of one entity.
type management struct {
	h    *Handler
	addr broker.Address
}

// managementOp handles one operation. It returns the response body (an
// amqp-value) and status, or an error mapped by rpcStatus.
type managementOp func(ctx context.Context, req *request) (value any, status int32, err error)

// request is a decoded management request.
type request struct {
	body encoding.Map
	// session is the session of the receiver that owns the management
	// link, or "" for links of non-session receivers.
	session string
}

func (m *management) handle(ctx context.Context, req *frames.Message, reply *amqp.SenderLink) *frames.Message {
	op, _ := appProperty(req, "operation")
	ops := map[string]managementOp{
		"com.microsoft:renew-lock":                 m.renewLock,
		"com.microsoft:update-disposition":         m.updateDisposition,
		"com.microsoft:peek-message":               m.peek,
		"com.microsoft:schedule-message":           m.schedule,
		"com.microsoft:cancel-scheduled-message":   m.cancelScheduled,
		"com.microsoft:receive-by-sequence-number": m.receiveBySequence,
		"com.microsoft:renew-session-lock":         m.renewSessionLock,
		"com.microsoft:get-session-state":          m.getSessionState,
		"com.microsoft:set-session-state":          m.setSessionState,
		"com.microsoft:get-message-sessions":       m.listSessions,
		"com.microsoft:add-rule":                   m.addRule,
		"com.microsoft:remove-rule":                m.removeRule,
		"com.microsoft:enumerate-rules":            m.enumerateRules,
		"com.microsoft:batch-delete-messages":      m.batchDelete,
	}
	fn, ok := ops[op]
	if !ok {
		return managementReply(statusNotImpl, fmt.Sprintf("management operation %q is not implemented", op))
	}
	if right := managementRight(op); !m.h.authorized(ctx, m.addr.Entity.Path, right) {
		return managementReply(statusUnauthorized, fmt.Sprintf("Unauthorized access. '%s' claim(s) are required to perform this operation. Resource: '%s'.", right.Names()[len(right.Names())-1], m.addr.Entity.DisplayPath()))
	}
	r := &request{}
	r.body, _ = req.Value.(encoding.Map)
	if name, ok := appProperty(req, "associated-link-name"); ok {
		if id, ok := m.h.linkSessions.Load(name); ok {
			r.session = id.(string)
		}
	}
	if r.session == "" && reply != nil {
		r.session, _ = sessionFilter(reply.Source())
	}
	value, status, err := fn(ctx, r)
	if err != nil {
		m.h.logger().Debug("management operation failed", "op", op, "entity", m.addr.String(), "err", err)
		return managementReply(rpcStatus(err), err.Error())
	}
	resp := managementReply(status, "")
	if value != nil {
		resp.Value, resp.HasValue = value, true
	}
	return resp
}

func (m *management) renewLock(ctx context.Context, req *request) (any, int32, error) {
	body := req.body
	tokens, err := lockTokens(body)
	if err != nil {
		return nil, 0, err
	}
	expirations, err := m.h.Broker.RenewLocks(ctx, m.addr, tokens)
	if err != nil {
		return nil, 0, err
	}
	arr := make(encoding.Array, len(expirations))
	for i, t := range expirations {
		arr[i] = t
	}
	return encoding.Map{{Key: "expirations", Value: arr}}, statusOK, nil
}

func (m *management) updateDisposition(ctx context.Context, req *request) (any, int32, error) {
	body := req.body
	tokens, err := lockTokens(body)
	if err != nil {
		return nil, 0, err
	}
	status, _ := getString(body, "disposition-status")
	reason, _ := getString(body, "deadletter-reason")
	desc, _ := getString(body, "deadletter-description")
	var props encoding.Map
	if v, ok := get(body, "properties-to-modify"); ok {
		pm, ok := v.(encoding.Map)
		if !ok && v != nil {
			return nil, 0, badRequest("properties-to-modify is %T, want map", v)
		}
		props = stringKeyed(pm)
	}
	for _, tok := range tokens {
		switch status {
		case "completed":
			err = m.h.Broker.Complete(ctx, m.addr, tok)
		case "abandoned":
			err = m.h.Broker.Abandon(ctx, m.addr, tok, props)
		case "suspended":
			err = m.h.Broker.DeadLetter(ctx, m.addr, tok, reason, desc, props)
		case "defered", "deferred":
			err = m.h.Broker.Defer(ctx, m.addr, tok, props)
		default:
			return nil, 0, badRequest("unknown disposition-status %q", status)
		}
		if err != nil {
			return nil, 0, err
		}
	}
	return nil, statusOK, nil
}

func (m *management) peek(ctx context.Context, req *request) (any, int32, error) {
	body := req.body
	from, _ := getInt64(body, "from-sequence-number")
	count, _ := getInt64(body, "message-count")
	if count <= 0 {
		count = 1
	}
	ds, err := m.h.Broker.Peek(ctx, m.addr, from, int(count), req.session)
	if err != nil {
		return nil, 0, err
	}
	if len(ds) == 0 {
		return nil, statusNoContent, nil
	}
	list, err := encodeDeliveries(ds, false)
	if err != nil {
		return nil, 0, err
	}
	return encoding.Map{{Key: "messages", Value: list}}, statusOK, nil
}

func (m *management) schedule(ctx context.Context, req *request) (any, int32, error) {
	body := req.body
	v, _ := get(body, "messages")
	entries, ok := v.([]any)
	if !ok {
		return nil, 0, badRequest("messages is %T, want list", v)
	}
	msgs := make([]broker.IncomingMessage, 0, len(entries))
	for i, e := range entries {
		entry, ok := e.(encoding.Map)
		if !ok {
			return nil, 0, badRequest("messages[%d] is %T, want map", i, e)
		}
		raw, ok := get(entry, "message")
		payload, isBytes := raw.([]byte)
		if !ok || !isBytes {
			return nil, 0, badRequest("messages[%d].message is %T, want binary", i, raw)
		}
		im, err := incomingMessage(payload)
		if err != nil {
			return nil, 0, badRequest("messages[%d]: %v", i, err)
		}
		msgs = append(msgs, im)
	}
	seqs, err := m.h.Broker.Send(ctx, m.addr.String(), msgs)
	if err != nil {
		return nil, 0, err
	}
	arr := make(encoding.Array, len(seqs))
	for i, s := range seqs {
		arr[i] = s
	}
	return encoding.Map{{Key: "sequence-numbers", Value: arr}}, statusOK, nil
}

func (m *management) cancelScheduled(ctx context.Context, req *request) (any, int32, error) {
	body := req.body
	seqs, err := sequenceNumbers(body)
	if err != nil {
		return nil, 0, err
	}
	return nil, statusOK, m.h.Broker.CancelScheduled(ctx, m.addr, seqs)
}

// batchDelete serves the batch delete operation, which the SDKs call to
// purge an entity: it deletes up to message-count of the oldest messages
// enqueued before enqueued-time-utc, and answers with how many it deleted.
func (m *management) batchDelete(ctx context.Context, req *request) (any, int32, error) {
	count, ok := getInt64(req.body, "message-count")
	if !ok {
		return nil, 0, badRequest("message-count is required")
	}
	before := time.Now()
	if v, ok := req.body.Get("enqueued-time-utc"); ok {
		t, ok := v.(time.Time)
		if !ok {
			return nil, 0, badRequest("enqueued-time-utc is %T, want a timestamp", v)
		}
		before = t
	}
	n, err := m.h.Broker.DeleteMessages(ctx, m.addr, int(count), before, req.session)
	if err != nil {
		return nil, 0, err
	}
	if n == 0 {
		return nil, statusNoContent, nil
	}
	return encoding.Map{{Key: "message-count", Value: int32(n)}}, statusOK, nil
}

func (m *management) receiveBySequence(ctx context.Context, req *request) (any, int32, error) {
	body := req.body
	seqs, err := sequenceNumbers(body)
	if err != nil {
		return nil, 0, err
	}
	// receiver-settle-mode 1 (second) means peek-lock.
	mode := broker.ReceiveAndDelete
	if v, _ := getInt64(body, "receiver-settle-mode"); v == 1 {
		mode = broker.PeekLock
	}
	ds, err := m.h.Broker.ReceiveDeferred(ctx, m.addr, seqs, mode, req.session)
	if err != nil {
		return nil, 0, err
	}
	if len(ds) == 0 {
		return nil, statusNoContent, nil
	}
	list, err := encodeDeliveries(ds, mode == broker.PeekLock)
	if err != nil {
		return nil, 0, err
	}
	return encoding.Map{{Key: "messages", Value: list}}, statusOK, nil
}

// sessionID returns the session a session operation applies to.
func sessionID(req *request) (string, error) {
	if id, ok := getString(req.body, "session-id"); ok && id != "" {
		return id, nil
	}
	if req.session != "" {
		return req.session, nil
	}
	return "", badRequest("missing session-id")
}

func (m *management) renewSessionLock(ctx context.Context, req *request) (any, int32, error) {
	id, err := sessionID(req)
	if err != nil {
		return nil, 0, err
	}
	until, err := m.h.Broker.RenewSessionLock(ctx, m.addr, id)
	if err != nil {
		return nil, 0, err
	}
	return encoding.Map{{Key: "expiration", Value: until}}, statusOK, nil
}

func (m *management) getSessionState(ctx context.Context, req *request) (any, int32, error) {
	id, err := sessionID(req)
	if err != nil {
		return nil, 0, err
	}
	state, err := m.h.Broker.SessionState(ctx, m.addr, id)
	if err != nil {
		return nil, 0, err
	}
	// The key is always present: some clients cannot type an empty map
	// as string-keyed. A null value means no state.
	var v any
	if state != nil {
		v = state
	}
	return encoding.Map{{Key: "session-state", Value: v}}, statusOK, nil
}

func (m *management) setSessionState(ctx context.Context, req *request) (any, int32, error) {
	id, err := sessionID(req)
	if err != nil {
		return nil, 0, err
	}
	var state []byte
	if v, ok := get(req.body, "session-state"); ok && v != nil {
		b, ok := v.([]byte)
		if !ok {
			return nil, 0, badRequest("session-state is %T, want binary", v)
		}
		state = b
	}
	return nil, statusOK, m.h.Broker.SetSessionState(ctx, m.addr, id, state)
}

// listAllSessions is the last-updated-time clients send to list every
// session with messages: year 10000, which .NET clamps to DateTime.MaxValue.
var listAllSessions = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

func (m *management) listSessions(ctx context.Context, req *request) (any, int32, error) {
	var after time.Time
	if v, ok := get(req.body, "last-updated-time"); ok {
		if t, ok := v.(time.Time); ok && t.Before(listAllSessions) {
			after = t
		}
	}
	skip, _ := getInt64(req.body, "skip")
	top, ok := getInt64(req.body, "top")
	if !ok || top <= 0 {
		top = 100
	}
	ids, err := m.h.Broker.ListSessions(ctx, m.addr, after, int(skip), int(top))
	if err != nil {
		return nil, 0, err
	}
	if len(ids) == 0 {
		return nil, statusNoContent, nil
	}
	arr := make(encoding.Array, len(ids))
	for i, id := range ids {
		arr[i] = id
	}
	return encoding.Map{
		{Key: "skip", Value: int32(skip) + int32(len(ids))},
		{Key: "sessions-ids", Value: arr},
	}, statusOK, nil
}

// encodeDeliveries builds the "messages" list of peek and receive
// responses: maps holding the encoded message and, when locked, its lock
// token.
func encodeDeliveries(ds []*broker.Delivery, locked bool) ([]any, error) {
	list := make([]any, 0, len(ds))
	for _, d := range ds {
		payload, err := deliveryPayload(d, locked)
		if err != nil {
			return nil, err
		}
		entry := encoding.Map{{Key: "message", Value: payload}}
		if locked {
			entry = append(entry, encoding.MapEntry{Key: "lock-token", Value: encoding.UUID(d.LockToken)})
		}
		list = append(list, entry)
	}
	return list, nil
}

func get(m encoding.Map, key string) (any, bool) {
	if v, ok := m.Get(key); ok {
		return v, true
	}
	return m.Get(encoding.Symbol(key))
}

func getString(m encoding.Map, key string) (string, bool) {
	v, ok := get(m, key)
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

func getInt64(m encoding.Map, key string) (int64, bool) {
	v, ok := get(m, key)
	if !ok {
		return 0, false
	}
	switch v := v.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int16:
		return int64(v), true
	case int8:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint64:
		return int64(v), true
	}
	return 0, false
}

func lockTokens(body encoding.Map) ([]broker.LockToken, error) {
	v, ok := get(body, "lock-tokens")
	if !ok {
		return nil, badRequest("missing lock-tokens")
	}
	var items []any
	switch v := v.(type) {
	case encoding.Array:
		items = v
	case []any:
		items = v
	case encoding.UUID:
		items = []any{v}
	default:
		return nil, badRequest("lock-tokens is %T, want array of uuid", v)
	}
	out := make([]broker.LockToken, len(items))
	for i, item := range items {
		u, ok := item.(encoding.UUID)
		if !ok {
			return nil, badRequest("lock-tokens[%d] is %T, want uuid", i, item)
		}
		out[i] = broker.LockToken(u)
	}
	return out, nil
}

func sequenceNumbers(body encoding.Map) ([]int64, error) {
	v, ok := get(body, "sequence-numbers")
	if !ok {
		return nil, badRequest("missing sequence-numbers")
	}
	var items []any
	switch v := v.(type) {
	case encoding.Array:
		items = v
	case []any:
		items = v
	case int64:
		items = []any{v}
	default:
		return nil, badRequest("sequence-numbers is %T, want array of long", v)
	}
	out := make([]int64, len(items))
	for i, item := range items {
		n, ok := item.(int64)
		if !ok {
			return nil, badRequest("sequence-numbers[%d] is %T, want long", i, item)
		}
		out[i] = n
	}
	return out, nil
}
