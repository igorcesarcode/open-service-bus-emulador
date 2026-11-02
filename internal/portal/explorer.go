package portal

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/message"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

const (
	defaultPeekCount = 100
	maxPeekCount     = 1000
	// maxRepeatWait bounds how long one Send request may sleep between
	// repeated sends.
	maxRepeatWait = 30 * time.Second
	maxRepeat     = 1000
)

func (h *Handler) registerExplorer() {
	h.mux.HandleFunc("GET "+Prefix+"explorer", h.explorerPage)
	h.mux.HandleFunc("GET "+Prefix+"explorer/view", h.explorerView)
	h.mux.HandleFunc("POST "+Prefix+"explorer/view", h.explorerView)
	h.mux.HandleFunc("GET "+Prefix+"explorer/detail", h.explorerDetail)
	h.mux.HandleFunc("POST "+Prefix+"explorer/settle", h.explorerSettle)
	h.mux.HandleFunc("POST "+Prefix+"explorer/resend", h.explorerResend)
	h.mux.HandleFunc("POST "+Prefix+"explorer/purge", h.explorerPurge)
	h.mux.HandleFunc("GET "+Prefix+"explorer/send", h.sendPanel)
	h.mux.HandleFunc("POST "+Prefix+"explorer/send", h.sendMessages)
	h.mux.HandleFunc("GET "+Prefix+"explorer/export", h.explorerExport)
	h.mux.HandleFunc("GET "+Prefix+"explorer/body", h.explorerBody)
}

// subqueueSuffix maps the Explorer's queue key to an address suffix.
func subqueueSuffix(queue string) string {
	switch queue {
	case "dlq":
		return "/$deadletterqueue"
	case "tdlq":
		return "/$transfer/$deadletterqueue"
	}
	return ""
}

func (h *Handler) address(path, queue string) (broker.Address, error) {
	return h.opts.Broker.Resolve(path + subqueueSuffix(queue))
}

// held is a received message carried between requests.
type held struct {
	seq    int64
	token  broker.LockToken
	locked time.Time
}

func (x held) String() string {
	return fmt.Sprintf("%d:%s:%d", x.seq, x.token, x.locked.UnixMilli())
}

func parseHeld(s string) (held, bool) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return held{}, false
	}
	seq, err1 := strconv.ParseInt(parts[0], 10, 64)
	ms, err2 := strconv.ParseInt(parts[2], 10, 64)
	tok, err3 := parseToken(parts[1])
	if err1 != nil || err2 != nil || err3 != nil {
		return held{}, false
	}
	return held{seq, tok, time.UnixMilli(ms).UTC()}, true
}

func formInt(r *http.Request, name string, def int64) int64 {
	if n, err := strconv.ParseInt(strings.TrimSpace(r.FormValue(name)), 10, 64); err == nil {
		return n
	}
	return def
}

// explorerState reads the Explorer's position from a request.
type explorerState struct {
	e     *broker.Entity
	queue string
	mode  string
	count int
	utc   bool
}

func (h *Handler) readState(w http.ResponseWriter, r *http.Request) *explorerState {
	e := h.entityFromRequest(w, r)
	if e == nil {
		return nil
	}
	if e.Kind == broker.KindTopic {
		http.Error(w, "a topic holds no messages", http.StatusBadRequest)
		return nil
	}
	st := &explorerState{e: e, queue: r.FormValue("queue"), mode: r.FormValue("mode")}
	if st.queue != "dlq" && st.queue != "tdlq" {
		st.queue = "active"
	}
	if st.mode != "receive" {
		st.mode = "peek"
	}
	st.count = int(min(max(formInt(r, "count", defaultPeekCount), 1), maxPeekCount))
	st.utc = r.FormValue("utc") != ""
	return st
}

func (h *Handler) tabs(ctx context.Context, e *broker.Entity, queue string) ([]views.ExplorerTab, int64, error) {
	c, err := h.opts.Broker.Counts(ctx, e.Path)
	if err != nil {
		return nil, 0, err
	}
	counts := map[string]int64{"active": c.Active + c.Scheduled + c.Deferred, "dlq": c.DeadLetter, "tdlq": c.TransferDeadLetter}
	tabs := []views.ExplorerTab{
		{Key: "active", Label: kindLabel(e), Count: counts["active"]},
		{Key: "dlq", Label: "Dead-letter", Count: counts["dlq"]},
		{Key: "tdlq", Label: "Transfer dead-letter", Count: counts["tdlq"]},
	}
	for i := range tabs {
		tabs[i].Active = tabs[i].Key == queue
	}
	return tabs, counts[queue], nil
}

func kindLabel(e *broker.Entity) string {
	if e.Kind == broker.KindSubscription {
		return "Subscription"
	}
	return "Queue"
}

// explorerData builds the Explorer for a request. op is "start", "next",
// "opts", "receive" or "" (keep the current position).
func (h *Handler) explorerData(r *http.Request, st *explorerState, op string, extra *explorerExtra) views.Explorer {
	ctx := r.Context()
	e := st.e
	ex := views.Explorer{
		Path: e.Path, Name: e.DisplayPath(), Kind: string(e.Kind), Mode: st.mode, Queue: st.queue,
		Count: st.count, UTC: st.utc, CanDeadLetter: st.queue == "active", Selected: formInt(r, "selected", 0),
	}
	if e.Topic != nil {
		ex.TopicPath, ex.TopicName = e.Topic.Path, e.Topic.Name
	}
	if extra != nil {
		ex.Message, ex.Error = extra.message, extra.err
	}
	var err error
	if ex.Tabs, ex.Total, err = h.tabs(ctx, e, st.queue); err != nil {
		ex.Error = err.Error()
		return ex
	}
	addr, err := h.address(e.Path, st.queue)
	if err != nil {
		ex.Error = err.Error()
		return ex
	}
	add := func(d *broker.Delivery) {
		x, err := decode(d)
		if err != nil {
			ex.Rows = append(ex.Rows, views.MessageRow{Seq: d.SequenceNumber, Text: "cannot decode: " + err.Error()})
			return
		}
		row := x.row(h.opts.Location, st.utc)
		if d.LockToken != (broker.LockToken{}) {
			row.Held = held{d.SequenceNumber, d.LockToken, d.LockedUntil}.String()
		}
		ex.Rows = append(ex.Rows, row)
	}
	if st.mode == "receive" {
		var fresh []*broker.Delivery
		if op == "receive" {
			mode := broker.PeekLock
			if r.FormValue("settle") == "delete" {
				mode = broker.ReceiveAndDelete
			}
			ds, err := h.opts.Broker.Receive(ctx, addr, st.count, mode)
			if err != nil {
				ex.Error = strings.TrimPrefix(err.Error(), "broker: ")
			}
			if mode == broker.ReceiveAndDelete {
				for _, d := range ds {
					h.opts.Broker.Complete(ctx, addr, d.LockToken)
				}
				ex.Message = fmt.Sprintf("Received and deleted %d messages.", len(ds))
				fresh = ds
				for _, d := range fresh {
					d.LockToken, d.LockedUntil = broker.LockToken{}, time.Time{}
				}
			} else {
				fresh = ds
				if err == nil {
					ex.Message = fmt.Sprintf("Received %d messages. They stay locked until they are settled or their lock expires.", len(ds))
				}
			}
		}
		// Messages received earlier, still held by this page.
		var keep []held
		for _, v := range r.Form["held"] {
			if x, ok := parseHeld(v); ok && !extra.settled(x.token) {
				keep = append(keep, x)
			}
		}
		for _, x := range keep {
			ds, err := h.opts.Broker.Peek(ctx, addr, x.seq, 1, "")
			if err != nil || len(ds) == 0 || ds[0].SequenceNumber != x.seq {
				continue // The lock expired and the message moved on.
			}
			ds[0].LockToken, ds[0].LockedUntil = x.token, x.locked
			add(ds[0])
		}
		for _, d := range fresh {
			add(d)
		}
		ex.Shown = len(ex.Rows)
		return ex
	}
	from := int64(0)
	switch op {
	case "next":
		from = formInt(r, "next", 0)
	case "opts", "":
		from = formInt(r, "from", 0)
	}
	ex.From = from
	ds, err := h.opts.Broker.Peek(ctx, addr, from, st.count, "")
	if err != nil {
		ex.Error = strings.TrimPrefix(err.Error(), "broker: ")
	}
	for _, d := range ds {
		add(d)
		ex.Next = d.SequenceNumber + 1
	}
	if len(ds) == 0 {
		ex.Next = from
	}
	ex.Shown = len(ex.Rows)
	return ex
}

// explorerExtra carries what an action reports back into the view.
type explorerExtra struct {
	message, err string
	done         map[broker.LockToken]bool
}

func (x *explorerExtra) settled(t broker.LockToken) bool {
	return x != nil && x.done[t]
}

func (h *Handler) explorerPage(w http.ResponseWriter, r *http.Request) {
	e := h.entityFromRequest(w, r)
	if e == nil {
		return
	}
	if e.Kind == broker.KindTopic {
		rows, _ := h.subscriptionRows(r.Context(), e.Path, "")
		h.render(w, r, http.StatusOK, views.ExplorerPage(h.layout("Explorer: "+e.DisplayPath(), navFor(e.Kind)),
			views.Explorer{Path: e.Path, Name: e.DisplayPath(), Kind: "topic", Subscriptions: rows}))
		return
	}
	st := h.readState(w, r)
	if st == nil {
		return
	}
	ex := h.explorerData(r, st, "start", nil)
	h.render(w, r, http.StatusOK, views.ExplorerPage(h.layout("Explorer: "+e.DisplayPath(), navFor(e.Kind)), ex))
}

func (h *Handler) explorerView(w http.ResponseWriter, r *http.Request) {
	st := h.readState(w, r)
	if st == nil {
		return
	}
	op := r.FormValue("op")
	if op == "receive" && r.Method != http.MethodPost {
		http.Error(w, "receiving changes messages; use POST", http.StatusMethodNotAllowed)
		return
	}
	h.render(w, r, http.StatusOK, views.ExplorerBody(h.explorerData(r, st, op, nil)))
}

// peekSeq returns the message with the given sequence number.
func (h *Handler) peekSeq(ctx context.Context, addr broker.Address, seq int64) (*broker.Delivery, error) {
	ds, err := h.opts.Broker.Peek(ctx, addr, seq, 1, "")
	if err != nil {
		return nil, err
	}
	if len(ds) == 0 || ds[0].SequenceNumber != seq {
		return nil, fmt.Errorf("message %d is no longer there", seq)
	}
	return ds[0], nil
}

func (h *Handler) explorerDetail(w http.ResponseWriter, r *http.Request) {
	st := h.readState(w, r)
	if st == nil {
		return
	}
	addr, err := h.address(st.e.Path, st.queue)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	seq := formInt(r, "seq", 0)
	d, err := h.peekSeq(r.Context(), addr, seq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	for _, v := range r.Form["held"] {
		if x, ok := parseHeld(v); ok && x.seq == seq {
			d.LockToken, d.LockedUntil = x.token, x.locked
		}
	}
	x, err := decode(d)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, r, http.StatusOK, views.DetailPanel(x.detail(h.opts.Location)))
}

func (h *Handler) explorerSettle(w http.ResponseWriter, r *http.Request) {
	st := h.readState(w, r)
	if st == nil {
		return
	}
	st.mode = "receive"
	addr, err := h.address(st.e.Path, st.queue)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	action := r.FormValue("action")
	extra := &explorerExtra{done: map[broker.LockToken]bool{}}
	tokens := r.Form["token"]
	if len(tokens) == 0 {
		extra.err = "Select the messages to " + action + "."
	}
	var ok, failed int
	var firstErr error
	for _, s := range tokens {
		tok, err := parseToken(s)
		if err == nil {
			ctx := r.Context()
			switch action {
			case "complete":
				err = h.opts.Broker.Complete(ctx, addr, tok)
			case "abandon":
				err = h.opts.Broker.Abandon(ctx, addr, tok, nil)
			case "defer":
				err = h.opts.Broker.Defer(ctx, addr, tok, nil)
			case "deadletter":
				err = h.opts.Broker.DeadLetter(ctx, addr, tok, r.FormValue("reason"), r.FormValue("description"), nil)
			default:
				err = fmt.Errorf("unknown action %q", action)
			}
		}
		if err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		ok++
		extra.done[tok] = true
	}
	if ok > 0 {
		extra.message = fmt.Sprintf("%s: %d messages.", strings.ToUpper(action[:1])+action[1:], ok)
	}
	if failed > 0 {
		extra.err = fmt.Sprintf("%d messages failed: %s", failed, strings.TrimPrefix(firstErr.Error(), "broker: "))
	}
	h.render(w, r, http.StatusOK, views.ExplorerBody(h.explorerData(r, st, "", extra)))
}

// sendTarget is where a message sent from the Explorer of e goes: the
// entity, or the topic of a subscription.
func sendTarget(e *broker.Entity) *broker.Entity {
	if e.Topic != nil {
		return e.Topic
	}
	return e
}

func (h *Handler) explorerResend(w http.ResponseWriter, r *http.Request) {
	st := h.readState(w, r)
	if st == nil {
		return
	}
	addr, err := h.address(st.e.Path, st.queue)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	target := sendTarget(st.e)
	extra := &explorerExtra{}
	sent := 0
	for _, s := range r.Form["seq"] {
		seq, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			continue
		}
		d, err := h.peekSeq(r.Context(), addr, seq)
		if err == nil {
			var in broker.IncomingMessage
			if in, err = message.Resend(d); err == nil {
				_, err = h.opts.Broker.Send(r.Context(), target.Path, []broker.IncomingMessage{in})
			}
		}
		if err != nil {
			extra.err = fmt.Sprintf("Message %d: %s", seq, strings.TrimPrefix(err.Error(), "broker: "))
			break
		}
		sent++
	}
	if sent == 0 && extra.err == "" {
		extra.err = "Select the messages to send again."
	}
	if sent > 0 {
		extra.message = fmt.Sprintf("Sent %d messages to %s.", sent, target.DisplayPath())
	}
	h.render(w, r, http.StatusOK, views.ExplorerBody(h.explorerData(r, st, "", extra)))
}

var contentTypes = []string{"application/json", "text/plain", "application/xml", "application/octet-stream"}
var propTypes = []string{"string", "int", "long", "double", "boolean", "uuid", "datetime"}

func sendFields(f sendForm) []views.Field {
	return []views.Field{
		{Name: "messageId", Label: "Message ID", Value: f.MessageID, Help: "Left empty, an id is generated."},
		{Name: "sessionId", Label: "Session ID", Value: f.SessionID},
		{Name: "correlationId", Label: "Correlation ID", Value: f.CorrelationID},
		{Name: "subject", Label: "Subject (label)", Value: f.Subject},
		{Name: "replyTo", Label: "Reply to", Value: f.ReplyTo},
		{Name: "replyToSessionId", Label: "Reply to session ID", Value: f.ReplyToSessionID},
		{Name: "to", Label: "To", Value: f.To},
		{Name: "partitionKey", Label: "Partition key", Value: f.PartitionKey},
		{Name: "ttl", Label: "Time to live", Value: f.TTL, Help: "ISO 8601 such as PT1H, or seconds."},
		{Name: "scheduled", Label: "Scheduled enqueue time (UTC)", Value: f.Scheduled, Help: "Such as 2026-01-31T14:30."},
	}
}

func (h *Handler) sendPanelFor(e *broker.Entity, f sendForm, errMsg string) views.SendPanel {
	custom := f.Custom
	if len(custom) == 0 {
		custom = []views.Prop{{Type: "string"}}
	}
	return views.SendPanel{
		Path: e.Path, Name: sendTarget(e).DisplayPath(), ContentTypes: contentTypes, ContentType: f.ContentType,
		Body: f.Body, Fields: sendFields(f), Custom: custom, Types: propTypes, Error: errMsg,
		Repeat: max(f.Repeat, 1), IntervalMs: f.IntervalMs,
	}
}

func (h *Handler) sendPanel(w http.ResponseWriter, r *http.Request) {
	e := h.entityFromRequest(w, r)
	if e == nil {
		return
	}
	h.render(w, r, http.StatusOK, views.SendPanelView(h.sendPanelFor(e, sendForm{ContentType: "application/json", Body: "{\n  \n}"}, "")))
}

func readSendForm(r *http.Request) sendForm {
	f := sendForm{
		ContentType: r.PostFormValue("contentType"), Body: r.PostFormValue("body"),
		MessageID: r.PostFormValue("messageId"), SessionID: r.PostFormValue("sessionId"),
		CorrelationID: r.PostFormValue("correlationId"), Subject: r.PostFormValue("subject"),
		ReplyTo: r.PostFormValue("replyTo"), ReplyToSessionID: r.PostFormValue("replyToSessionId"),
		To: r.PostFormValue("to"), PartitionKey: r.PostFormValue("partitionKey"),
		TTL: r.PostFormValue("ttl"), Scheduled: r.PostFormValue("scheduled"),
	}
	f.Repeat = int(formInt(r, "repeat", 1))
	f.IntervalMs = int(formInt(r, "interval", 0))
	keys, types, values := r.PostForm["pkey"], r.PostForm["ptype"], r.PostForm["pvalue"]
	for i, k := range keys {
		p := views.Prop{Key: strings.TrimSpace(k)}
		if i < len(types) {
			p.Type = types[i]
		}
		if i < len(values) {
			p.Value = values[i]
		}
		f.Custom = append(f.Custom, p)
	}
	return f
}

func (h *Handler) sendMessages(w http.ResponseWriter, r *http.Request) {
	e := h.entityFromRequest(w, r)
	if e == nil {
		return
	}
	f := readSendForm(r)
	fail := func(msg string) {
		h.render(w, r, http.StatusUnprocessableEntity, views.SendPanelView(h.sendPanelFor(e, f, msg)))
	}
	if strings.Contains(f.ContentType, "json") {
		var v any
		if err := json.Unmarshal([]byte(f.Body), &v); err != nil {
			fail("The body is not valid JSON: " + err.Error())
			return
		}
	}
	if f.Repeat < 1 || f.Repeat > maxRepeat {
		fail(fmt.Sprintf("Repeat send: want between 1 and %d.", maxRepeat))
		return
	}
	interval := time.Duration(max(f.IntervalMs, 0)) * time.Millisecond
	if interval*time.Duration(f.Repeat-1) > maxRepeatWait {
		fail(fmt.Sprintf("The repeated sends would take more than %v.", maxRepeatWait))
		return
	}
	target := sendTarget(e)
	for i := 0; i < f.Repeat; i++ {
		in, err := f.build()
		if err == nil {
			// Each repeat is a new message, so each gets its own id unless
			// the form fixes one.
			_, err = h.opts.Broker.Send(r.Context(), target.Path, []broker.IncomingMessage{in})
		}
		if err != nil {
			msg := fmt.Sprintf("%s", strings.TrimPrefix(err.Error(), "broker: "))
			if i > 0 {
				msg = fmt.Sprintf("Sent %d of %d, then: %s", i, f.Repeat, msg)
			}
			fail(msg)
			return
		}
		if i < f.Repeat-1 && interval > 0 {
			select {
			case <-time.After(interval):
			case <-r.Context().Done():
				return
			}
		}
	}
	w.Header().Set("HX-Trigger", "refresh-explorer")
	h.render(w, r, http.StatusOK, views.SendDone(h.sendPanelFor(e, f, ""), fmt.Sprintf("Sent %d messages to %s.", f.Repeat, target.DisplayPath())))
}

// selectedOrPeeked returns the messages an export or download covers: the
// selected sequence numbers, or the current peek window.
func (h *Handler) selectedOrPeeked(r *http.Request, st *explorerState) ([]*decoded, error) {
	addr, err := h.address(st.e.Path, st.queue)
	if err != nil {
		return nil, err
	}
	var out []*decoded
	add := func(d *broker.Delivery) error {
		x, err := decode(d)
		if err != nil {
			return err
		}
		out = append(out, x)
		return nil
	}
	if seqs := r.Form["seq"]; len(seqs) > 0 {
		for _, s := range seqs {
			seq, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				continue
			}
			d, err := h.peekSeq(r.Context(), addr, seq)
			if err != nil {
				return nil, err
			}
			if err := add(d); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	ds, err := h.opts.Broker.Peek(r.Context(), addr, formInt(r, "from", 0), st.count, "")
	if err != nil {
		return nil, err
	}
	for _, d := range ds {
		if err := add(d); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (h *Handler) explorerExport(w http.ResponseWriter, r *http.Request) {
	st := h.readState(w, r)
	if st == nil {
		return
	}
	msgs, err := h.selectedOrPeeked(r, st)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	name := strings.NewReplacer("/", "_").Replace(st.e.Path) + "-" + st.queue
	type record struct {
		SequenceNumber int64          `json:"sequenceNumber"`
		MessageID      string         `json:"messageId"`
		EnqueuedTime   string         `json:"enqueuedTimeUtc"`
		DeliveryCount  int            `json:"deliveryCount"`
		State          string         `json:"state"`
		Subject        string         `json:"subject,omitempty"`
		SessionID      string         `json:"sessionId,omitempty"`
		ContentType    string         `json:"contentType,omitempty"`
		Properties     map[string]any `json:"applicationProperties,omitempty"`
		Body           string         `json:"body"`
	}
	recs := make([]record, len(msgs))
	for i, x := range msgs {
		p := x.props()
		text, _ := bodyText(x.body)
		rec := record{
			SequenceNumber: x.d.SequenceNumber, MessageID: x.messageID(), EnqueuedTime: x.d.EnqueuedAt.UTC().Format(time.RFC3339Nano),
			DeliveryCount: x.d.DeliveryCount, State: stateNames[x.d.State], Subject: p.Subject, SessionID: x.d.SessionID,
			ContentType: string(p.ContentType), Body: text,
		}
		if len(x.msg.ApplicationProperties) > 0 {
			rec.Properties = message.Jsonable(x.msg.ApplicationProperties).(map[string]any)
		}
		recs[i] = rec
	}
	if r.FormValue("format") == "csv" {
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name+".csv"))
		cw := csv.NewWriter(w)
		cw.Write([]string{"sequenceNumber", "messageId", "enqueuedTimeUtc", "deliveryCount", "state", "subject", "sessionId", "contentType", "applicationProperties", "body"})
		for _, rec := range recs {
			props, _ := json.Marshal(rec.Properties)
			if rec.Properties == nil {
				props = nil
			}
			cw.Write([]string{strconv.FormatInt(rec.SequenceNumber, 10), rec.MessageID, rec.EnqueuedTime, strconv.Itoa(rec.DeliveryCount),
				rec.State, rec.Subject, rec.SessionID, rec.ContentType, string(props), rec.Body})
		}
		cw.Flush()
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name+".json"))
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(recs)
}

func (h *Handler) explorerBody(w http.ResponseWriter, r *http.Request) {
	st := h.readState(w, r)
	if st == nil {
		return
	}
	msgs, err := h.selectedOrPeeked(r, st)
	if err != nil || len(msgs) != 1 || len(r.Form["seq"]) != 1 {
		http.Error(w, "select exactly one message to download its body", http.StatusBadRequest)
		return
	}
	x := msgs[0]
	ctype := string(x.props().ContentType)
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fmt.Sprintf("message-%d.txt", x.d.SequenceNumber)))
	w.Write(x.body)
}

// explorerPurge deletes every message of the current tab that is not locked
// or scheduled, in rounds of the broker's batch size, as the SDKs' purge
// does.
func (h *Handler) explorerPurge(w http.ResponseWriter, r *http.Request) {
	st := h.readState(w, r)
	if st == nil {
		return
	}
	addr, err := h.address(st.e.Path, st.queue)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	ctx := broker.WithActor(r.Context(), Actor)
	cutoff := h.opts.Now()
	extra := &explorerExtra{}
	total := 0
	for {
		n, err := h.opts.Broker.DeleteMessages(ctx, addr, h.opts.Broker.MaxBatchDelete(), cutoff, "")
		total += n
		if err != nil {
			extra.err = fmt.Sprintf("Purged %d messages, then: %s", total, strings.TrimPrefix(err.Error(), "broker: "))
			break
		}
		if n == 0 {
			extra.message = fmt.Sprintf("Purged %d messages from %s.", total, addr)
			break
		}
	}
	h.render(w, r, http.StatusOK, views.ExplorerBody(h.explorerData(r, st, "", extra)))
}
