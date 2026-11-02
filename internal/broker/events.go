package broker

import (
	"encoding/hex"
	"log/slog"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// Events, when Options.Events is set, are one log line for each thing that
// happens to a message: sent, routed by a rule, received, settled,
// dead-lettered, expired. They carry identifiers and sizes only, never the
// body or the properties of a message.
//
// An event inside a transaction is logged once the transaction commits, so a
// line is never written for a change that was rolled back.

// eventsOn reports whether events are logged, for callers that would
// otherwise build their attributes for nothing.
func (b *Broker) eventsOn() bool { return b.events != nil }

// emit logs an event after tx commits.
func (b *Broker) emit(tx *store.Tx, name string, attrs ...any) {
	if b.events != nil {
		tx.AfterCommit(func() { b.events.Info(name, attrs...) })
	}
}

// emitNow logs an event of something already committed.
func (b *Broker) emitNow(name string, attrs ...any) {
	if b.events != nil {
		b.events.Info(name, attrs...)
	}
}

// storedAttrs identifies a stored message.
func storedAttrs(e *Entity, m *store.Message) []any {
	a := []any{slog.String("entity", e.DisplayPath()), slog.Int64("seq", publicSeqOf(m.Partition, m.Seq))}
	if m.MessageID != "" {
		a = append(a, slog.String("id", m.MessageID))
	}
	if m.SessionID != "" {
		a = append(a, slog.String("session", m.SessionID))
	}
	return a
}

// lockAttr is the start of a lock token, enough to tell locks apart.
func lockAttr(token []byte) slog.Attr {
	n := min(len(token), 4)
	return slog.String("lock", hex.EncodeToString(token[:n]))
}

// sentEvents logs the messages of a send, ahead of the routing events the
// same transaction logs.
func (b *Broker) sentEvents(tx *store.Tx, e *Entity, msgs []IncomingMessage, seqs []int64) {
	now := b.now()
	tx.AfterCommitFirst(func() {
		for i, m := range msgs {
			a := []any{slog.String("entity", e.DisplayPath()), slog.Int64("seq", seqs[i]), slog.Int("bytes", len(m.Payload))}
			if m.MessageID != "" {
				a = append(a, slog.String("id", m.MessageID))
			}
			if m.SessionID != "" {
				a = append(a, slog.String("session", m.SessionID))
			}
			if m.ScheduledEnqueueTime.After(now) {
				a = append(a, slog.Time("scheduled", m.ScheduledEnqueueTime.UTC()))
			}
			b.events.Info("send", a...)
		}
	})
}
