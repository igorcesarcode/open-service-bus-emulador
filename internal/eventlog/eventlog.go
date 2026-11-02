// Package eventlog writes the broker's message events as one short line each:
//
//	15:04:05.123 send         orders seq=12 id=7f3a bytes=120
//
// Lines carry identifiers and sizes, never the body or the properties of a
// message.
package eventlog

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
)

// New returns a logger that writes events to w.
func New(w io.Writer) *slog.Logger {
	return slog.New(&handler{w: w, mu: new(sync.Mutex)})
}

type handler struct {
	w     io.Writer
	mu    *sync.Mutex
	attrs []slog.Attr
}

func (h *handler) Enabled(context.Context, slog.Level) bool { return true }

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	c := *h
	c.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &c
}

func (h *handler) WithGroup(string) slog.Handler { return h }

func (h *handler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Time.Format("15:04:05.000"))
	b.WriteByte(' ')
	b.WriteString(r.Message)
	for b.Len() < len("15:04:05.000 dead-letter ") {
		b.WriteByte(' ')
	}
	// The entity comes first, so that a stream reads as a list of what
	// happened where.
	var rest []slog.Attr
	put := func(a slog.Attr) {
		if a.Key == "entity" {
			b.WriteByte(' ')
			b.WriteString(a.Value.String())
			return
		}
		rest = append(rest, a)
	}
	for _, a := range h.attrs {
		put(a)
	}
	r.Attrs(func(a slog.Attr) bool { put(a); return true })
	for _, a := range rest {
		b.WriteByte(' ')
		b.WriteString(a.Key)
		b.WriteByte('=')
		b.WriteString(quote(a.Value.String()))
	}
	b.WriteByte('\n')
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, b.String())
	return err
}

func quote(s string) string {
	if s == "" || strings.ContainsAny(s, " \t\"=") {
		return strconv.Quote(s)
	}
	return s
}
