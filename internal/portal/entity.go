package portal

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

func (h *Handler) entityFromRequest(w http.ResponseWriter, r *http.Request) *broker.Entity {
	path := r.FormValue("path")
	e, err := h.opts.Broker.Entity(path)
	if err != nil {
		http.NotFound(w, r)
		return nil
	}
	return e
}

// ttl writes a duration for a property list; infinite durations read
// "Never".
func ttl(d time.Duration) string {
	if d >= broker.Infinite {
		return "Never"
	}
	return humanDuration(d)
}

func humanDuration(d time.Duration) string {
	switch {
	case d%(24*time.Hour) == 0 && d >= 24*time.Hour:
		return fmt.Sprintf("%d days", d/(24*time.Hour))
	case d%time.Hour == 0 && d >= time.Hour:
		return fmt.Sprintf("%d hours", d/time.Hour)
	case d%time.Minute == 0 && d >= time.Minute:
		return fmt.Sprintf("%d minutes", d/time.Minute)
	}
	return duration.Format(d)
}

func yesNo(b bool) string {
	if b {
		return "Yes"
	}
	return "No"
}

func (h *Handler) entityLive(ctx context.Context, e *broker.Entity, rangeKey string) (views.EntityLive, error) {
	rg := findRange(rangeKey)
	to := h.opts.Now()
	from := to.Add(-rg.Span)
	m, err := h.opts.Broker.Metrics(ctx, e.Path, from, to, rg.Step)
	if err != nil {
		return views.EntityLive{}, err
	}
	c, err := h.opts.Broker.Counts(ctx, e.Path)
	if err != nil {
		return views.EntityLive{}, err
	}
	p := e.Properties
	max := h.opts.Broker.MaxSizeBytes(e)
	free := 100.0
	if max > 0 {
		free = 100 * (1 - float64(c.SizeBytes)/float64(max))
	}
	size, unit := splitBytes(formatBytes(c.SizeBytes))
	l := views.EntityLive{Path: e.Path, Ranges: rangeOptions(rg), Range: rg.Key}
	l.Tiles = append(l.Tiles, views.Tile{Label: "Current size", Value: size, Unit: unit})
	if e.Kind != broker.KindSubscription {
		maxV, maxU := splitBytes(formatBytes(max))
		l.Tiles = append(l.Tiles, views.Tile{Label: "Max size", Value: maxV, Unit: maxU})
	}
	days := ttl(p.DefaultMessageTimeToLive)
	l.Tiles = append(l.Tiles,
		views.Tile{Label: "Message time to live", Value: days},
		views.Tile{Label: "Auto-delete", Value: strings.ToUpper(ttl(p.AutoDeleteOnIdle))})
	if e.Kind != broker.KindSubscription {
		l.Tiles = append(l.Tiles, views.Tile{Label: "Free space", Value: fmt.Sprintf("%.1f", free), Unit: "%"})
	}
	if e.Kind != broker.KindTopic {
		l.Counts = []views.Tile{
			{Label: "Active", Value: fmt.Sprint(c.Active)},
			{Label: "Dead-letter", Value: fmt.Sprint(c.DeadLetter)},
			{Label: "Scheduled", Value: fmt.Sprint(c.Scheduled)},
			{Label: "Transfer dead-letter", Value: fmt.Sprint(c.TransferDeadLetter)},
		}
	}
	line := func(names map[metrics.Name]string, order []metrics.Name, gauge bool) []lineSeries {
		var out []lineSeries
		for _, n := range order {
			out = append(out, lineSeries{name: names[n], points: m[n], gauge: gauge})
		}
		return out
	}
	req := line(map[metrics.Name]string{
		metrics.IncomingRequests: "Incoming Requests", metrics.SuccessfulRequests: "Successful Requests",
		metrics.ServerErrors: "Server Errors", metrics.UserErrors: "User Errors", metrics.ThrottledRequests: "Throttled Requests",
	}, []metrics.Name{metrics.IncomingRequests, metrics.SuccessfulRequests, metrics.ServerErrors, metrics.UserErrors, metrics.ThrottledRequests}, false)
	msg := append(line(map[metrics.Name]string{
		metrics.IncomingMessages: "Incoming Messages", metrics.OutgoingMessages: "Outgoing Messages",
	}, []metrics.Name{metrics.IncomingMessages, metrics.OutgoingMessages}, false),
		line(map[metrics.Name]string{metrics.ActiveMessages: "Active Messages", metrics.DeadletteredMessages: "Dead-lettered Messages"},
			[]metrics.Name{metrics.ActiveMessages, metrics.DeadletteredMessages}, true)...)
	l.Requests = buildChart("Requests", req, from, to, h.opts.Location)
	l.Messages = buildChart("Messages", msg, from, to, h.opts.Location)
	return l, nil
}

// splitBytes separates "2.1 KB" into the number and the unit.
func splitBytes(s string) (string, string) {
	num, unit, _ := strings.Cut(s, " ")
	return num, unit
}

func entityProps(e *broker.Entity) []views.KV {
	p := e.Properties
	kv := []views.KV{{Key: "Status", Value: string(p.Status)}}
	add := func(k, v string) { kv = append(kv, views.KV{Key: k, Value: v}) }
	if e.Kind != broker.KindTopic {
		add("Lock duration", humanDuration(p.LockDuration))
		add("Max delivery count", fmt.Sprint(p.MaxDeliveryCount))
		add("Dead-letter on expiration", yesNo(p.DeadLetteringOnMessageExpiration))
		add("Sessions", yesNo(p.RequiresSession))
		if p.ForwardTo != "" {
			add("Forward to", p.ForwardTo)
		}
		if p.ForwardDeadLetteredMessagesTo != "" {
			add("Forward dead-lettered to", p.ForwardDeadLetteredMessagesTo)
		}
	}
	if e.Kind != broker.KindSubscription {
		add("Duplicate detection", yesNo(p.RequiresDuplicateDetection))
		if p.RequiresDuplicateDetection {
			add("Duplicate detection window", humanDuration(p.DuplicateDetectionHistoryTimeWindow))
		}
		add("Partitioning", yesNo(p.EnablePartitioning))
		add("Express", yesNo(p.EnableExpress))
		add("Max message size", fmt.Sprintf("%d KB", p.MaxMessageSizeInKilobytes))
	}
	add("Created", e.CreatedAt.Format("2006-01-02 15:04:05 MST"))
	add("Updated", e.UpdatedAt.Format("2006-01-02 15:04:05 MST"))
	add("Last accessed", e.AccessedAt.Format("2006-01-02 15:04:05 MST"))
	return kv
}

func (h *Handler) subscriptionRows(ctx context.Context, topic, query string) ([]views.SubscriptionRow, error) {
	q := strings.ToLower(strings.TrimSpace(query))
	var rows []views.SubscriptionRow
	for _, s := range h.opts.Broker.Entities(broker.KindSubscription, topic) {
		if q != "" && !strings.Contains(strings.ToLower(s.Name), q) {
			continue
		}
		c, err := h.opts.Broker.Counts(ctx, s.Path)
		if err != nil {
			continue // Deleted since the listing.
		}
		rows = append(rows, views.SubscriptionRow{
			Name: s.Name, Path: s.Path, Status: string(s.Properties.Status),
			Messages: c.Active + c.DeadLetter + c.Scheduled + c.TransferDeadLetter, MaxDelivery: s.Properties.MaxDeliveryCount,
		})
	}
	return rows, nil
}

func (h *Handler) entityPage(w http.ResponseWriter, r *http.Request) {
	e := h.entityFromRequest(w, r)
	if e == nil {
		return
	}
	live, err := h.entityLive(r.Context(), e, r.URL.Query().Get("range"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	page := views.EntityPage{Kind: string(e.Kind), Name: e.Name, Path: e.Path, Props: entityProps(e), Live: live}
	if e.Topic != nil {
		page.TopicPath, page.TopicName = e.Topic.Path, e.Topic.Name
		page.Name = e.Name
	}
	if e.Kind == broker.KindTopic {
		if page.Subscriptions, err = h.subscriptionRows(r.Context(), e.Path, ""); err != nil {
			h.fail(w, r, err)
			return
		}
	}
	h.render(w, r, http.StatusOK, views.EntityPageView(h.layout(e.DisplayPath(), navFor(e.Kind)), page))
}

func (h *Handler) entityLiveHandler(w http.ResponseWriter, r *http.Request) {
	e := h.entityFromRequest(w, r)
	if e == nil {
		return
	}
	live, err := h.entityLive(r.Context(), e, r.FormValue("range"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.render(w, r, http.StatusOK, views.EntityLiveView(live))
}

func (h *Handler) entitySubscriptions(w http.ResponseWriter, r *http.Request) {
	e := h.entityFromRequest(w, r)
	if e == nil {
		return
	}
	rows, err := h.subscriptionRows(r.Context(), e.Path, r.FormValue("q"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.render(w, r, http.StatusOK, views.SubscriptionRows(rows))
}
