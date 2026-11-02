package portal

import (
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

// metricLabels name the metrics as the Azure portal does.
var metricLabels = map[metrics.Name]string{
	metrics.IncomingRequests: "Incoming Requests", metrics.SuccessfulRequests: "Successful Requests",
	metrics.ServerErrors: "Server Errors", metrics.UserErrors: "User Errors", metrics.ThrottledRequests: "Throttled Requests",
	metrics.IncomingMessages: "Incoming Messages", metrics.OutgoingMessages: "Outgoing Messages",
	metrics.ConnectionsOpened: "Connections Opened", metrics.ConnectionsClosed: "Connections Closed",
	metrics.ActiveMessages: "Active Messages", metrics.DeadletteredMessages: "Dead-lettered Messages",
	metrics.ScheduledMessages: "Scheduled Messages", metrics.TransferDeadletteredMessages: "Transfer Dead-lettered Messages",
	metrics.Size: "Size (bytes)", metrics.ActiveConnections: "Active Connections",
}

var defaultMetrics = []metrics.Name{metrics.IncomingMessages, metrics.OutgoingMessages, metrics.ActiveMessages}

func (h *Handler) registerMetrics() {
	h.mux.HandleFunc("GET "+Prefix+"metrics", h.metricsPage)
	h.mux.HandleFunc("GET "+Prefix+"metrics/view", h.metricsView)
}

func (h *Handler) metricsData(r *http.Request, defaults bool) (views.MetricsPage, error) {
	q := r.URL.Query()
	rg := findRange(q.Get("range"))
	selected := q["m"]
	if len(selected) == 0 && defaults {
		for _, n := range defaultMetrics {
			selected = append(selected, string(n))
		}
	}
	entity := broker.NormalizePath(q.Get("entity"))
	p := views.MetricsPage{Ranges: rangeOptions(rg), Range: rg.Key}
	p.Entities = append(p.Entities, views.Option{Value: "", Label: "Namespace", Selected: entity == ""})
	for _, kind := range []broker.EntityKind{broker.KindQueue, broker.KindTopic, broker.KindSubscription} {
		for _, e := range h.opts.Broker.Entities(kind, "") {
			p.Entities = append(p.Entities, views.Option{Value: e.Path, Label: string(kind) + ": " + e.DisplayPath(), Selected: e.Path == entity})
		}
	}
	option := func(n metrics.Name) views.MetricOption {
		return views.MetricOption{Name: string(n), Label: metricLabels[n], On: slices.Contains(selected, string(n))}
	}
	for _, n := range metrics.Counters {
		p.Counters = append(p.Counters, option(n))
	}
	for _, n := range metrics.Gauges {
		p.Gauges = append(p.Gauges, option(n))
	}

	to := h.opts.Now()
	from := to.Add(-rg.Span)
	if entity != "" {
		if _, err := h.opts.Broker.Entity(entity); err != nil {
			entity = "" // Deleted since the page was drawn.
		}
	}
	series, err := h.opts.Broker.Metrics(r.Context(), entity, from, to, rg.Step)
	if err != nil {
		return p, err
	}
	for _, n := range append(append([]metrics.Name{}, metrics.Counters...), metrics.Gauges...) {
		if !slices.Contains(selected, string(n)) {
			continue
		}
		line := lineSeries{name: metricLabels[n], points: series[n], gauge: n.Gauge()}
		p.Charts = append(p.Charts, buildChart(metricLabels[n], []lineSeries{line}, from, to, h.opts.Location))
	}
	api := url.Values{"entity": {entity}, "from": {from.UTC().Format("2006-01-02T15:04:05Z")}, "to": {to.UTC().Format("2006-01-02T15:04:05Z")}, "step": {rg.Step.String()}}
	p.APIURL = "/$api/metrics?" + strings.ReplaceAll(api.Encode(), "%3A", ":")
	return p, nil
}

func (h *Handler) metricsPage(w http.ResponseWriter, r *http.Request) {
	p, err := h.metricsData(r, true)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.render(w, r, http.StatusOK, views.MetricsPageView(h.layout("Metrics", views.NavMetrics), p))
}

func (h *Handler) metricsView(w http.ResponseWriter, r *http.Request) {
	p, err := h.metricsData(r, false)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.render(w, r, http.StatusOK, views.MetricsView(p))
}
