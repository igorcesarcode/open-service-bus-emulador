package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
)

// maxMetricPoints bounds the points a metrics request may ask for.
const maxMetricPoints = 5000

// servePrometheus writes the metrics in the Prometheus text format.
func (n *Namespace) servePrometheus(w http.ResponseWriter, r *http.Request) {
	counters, gauges, err := n.broker.MetricTotals(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	metrics.WritePrometheus(w, counters, gauges)
}

type metricPoint struct {
	Time  time.Time `json:"time"`
	Value int64     `json:"value"`
}

// serveMetricsJSON answers /$api/metrics?entity=&from=&to=&step= with the
// metrics of an entity, or of the namespace when entity is empty. from and
// to are RFC 3339 times, by default the last hour; step is a duration such
// as 1m, 5m, 1h or 24h.
func (n *Namespace) serveMetricsJSON(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	to, err := parseTime(q.Get("to"), time.Now())
	if err != nil {
		httpError(w, http.StatusBadRequest, "to: %v", err)
		return
	}
	from, err := parseTime(q.Get("from"), to.Add(-time.Hour))
	if err != nil {
		httpError(w, http.StatusBadRequest, "from: %v", err)
		return
	}
	step := time.Minute
	if v := q.Get("step"); v != "" {
		if step, err = time.ParseDuration(v); err != nil || step < time.Minute {
			httpError(w, http.StatusBadRequest, "step: want a duration of at least 1m, got %q", v)
			return
		}
	}
	if to.Sub(from)/step > maxMetricPoints {
		httpError(w, http.StatusBadRequest, "the range holds more than %d points; use a larger step", maxMetricPoints)
		return
	}
	entity := q.Get("entity")
	if entity != "" {
		if _, err := n.broker.Entity(entity); err != nil {
			httpError(w, http.StatusNotFound, "%v", err)
			return
		}
	}
	series, err := n.broker.Metrics(r.Context(), entity, from, to, step)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	out := make(map[string][]metricPoint, len(series))
	for name, points := range series {
		list := make([]metricPoint, len(points))
		for i, p := range points {
			list[i] = metricPoint{p.Time, p.Value}
		}
		out[string(name)] = list
	}
	writeJSON(w, map[string]any{"entity": entity, "from": from.UTC(), "to": to.UTC(), "step": step.String(), "metrics": out})
}

type activityJSON struct {
	ID        int64     `json:"id"`
	Time      time.Time `json:"time"`
	Actor     string    `json:"actor"`
	Operation string    `json:"operation"`
	Entity    string    `json:"entity"`
	Status    string    `json:"status"`
	Detail    string    `json:"detail"`
}

// serveActivityJSON answers /$api/activity?from=&to=&actor=&operation=
// &entity=&status=&limit= with the activity log, newest first.
func (n *Namespace) serveActivityJSON(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	aq := broker.ActivityQuery{
		Actor: q.Get("actor"), Operation: q.Get("operation"), Entity: q.Get("entity"), Status: q.Get("status"),
	}
	var err error
	if v := q.Get("from"); v != "" {
		if aq.From, err = time.Parse(time.RFC3339, v); err != nil {
			httpError(w, http.StatusBadRequest, "from: %v", err)
			return
		}
	}
	if v := q.Get("to"); v != "" {
		if aq.To, err = time.Parse(time.RFC3339, v); err != nil {
			httpError(w, http.StatusBadRequest, "to: %v", err)
			return
		}
	}
	if v := q.Get("limit"); v != "" {
		if aq.Limit, err = strconv.Atoi(v); err != nil || aq.Limit < 1 {
			httpError(w, http.StatusBadRequest, "limit: want a positive number, got %q", v)
			return
		}
	}
	entries, err := n.broker.Activity(r.Context(), aq)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	out := make([]activityJSON, len(entries))
	for i, e := range entries {
		out[i] = activityJSON(e)
	}
	writeJSON(w, out)
}

func parseTime(v string, def time.Time) (time.Time, error) {
	if v == "" {
		return def, nil
	}
	return time.Parse(time.RFC3339, v)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, status int, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf(format, args...)})
}

// manage requires Manage on the namespace for the observability endpoints
// when authentication is on.
func (n *Namespace) manage(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := n.auth.AuthorizeRequest(r, "", auth.Manage); err != nil {
			httpError(w, http.StatusUnauthorized, "%v", err)
			return
		}
		next(w, r)
	}
}

func (s *Server) serveJWKS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(s.dev.JWKS())
}

// serveDevToken signs a token for the principal in the form field
// "principal", valid for "ttl" seconds (default one hour). It exists for
// tests without Entra ID, and works only with -dev-issuer.
func (s *Server) serveDevToken(w http.ResponseWriter, r *http.Request) {
	principal := strings.TrimSpace(r.PostFormValue("principal"))
	if principal == "" {
		httpError(w, http.StatusBadRequest, "principal is required")
		return
	}
	ttl := time.Hour
	if v := r.PostFormValue("ttl"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil {
			httpError(w, http.StatusBadRequest, "ttl: want a number of seconds, got %q", v)
			return
		}
		ttl = time.Duration(secs) * time.Second
	}
	token, expires, err := s.dev.Token(principal, ttl)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	writeJSON(w, map[string]any{"access_token": token, "token_type": "Bearer", "expires_on": expires.Unix()})
}
