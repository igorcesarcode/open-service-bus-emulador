package portal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

// GeoDRAlias is a Geo-Disaster Recovery alias.
type GeoDRAlias struct {
	Alias, Primary, Secondary string
	Paired                    bool
	LastSync                  time.Time
	Error                     string
}

// GeoReplicationInfo is the Geo-Replication state of the portal's namespace.
type GeoReplicationInfo struct {
	Enabled, Synchronous           bool
	MaxLag, Delay                  time.Duration
	PrimaryRegion, SecondaryRegion string
	State                          string
	LagDuration                    time.Duration
	LagCount                       int
	Promoting                      bool
	LastPromotion                  time.Time
	LostChanges                    int
}

// GeoReplicationConfig configures the replication of the portal's namespace.
type GeoReplicationConfig struct {
	Synchronous                    bool
	MaxLag, Delay                  time.Duration
	PrimaryRegion, SecondaryRegion string
}

// GeoControl gives the portal the server's Geo-DR and Geo-Replication
// operations. Operations on replication are on the portal's namespace.
type GeoControl interface {
	// Namespaces lists the names of the server's namespaces, and Current is
	// the portal's.
	Namespaces() []string
	Current() string
	GeoDRAliases() []GeoDRAlias
	PairGeoDR(ctx context.Context, alias, primary, secondary string) error
	FailoverGeoDR(ctx context.Context, alias string, force bool) error
	BreakGeoDR(ctx context.Context, alias string) error
	DeleteGeoDRAlias(ctx context.Context, alias string) error
	GeoReplication() GeoReplicationInfo
	EnableGeoReplication(ctx context.Context, cfg GeoReplicationConfig) error
	DisableGeoReplication() error
	PromoteGeoReplication(ctx context.Context, forced bool) error
}

func (h *Handler) geoOptions() []views.Option {
	var out []views.Option
	for _, n := range h.opts.Geo.Namespaces() {
		out = append(out, views.Option{Value: n, Label: n, Selected: strings.EqualFold(n, h.opts.Geo.Current())})
	}
	return out
}

func fmtTime(t time.Time, loc *time.Location) string {
	if t.IsZero() {
		return ""
	}
	return t.In(loc).Format("2006-01-02 15:04:05")
}

func (h *Handler) geoDRPage(msg, errMsg string) views.GeoDRPage {
	p := views.GeoDRPage{Namespace: h.opts.Geo.Current(), Namespaces: h.geoOptions(), Message: msg, Error: errMsg}
	cur := h.opts.Geo.Current()
	for _, a := range h.opts.Geo.GeoDRAliases() {
		row := views.GeoDRRow{Alias: a.Alias, Primary: a.Primary, Secondary: a.Secondary, Paired: a.Paired, LastSync: fmtTime(a.LastSync, h.opts.Location), Error: a.Error}
		switch {
		case strings.EqualFold(a.Primary, cur):
			row.Role = "primary"
		case a.Paired && strings.EqualFold(a.Secondary, cur):
			row.Role = "secondary"
		}
		p.Rows = append(p.Rows, row)
	}
	return p
}

func (h *Handler) geoReplicationView(msg, errMsg string) views.GeoReplicationView {
	g := h.opts.Geo.GeoReplication()
	v := views.GeoReplicationView{
		Namespace: h.opts.Geo.Current(), Enabled: g.Enabled, Message: msg, Error: errMsg,
		PrimaryRegion: g.PrimaryRegion, SecondaryRegion: g.SecondaryRegion, State: g.State,
		LagDuration: g.LagDuration.Round(time.Millisecond).String(), LagCount: g.LagCount, Promoting: g.Promoting,
		LastPromotion: fmtTime(g.LastPromotion, h.opts.Location), LostChanges: g.LostChanges,
		MaxLag: g.MaxLag.String(), Delay: g.Delay.String(),
	}
	if g.MaxLag == 0 {
		v.MaxLag = "1m0s"
	}
	if g.Synchronous {
		v.Mode = "Synchronous"
	} else {
		v.Mode = "Asynchronous"
	}
	if !g.Enabled {
		v.PrimaryRegion, v.SecondaryRegion, v.MaxLag, v.Delay = "primary", "secondary", "1m", "0s"
	}
	return v
}

func (h *Handler) registerGeo() {
	if h.opts.Geo == nil {
		return
	}
	h.mux.HandleFunc("GET "+Prefix+"geo-dr", func(w http.ResponseWriter, r *http.Request) {
		h.render(w, r, http.StatusOK, views.GeoDRPageView(h.layout("Geo-Recovery", views.NavGeoDR), h.geoDRPage("", "")))
	})
	h.mux.HandleFunc("GET "+Prefix+"geo-replication", func(w http.ResponseWriter, r *http.Request) {
		h.render(w, r, http.StatusOK, views.GeoReplicationPageView(h.layout("Geo-Replication", views.NavGeoRep), h.geoReplicationView("", "")))
	})
	h.mux.HandleFunc("GET "+Prefix+"geo-replication/status", func(w http.ResponseWriter, r *http.Request) {
		h.render(w, r, http.StatusOK, views.GeoReplicationStatusView(h.geoReplicationView("", "")))
	})
	h.mux.HandleFunc("POST "+Prefix+"geo-dr/action", func(w http.ResponseWriter, r *http.Request) {
		err := h.geoDRAction(r.Context(), r.PostFormValue("action"), r.PostFormValue("alias"), r.PostFormValue("primary"), r.PostFormValue("secondary"))
		page := h.geoDRPage(doneMessage(r.PostFormValue("action"), err), errorMessage(err))
		status := http.StatusOK
		if err != nil {
			status = http.StatusUnprocessableEntity
		}
		h.render(w, r, status, views.GeoDRPageView(h.layout("Geo-Recovery", views.NavGeoDR), page))
	})
	h.mux.HandleFunc("POST "+Prefix+"geo-replication/action", func(w http.ResponseWriter, r *http.Request) {
		cfg, err := geoConfigFromForm(r)
		if err == nil {
			err = h.geoReplicationAction(r.Context(), r.PostFormValue("action"), cfg)
		}
		page := h.geoReplicationView(doneMessage(r.PostFormValue("action"), err), errorMessage(err))
		status := http.StatusOK
		if err != nil {
			status = http.StatusUnprocessableEntity
		}
		h.render(w, r, status, views.GeoReplicationPageView(h.layout("Geo-Replication", views.NavGeoRep), page))
	})
	// The JSON API, for scripts: the same operations.
	h.mux.HandleFunc("GET "+Prefix+"api/geo", h.apiGeo)
	h.mux.HandleFunc("POST "+Prefix+"api/geo-dr", h.apiGeoDR)
	h.mux.HandleFunc("POST "+Prefix+"api/geo-replication", h.apiGeoReplication)
}

func doneMessage(action string, err error) string {
	if err != nil {
		return ""
	}
	return "Done: " + action + "."
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimPrefix(err.Error(), "geo-dr: ")
}

func (h *Handler) geoDRAction(ctx context.Context, action, alias, primary, secondary string) error {
	g := h.opts.Geo
	switch action {
	case "pair":
		return g.PairGeoDR(ctx, alias, primary, secondary)
	case "failover":
		return g.FailoverGeoDR(ctx, alias, false)
	case "forced-failover":
		return g.FailoverGeoDR(ctx, alias, true)
	case "break":
		return g.BreakGeoDR(ctx, alias)
	case "delete":
		return g.DeleteGeoDRAlias(ctx, alias)
	}
	return fmt.Errorf("unknown action %q", action)
}

func (h *Handler) geoReplicationAction(ctx context.Context, action string, cfg GeoReplicationConfig) error {
	g := h.opts.Geo
	switch action {
	case "enable":
		return g.EnableGeoReplication(ctx, cfg)
	case "disable":
		return g.DisableGeoReplication()
	case "promote":
		return g.PromoteGeoReplication(ctx, false)
	case "forced-promote":
		return g.PromoteGeoReplication(ctx, true)
	}
	return fmt.Errorf("unknown action %q", action)
}

func geoConfigFromForm(r *http.Request) (GeoReplicationConfig, error) {
	cfg := GeoReplicationConfig{
		Synchronous: r.PostFormValue("synchronous") == "true", PrimaryRegion: strings.TrimSpace(r.PostFormValue("primary")),
		SecondaryRegion: strings.TrimSpace(r.PostFormValue("secondary")),
	}
	var err error
	if v := strings.TrimSpace(r.PostFormValue("maxlag")); v != "" {
		if cfg.MaxLag, err = time.ParseDuration(v); err != nil {
			return cfg, fmt.Errorf("maximum replication lag: %v", err)
		}
	}
	if v := strings.TrimSpace(r.PostFormValue("delay")); v != "" {
		if cfg.Delay, err = time.ParseDuration(v); err != nil {
			return cfg, fmt.Errorf("replication delay: %v", err)
		}
	}
	return cfg, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func apiError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusGatewayTimeout
	}
	writeJSON(w, status, map[string]string{"error": errorMessage(err)})
}

func (h *Handler) apiGeo(w http.ResponseWriter, r *http.Request) {
	g := h.opts.Geo.GeoReplication()
	rep := map[string]any{"enabled": g.Enabled}
	if g.Enabled {
		rep = map[string]any{
			"enabled": true, "synchronous": g.Synchronous, "maxLagSeconds": g.MaxLag.Seconds(), "delayMilliseconds": g.Delay.Milliseconds(),
			"primaryRegion": g.PrimaryRegion, "secondaryRegion": g.SecondaryRegion, "state": g.State,
			"lagSeconds": g.LagDuration.Seconds(), "lagCount": g.LagCount, "promoting": g.Promoting, "lostChanges": g.LostChanges,
		}
	}
	var dr []map[string]any
	for _, a := range h.opts.Geo.GeoDRAliases() {
		dr = append(dr, map[string]any{"alias": a.Alias, "primary": a.Primary, "secondary": a.Secondary, "paired": a.Paired, "error": a.Error})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": h.opts.Geo.Current(), "namespaces": h.opts.Geo.Namespaces(), "geoDR": dr, "geoReplication": rep,
	})
}

func (h *Handler) apiGeoDR(w http.ResponseWriter, r *http.Request) {
	var in struct{ Action, Alias, Primary, Secondary string }
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		apiError(w, fmt.Errorf("decoding the request: %v", err))
		return
	}
	if err := h.geoDRAction(r.Context(), in.Action, in.Alias, in.Primary, in.Secondary); err != nil {
		apiError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) apiGeoReplication(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Action                         string
		Synchronous                    bool
		MaxLagSeconds, DelayMillis     float64
		PrimaryRegion, SecondaryRegion string
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		apiError(w, fmt.Errorf("decoding the request: %v", err))
		return
	}
	cfg := GeoReplicationConfig{
		Synchronous: in.Synchronous, MaxLag: time.Duration(in.MaxLagSeconds * float64(time.Second)),
		Delay: time.Duration(in.DelayMillis * float64(time.Millisecond)), PrimaryRegion: in.PrimaryRegion, SecondaryRegion: in.SecondaryRegion,
	}
	if err := h.geoReplicationAction(r.Context(), in.Action, cfg); err != nil {
		apiError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// apiAuthorized accepts the JSON API with a token in the Authorization
// header, which scripts have instead of a login session.
func (h *Handler) apiAuthorized(r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, Prefix+"api/") || r.Header.Get("Authorization") == "" {
		return false
	}
	return h.opts.Auth.AuthorizeRequest(r, "", auth.Manage) == nil
}
