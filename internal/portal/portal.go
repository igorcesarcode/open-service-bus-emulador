// Package portal serves the local web portal: a view of the namespace, its
// entities and its activity, in the layout of the Azure portal. Pages are
// rendered on the server with templ and updated with HTMX; charts are SVG.
package portal

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/a-h/templ"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

//go:embed assets
var assets embed.FS

// Prefix is where the portal is served.
const Prefix = "/$portal/"

// Actor is the name the portal's operations carry in the activity log.
const Actor = "portal"

// Options configures a Handler.
type Options struct {
	Broker *broker.Broker
	// Auth, when enabled, puts the portal behind a login with the name
	// and key of a namespace policy that has Manage.
	Auth *auth.Authority
	// Info returns the static facts of the Essentials block. Its Queues,
	// Topics and Uptime are filled by the portal.
	Info func() views.Info
	// Geo gives the Geo-Recovery and Geo-Replication pages their operations
	// and the namespace switcher its list. Nil leaves those pages out.
	Geo GeoControl
	// Export returns the files of the export template. Nil leaves the
	// page out.
	Export func(ctx context.Context) ([]ExportFile, error)
	// Certificate returns the TLS certificate in PEM form, or nil.
	Certificate func() []byte
	Started     time.Time
	Logger      *slog.Logger
	// Location is the time zone of chart labels and the activity log. Nil
	// means the local one.
	Location *time.Location
	// Now overrides the clock, for tests.
	Now func() time.Time
}

// Handler serves the portal under Prefix.
type Handler struct {
	opts Options
	mux  *http.ServeMux

	sessionMu sync.Mutex
	sessions  map[string]time.Time
}

// NewHandler returns the portal handler.
func NewHandler(opts Options) *Handler {
	if opts.Location == nil {
		opts.Location = time.Local
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	h := &Handler{opts: opts, mux: http.NewServeMux(), sessions: map[string]time.Time{}}
	h.mux.HandleFunc("GET "+Prefix+"login", h.loginPage)
	h.mux.HandleFunc("POST "+Prefix+"login", h.login)
	h.mux.HandleFunc("POST "+Prefix+"logout", h.logout)
	sub, _ := fs.Sub(assets, "assets")
	h.mux.Handle("GET "+Prefix+"assets/", http.StripPrefix(Prefix+"assets/", cached(http.FileServerFS(sub))))
	h.mux.HandleFunc("GET "+Prefix+"{$}", h.overview)
	h.mux.HandleFunc("GET "+Prefix+"overview/live", h.overviewLive)
	h.mux.HandleFunc("GET "+Prefix+"certificate.pem", h.certificate)
	h.mux.HandleFunc("GET "+Prefix+"activity", h.activity)
	h.mux.HandleFunc("GET "+Prefix+"activity/rows", h.activityRows)
	for _, kind := range []broker.EntityKind{broker.KindQueue, broker.KindTopic} {
		p := Prefix + string(kind) + "s"
		h.mux.HandleFunc("GET "+p, h.entities(kind, false))
		h.mux.HandleFunc("GET "+p+"/rows", h.entities(kind, true))
		h.mux.HandleFunc("GET "+p+"/new", h.newForm(kind))
		h.mux.HandleFunc("POST "+p, h.create(kind))
	}
	h.mux.HandleFunc("POST "+Prefix+"entities/delete", h.deleteEntity)
	h.mux.HandleFunc("GET "+Prefix+"entity", h.entityPage)
	h.mux.HandleFunc("GET "+Prefix+"entity/live", h.entityLiveHandler)
	h.mux.HandleFunc("GET "+Prefix+"entity/subscriptions", h.entitySubscriptions)
	h.mux.HandleFunc("GET "+Prefix+"entity/edit", h.editForm)
	h.mux.HandleFunc("POST "+Prefix+"entity/edit", h.update)
	h.mux.HandleFunc("GET "+Prefix+"subscriptions/new", h.newForm(broker.KindSubscription))
	h.mux.HandleFunc("POST "+Prefix+"subscriptions", h.create(broker.KindSubscription))
	h.registerRules()
	h.registerExplorer()
	h.registerMetrics()
	h.registerAccess()
	h.registerGeo()
	h.registerSettings()
	return h
}

func cached(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		next.ServeHTTP(w, r)
	})
}

// ServeHTTP serves requests under Prefix. Changes require an HTMX request or
// a same-origin form, so another site cannot drive the portal through the
// browser.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && !sameOrigin(r) {
		http.Error(w, "cross-origin request refused", http.StatusForbidden)
		return
	}
	if h.opts.Auth.Enabled() && !h.public(r) && !h.loggedIn(r) && !h.apiAuthorized(r) {
		if r.Header.Get("HX-Request") == "true" {
			w.Header().Set("HX-Redirect", Prefix+"login")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, Prefix+"login", http.StatusSeeOther)
		return
	}
	h.mux.ServeHTTP(w, r)
}

func sameOrigin(r *http.Request) bool {
	// A JSON body cannot come from a cross-site form, and a cross-site
	// script needs a preflight the portal never allows.
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		return true
	}
	if r.Header.Get("HX-Request") == "true" {
		// Browsers refuse to add this header to a cross-origin request
		// unless the server allows it, which it does not.
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}

func (h *Handler) layout(title string, nav views.Nav) views.Layout {
	name := h.opts.Info().Name
	l := views.Layout{Title: title, Name: name, Nav: nav, SignOut: h.opts.Auth.Enabled()}
	if h.opts.Geo != nil {
		l.Namespaces = h.geoOptions()
	}
	return l
}

func (h *Handler) render(w http.ResponseWriter, r *http.Request, status int, c templ.Component) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := c.Render(r.Context(), w); err != nil {
		h.opts.Logger.Debug("rendering the portal", "path", r.URL.Path, "err", err)
	}
}

func (h *Handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	h.opts.Logger.Error("portal request failed", "path", r.URL.Path, "err", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (h *Handler) info() views.Info {
	i := h.opts.Info()
	i.Queues = len(h.opts.Broker.Entities(broker.KindQueue, ""))
	i.Topics = len(h.opts.Broker.Entities(broker.KindTopic, ""))
	i.Uptime = formatUptime(h.opts.Now().Sub(h.opts.Started))
	if i.Status == "" {
		i.Status = "Active"
	}
	if h.opts.Certificate != nil {
		i.HasCertificate = len(h.opts.Certificate()) > 0
	}
	return i
}

func formatUptime(d time.Duration) string {
	d = d.Round(time.Second)
	switch {
	case d >= 24*time.Hour:
		return fmt.Sprintf("%dd %dh", d/(24*time.Hour), d%(24*time.Hour)/time.Hour)
	case d >= time.Hour:
		return fmt.Sprintf("%dh %dm", d/time.Hour, d%time.Hour/time.Minute)
	}
	return fmt.Sprintf("%dm %ds", d/time.Minute, d%time.Minute/time.Second)
}

func (h *Handler) overviewData(ctx context.Context, rangeKey string) (views.Overview, error) {
	rg := findRange(rangeKey)
	to := h.opts.Now()
	from := to.Add(-rg.Span)
	m, err := h.opts.Broker.Metrics(ctx, metrics.Namespace, from, to, rg.Step)
	if err != nil {
		return views.Overview{}, err
	}
	pick := func(names map[metrics.Name]string, order []metrics.Name, gauge bool) []lineSeries {
		var out []lineSeries
		for _, n := range order {
			out = append(out, lineSeries{name: names[n], points: m[n], gauge: gauge})
		}
		return out
	}
	req := pick(map[metrics.Name]string{
		metrics.IncomingRequests: "Incoming Requests", metrics.SuccessfulRequests: "Successful Requests",
		metrics.ServerErrors: "Server Errors", metrics.UserErrors: "User Errors", metrics.ThrottledRequests: "Throttled Requests",
	}, []metrics.Name{metrics.IncomingRequests, metrics.SuccessfulRequests, metrics.ServerErrors, metrics.UserErrors, metrics.ThrottledRequests}, false)
	msg := append(pick(map[metrics.Name]string{
		metrics.IncomingMessages: "Incoming Messages", metrics.OutgoingMessages: "Outgoing Messages",
	}, []metrics.Name{metrics.IncomingMessages, metrics.OutgoingMessages}, false),
		pick(map[metrics.Name]string{
			metrics.ActiveMessages: "Active Messages", metrics.DeadletteredMessages: "Dead-lettered Messages",
		}, []metrics.Name{metrics.ActiveMessages, metrics.DeadletteredMessages}, true)...)
	return views.Overview{
		Info: h.info(), Ranges: rangeOptions(rg), Range: rg.Key,
		Requests: buildChart("Requests", req, from, to, h.opts.Location),
		Messages: buildChart("Messages", msg, from, to, h.opts.Location),
	}, nil
}

func (h *Handler) overview(w http.ResponseWriter, r *http.Request) {
	o, err := h.overviewData(r.Context(), r.URL.Query().Get("range"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.render(w, r, http.StatusOK, views.OverviewPage(h.layout("Overview", views.NavOverview), o))
}

func (h *Handler) overviewLive(w http.ResponseWriter, r *http.Request) {
	o, err := h.overviewData(r.Context(), r.URL.Query().Get("range"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.render(w, r, http.StatusOK, views.Live(o))
}

func (h *Handler) certificate(w http.ResponseWriter, r *http.Request) {
	if h.opts.Certificate == nil || len(h.opts.Certificate()) == 0 {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/x-pem-file")
	w.Write(h.opts.Certificate())
}

func (h *Handler) activityPage(r *http.Request) (views.ActivityPage, error) {
	q := r.URL.Query()
	p := views.ActivityPage{Actor: q.Get("actor"), Operation: q.Get("operation"), Entity: q.Get("entity"), Status: q.Get("status")}
	entries, err := h.opts.Broker.Activity(r.Context(), broker.ActivityQuery{
		Actor: p.Actor, Operation: p.Operation, Entity: p.Entity, Status: p.Status, Limit: 200,
	})
	if err != nil {
		return p, err
	}
	for _, e := range entries {
		p.Rows = append(p.Rows, views.ActivityRow{
			Time: e.Time.In(h.opts.Location).Format("2006-01-02 15:04:05"), Actor: e.Actor, Operation: e.Operation,
			Entity: e.Entity, Status: e.Status, Detail: e.Detail, Failed: e.Status == broker.StatusFailed,
		})
	}
	return p, nil
}

func (h *Handler) activity(w http.ResponseWriter, r *http.Request) {
	p, err := h.activityPage(r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.render(w, r, http.StatusOK, views.ActivityPageView(h.layout("Activity log", views.NavActivity), p))
}

func (h *Handler) activityRows(w http.ResponseWriter, r *http.Request) {
	p, err := h.activityPage(r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.render(w, r, http.StatusOK, views.ActivityRows(p))
}

func (h *Handler) entityList(ctx context.Context, kind broker.EntityKind, query string) (views.EntityList, error) {
	list := views.EntityList{Kind: string(kind), Query: query}
	q := strings.ToLower(strings.TrimSpace(query))
	for _, e := range h.opts.Broker.Entities(kind, "") {
		if q != "" && !strings.Contains(strings.ToLower(e.Name), q) {
			continue
		}
		c, err := h.opts.Broker.Counts(ctx, e.Path)
		if err != nil {
			if errors.Is(err, broker.ErrEntityNotFound) {
				continue // Deleted since the listing.
			}
			return list, err
		}
		row := views.EntityRow{
			Name: e.Name, Path: e.Path, Status: string(e.Properties.Status),
			Active: c.Active, DeadLetter: c.DeadLetter, Scheduled: c.Scheduled,
			Size: formatBytes(c.SizeBytes), MaxSize: formatBytes(h.opts.Broker.MaxSizeBytes(e)),
			MaxDelivery: e.Properties.MaxDeliveryCount,
		}
		if kind == broker.KindTopic {
			row.Subscriptions = len(h.opts.Broker.Entities(broker.KindSubscription, e.Path))
		}
		list.Rows = append(list.Rows, row)
	}
	return list, nil
}

func (h *Handler) entities(kind broker.EntityKind, rowsOnly bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		list, err := h.entityList(r.Context(), kind, r.URL.Query().Get("q"))
		if err != nil {
			h.fail(w, r, err)
			return
		}
		if rowsOnly {
			h.render(w, r, http.StatusOK, views.EntityRows(list))
			return
		}
		nav := views.NavQueues
		if kind == broker.KindTopic {
			nav = views.NavTopics
		}
		h.render(w, r, http.StatusOK, views.EntitiesPage(h.layout(strings.ToUpper(string(kind[:1]))+string(kind[1:])+"s", nav), list))
	}
}

func (h *Handler) deleteEntity(w http.ResponseWriter, r *http.Request) {
	path := r.PostFormValue("path")
	next := Prefix + "queues"
	if e, err := h.opts.Broker.Entity(path); err == nil {
		switch {
		case e.Kind == broker.KindTopic:
			next = Prefix + "topics"
		case e.Topic != nil:
			next = EntityURL(e.Topic.Path)
		}
	}
	if err := h.opts.Broker.DeleteEntity(broker.WithActor(r.Context(), Actor), path); err != nil && !errors.Is(err, broker.ErrEntityNotFound) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.PostFormValue("redirect") != "" {
		// Deleted from the entity's own page: leave it.
		w.Header().Set("HX-Redirect", next)
	}
	// An empty body removes the row.
	w.WriteHeader(http.StatusOK)
}

func urlQuery(s string) string { return url.QueryEscape(s) }

// atoi parses an optional integer field.
func atoi(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}
