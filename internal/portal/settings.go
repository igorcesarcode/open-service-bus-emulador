package portal

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

// ExportFile is a file of the export template.
type ExportFile struct {
	Name string
	Data []byte
}

// exportPreview is how much of a file the export page shows.
const exportPreview = 8000

func (h *Handler) propertiesPage(msg, errMsg string) views.PropertiesPage {
	m := h.opts.Broker.Meta()
	p := views.PropertiesPage{Namespace: h.opts.Info().Name, Message: msg, Error: errMsg}
	keys := make([]string, 0, len(m.Tags))
	for k := range m.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var tags []string
	for _, k := range keys {
		tags = append(tags, k+"="+m.Tags[k])
	}
	p.Tags = strings.Join(tags, "\n")
	for _, l := range m.Locks {
		p.Locks = append(p.Locks, views.LockRow{Name: l.Name, Level: string(l.Level), Scope: scopeLabel(l.Scope), Notes: l.Notes})
	}
	p.Scopes = []views.Option{{Value: "", Label: "Namespace"}}
	for _, q := range h.opts.Broker.Entities(broker.KindQueue, "") {
		p.Scopes = append(p.Scopes, views.Option{Value: q.Path, Label: "Queue " + q.Name})
	}
	for _, t := range h.opts.Broker.Entities(broker.KindTopic, "") {
		p.Scopes = append(p.Scopes, views.Option{Value: t.Path, Label: "Topic " + t.Name})
		for _, s := range h.opts.Broker.Entities(broker.KindSubscription, t.Path) {
			p.Scopes = append(p.Scopes, views.Option{Value: s.Path, Label: "Subscription " + t.Name + "/" + s.Name})
		}
	}
	if e := m.Encryption; e != nil {
		p.Encrypted, p.KeyIDs, p.Identity, p.Infrastructure = true, strings.Join(e.KeyVaultKeyIDs, "\n"), e.Identity, e.InfrastructureEncryption
	}
	return p
}

func (h *Handler) propertiesAction(ctx context.Context, r *http.Request) error {
	b := h.opts.Broker
	ctx = broker.WithActor(ctx, Actor)
	switch r.PostFormValue("action") {
	case "add-lock":
		return b.SetLock(ctx, broker.Lock{
			Name: strings.TrimSpace(r.PostFormValue("name")), Level: broker.LockLevel(r.PostFormValue("level")),
			Scope: r.PostFormValue("scope"), Notes: strings.TrimSpace(r.PostFormValue("notes")),
		})
	case "delete-lock":
		return b.DeleteLock(ctx, r.PostFormValue("name"))
	case "set-tags":
		tags := map[string]string{}
		for _, line := range strings.Split(r.PostFormValue("tags"), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			if !ok {
				return fmt.Errorf("tag %q: want name=value", line)
			}
			tags[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
		return b.SetTags(ctx, tags)
	case "set-encryption":
		e := &broker.Encryption{Identity: strings.TrimSpace(r.PostFormValue("identity")), InfrastructureEncryption: r.PostFormValue("infrastructure") == "true"}
		for _, k := range strings.Fields(r.PostFormValue("keys")) {
			e.KeyVaultKeyIDs = append(e.KeyVaultKeyIDs, k)
		}
		if len(e.KeyVaultKeyIDs) == 0 && e.Identity == "" && !e.InfrastructureEncryption {
			e = nil
		}
		return b.SetEncryption(ctx, e)
	case "clear-encryption":
		return b.SetEncryption(ctx, nil)
	}
	return fmt.Errorf("unknown action %q", r.PostFormValue("action"))
}

func (h *Handler) registerSettings() {
	h.mux.HandleFunc("GET "+Prefix+"unsupported", func(w http.ResponseWriter, r *http.Request) {
		h.render(w, r, http.StatusOK, views.UnsupportedPageView(h.layout("Not supported", views.NavUnsupported)))
	})
	h.mux.HandleFunc("GET "+Prefix+"properties", func(w http.ResponseWriter, r *http.Request) {
		h.render(w, r, http.StatusOK, views.PropertiesPageView(h.layout("Properties and locks", views.NavProps), h.propertiesPage("", "")))
	})
	h.mux.HandleFunc("POST "+Prefix+"properties/action", func(w http.ResponseWriter, r *http.Request) {
		err := h.propertiesAction(r.Context(), r)
		page := h.propertiesPage(doneMessage(r.PostFormValue("action"), err), errorMessage(err))
		status := http.StatusOK
		if err != nil {
			status = http.StatusUnprocessableEntity
		}
		h.render(w, r, status, views.PropertiesPageView(h.layout("Properties and locks", views.NavProps), page))
	})
	if h.opts.Export == nil {
		return
	}
	h.mux.HandleFunc("GET "+Prefix+"export", func(w http.ResponseWriter, r *http.Request) {
		files, err := h.opts.Export(r.Context())
		page := views.ExportPage{}
		if err != nil {
			page.Error = err.Error()
		}
		for _, f := range files {
			preview := string(f.Data)
			if len(preview) > exportPreview {
				preview = preview[:exportPreview] + "\n... (download the file for the rest)"
			}
			page.Files = append(page.Files, views.ExportFile{Name: f.Name, Size: len(f.Data), Preview: preview})
		}
		h.render(w, r, http.StatusOK, views.ExportPageView(h.layout("Export template", views.NavExport), page))
	})
	h.mux.HandleFunc("GET "+Prefix+"export/{name}", func(w http.ResponseWriter, r *http.Request) {
		files, err := h.opts.Export(r.Context())
		if err != nil {
			h.fail(w, r, err)
			return
		}
		for _, f := range files {
			if f.Name == r.PathValue("name") {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.Header().Set("Content-Disposition", `attachment; filename="`+f.Name+`"`)
				w.Write(f.Data)
				return
			}
		}
		http.NotFound(w, r)
	})
}
