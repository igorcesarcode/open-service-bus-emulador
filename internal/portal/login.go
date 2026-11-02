package portal

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

const (
	sessionCookie = "osb_portal"
	sessionTTL    = 8 * time.Hour
)

// public reports whether a request needs no login: the login page and the
// assets it uses.
func (h *Handler) public(r *http.Request) bool {
	p := r.URL.Path
	return p == Prefix+"login" || strings.HasPrefix(p, Prefix+"assets/")
}

func (h *Handler) loggedIn(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return false
	}
	h.sessionMu.Lock()
	defer h.sessionMu.Unlock()
	exp, ok := h.sessions[c.Value]
	if ok && h.opts.Now().After(exp) {
		delete(h.sessions, c.Value)
		return false
	}
	return ok
}

func (h *Handler) loginPage(w http.ResponseWriter, r *http.Request) {
	if !h.opts.Auth.Enabled() {
		http.Redirect(w, r, Prefix, http.StatusSeeOther)
		return
	}
	h.render(w, r, http.StatusOK, views.LoginPage(h.opts.Info().Name, ""))
}

// login accepts the name and key of a namespace policy with Manage, given
// separately or as a connection string.
func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	name, key := strings.TrimSpace(r.PostFormValue("name")), strings.TrimSpace(r.PostFormValue("key"))
	if cs := strings.TrimSpace(r.PostFormValue("connection")); cs != "" {
		for _, part := range strings.Split(cs, ";") {
			k, v, _ := strings.Cut(part, "=")
			switch strings.TrimSpace(k) {
			case "SharedAccessKeyName":
				name = v
			case "SharedAccessKey":
				// A key may end in "=", which Cut keeps.
				key = v
			}
		}
	}
	if err := h.opts.Auth.Login(name, key); err != nil {
		h.render(w, r, http.StatusUnauthorized, views.LoginPage(h.opts.Info().Name, "The policy or key is not valid, or it lacks Manage."))
		return
	}
	var b [24]byte
	rand.Read(b[:])
	id := hex.EncodeToString(b[:])
	h.sessionMu.Lock()
	h.sessions[id] = h.opts.Now().Add(sessionTTL)
	h.sessionMu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: id, Path: Prefix, HttpOnly: true, SameSite: http.SameSiteStrictMode,
		MaxAge: int(sessionTTL / time.Second),
	})
	http.Redirect(w, r, Prefix, http.StatusSeeOther)
}

func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		h.sessionMu.Lock()
		delete(h.sessions, c.Value)
		h.sessionMu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: Prefix, MaxAge: -1})
	http.Redirect(w, r, Prefix+"login", http.StatusSeeOther)
}
