package portal

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

func (h *Handler) registerAccess() {
	h.mux.HandleFunc("GET "+Prefix+"policies", h.policiesPage)
	h.mux.HandleFunc("POST "+Prefix+"policies", h.createPolicy)
	h.mux.HandleFunc("POST "+Prefix+"policies/regenerate", h.regenerateKey)
	h.mux.HandleFunc("POST "+Prefix+"policies/delete", h.deletePolicy)
	h.mux.HandleFunc("GET "+Prefix+"access", h.accessPage)
	h.mux.HandleFunc("POST "+Prefix+"access", h.addAssignment)
	h.mux.HandleFunc("POST "+Prefix+"access/delete", h.deleteAssignment)
}

// authority is the namespace's authority, or one that checks nothing when
// the portal runs without one.
func (h *Handler) authority(w http.ResponseWriter, r *http.Request) *auth.Authority {
	if h.opts.Auth == nil {
		http.Error(w, "this portal has no policies to manage", http.StatusNotFound)
		return nil
	}
	return h.opts.Auth
}

// scopeOptions lists the namespace and every queue and topic.
func (h *Handler) scopeOptions() []views.Option {
	out := []views.Option{{Value: "", Label: "Namespace"}}
	for _, kind := range []broker.EntityKind{broker.KindQueue, broker.KindTopic} {
		for _, e := range h.opts.Broker.Entities(kind, "") {
			out = append(out, views.Option{Value: e.Path, Label: string(kind) + ": " + e.Name})
		}
	}
	return out
}

func scopeLabel(scope string) string {
	if scope == "" {
		return "Namespace"
	}
	return scope
}

func (h *Handler) policiesData(a *auth.Authority, errMsg string) views.PoliciesPage {
	p := views.PoliciesPage{AuthMode: string(a.Mode()), Scopes: h.scopeOptions(), Error: errMsg}
	conn := h.opts.Info().ConnectionStr
	for _, pol := range a.Policies("", true) {
		cs := strings.Replace(conn, "SharedAccessKeyName=RootManageSharedAccessKey", "SharedAccessKeyName="+pol.Name, 1)
		cs = strings.Replace(cs, "SharedAccessKey=SAS_KEY_VALUE", "SharedAccessKey="+pol.PrimaryKey, 1)
		p.Rows = append(p.Rows, views.PolicyRow{
			Scope: pol.Scope, ScopeLabel: scopeLabel(pol.Scope), Name: pol.Name, Rights: strings.Join(pol.Rights.Normalize().Names(), ", "),
			PrimaryKey: pol.PrimaryKey, SecondaryKey: pol.SecondaryKey, Connection: cs,
		})
	}
	return p
}

func (h *Handler) policiesPage(w http.ResponseWriter, r *http.Request) {
	a := h.authority(w, r)
	if a == nil {
		return
	}
	h.render(w, r, http.StatusOK, views.PoliciesPageView(h.layout("Shared access policies", views.NavPolicies), h.policiesData(a, "")))
}

func (h *Handler) createPolicy(w http.ResponseWriter, r *http.Request) {
	a := h.authority(w, r)
	if a == nil {
		return
	}
	r.ParseForm()
	rights, err := auth.ParseRights(r.PostForm["rights"])
	if err == nil {
		err = a.SavePolicy(r.Context(), auth.Policy{Scope: r.PostFormValue("scope"), Name: strings.TrimSpace(r.PostFormValue("name")), Rights: rights})
	}
	if err != nil {
		h.record(r, "CreatePolicy", policyEntity(r.PostFormValue("scope"), r.PostFormValue("name")), err)
		h.render(w, r, http.StatusUnprocessableEntity, views.PoliciesPageView(h.layout("Shared access policies", views.NavPolicies), h.policiesData(a, err.Error())))
		return
	}
	h.record(r, "CreatePolicy", policyEntity(r.PostFormValue("scope"), r.PostFormValue("name")), nil)
	http.Redirect(w, r, Prefix+"policies", http.StatusSeeOther)
}

// record logs an access change in the activity log, as the portal. Keys
// never go in the log.
func (h *Handler) record(r *http.Request, op, entity string, err error) {
	h.opts.Broker.Record(broker.WithActor(r.Context(), Actor), op, entity, err, "")
}

func policyEntity(scope, name string) string {
	if scope == "" {
		return "policies/" + name
	}
	return scope + "/policies/" + name
}

func (h *Handler) regenerateKey(w http.ResponseWriter, r *http.Request) {
	a := h.authority(w, r)
	if a == nil {
		return
	}
	_, err := a.RegenerateKey(r.Context(), r.PostFormValue("scope"), r.PostFormValue("name"), r.PostFormValue("which") == "secondary")
	h.record(r, "RegeneratePolicyKey", policyEntity(r.PostFormValue("scope"), r.PostFormValue("name")), err)
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	h.render(w, r, http.StatusOK, views.PolicyRows(h.policiesData(a, msg)))
}

func (h *Handler) deletePolicy(w http.ResponseWriter, r *http.Request) {
	a := h.authority(w, r)
	if a == nil {
		return
	}
	msg := ""
	err := a.DeletePolicy(r.Context(), r.PostFormValue("scope"), r.PostFormValue("name"))
	h.record(r, "DeletePolicy", policyEntity(r.PostFormValue("scope"), r.PostFormValue("name")), err)
	if err != nil {
		msg = err.Error()
	}
	h.render(w, r, http.StatusOK, views.PolicyRows(h.policiesData(a, msg)))
}

func (h *Handler) accessData(a *auth.Authority, errMsg string) views.AccessPage {
	p := views.AccessPage{AuthMode: string(a.Mode()), Scopes: h.scopeOptions(), Error: errMsg, Issuer: a.Mode() == auth.ModeFull}
	for _, role := range []auth.Role{auth.RoleOwner, auth.RoleSender, auth.RoleReceiver} {
		p.Roles = append(p.Roles, string(role))
	}
	for _, x := range a.Assignments() {
		p.Rows = append(p.Rows, views.AssignmentRow{ID: x.ID, Principal: x.Principal, Role: string(x.Role), ScopeLabel: scopeLabel(x.Scope)})
	}
	return p
}

func (h *Handler) accessPage(w http.ResponseWriter, r *http.Request) {
	a := h.authority(w, r)
	if a == nil {
		return
	}
	h.render(w, r, http.StatusOK, views.AccessPageView(h.layout("Access control (IAM)", views.NavAccess), h.accessData(a, "")))
}

func (h *Handler) addAssignment(w http.ResponseWriter, r *http.Request) {
	a := h.authority(w, r)
	if a == nil {
		return
	}
	role, err := auth.ParseRole(r.PostFormValue("role"))
	if err == nil {
		err = a.AddAssignment(r.Context(), strings.TrimSpace(r.PostFormValue("principal")), role, r.PostFormValue("scope"))
	}
	h.record(r, "CreateRoleAssignment", "access/"+r.PostFormValue("principal"), err)
	if err != nil {
		h.render(w, r, http.StatusUnprocessableEntity, views.AccessPageView(h.layout("Access control (IAM)", views.NavAccess), h.accessData(a, err.Error())))
		return
	}
	http.Redirect(w, r, Prefix+"access", http.StatusSeeOther)
}

func (h *Handler) deleteAssignment(w http.ResponseWriter, r *http.Request) {
	a := h.authority(w, r)
	if a == nil {
		return
	}
	id, _ := strconv.ParseInt(r.PostFormValue("id"), 10, 64)
	msg := ""
	err := a.DeleteAssignment(r.Context(), id)
	h.record(r, "DeleteRoleAssignment", "access/"+r.PostFormValue("id"), err)
	if err != nil {
		msg = err.Error()
	}
	h.render(w, r, http.StatusOK, views.AssignmentRows(h.accessData(a, msg)))
}
