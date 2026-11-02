package integration_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/export"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func TestPortalPropertiesAndExport(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders"), Export: export.Files})
	hx := map[string]string{"HX-Request": "true"}
	post := func(form url.Values) (int, string) {
		status, body, _ := portalReq(t, srv, "POST", "/$portal/properties/action", "application/x-www-form-urlencoded", form.Encode(), hx)
		return status, body
	}

	if status, body, _ := portalReq(t, srv, "GET", "/$portal/properties", "", "", nil); status != http.StatusOK || !strings.Contains(body, "No locks.") {
		t.Fatalf("GET properties = %d\n%s", status, body)
	}
	if status, body := post(url.Values{"action": {"add-lock"}, "name": {"keep"}, "level": {"CanNotDelete"}, "scope": {"orders"}, "notes": {"prod"}}); status != http.StatusOK || !strings.Contains(body, "keep") {
		t.Fatalf("add-lock = %d\n%s", status, body)
	}
	if err := srv.Broker().DeleteEntity(testContext(t), "orders"); err == nil {
		t.Error("DeleteEntity(orders) succeeded under the lock added in the portal")
	}
	if status, body := post(url.Values{"action": {"add-lock"}, "name": {"bad"}, "level": {"Write"}}); status != http.StatusUnprocessableEntity || !strings.Contains(body, "CanNotDelete or ReadOnly") {
		t.Errorf("add-lock with a bad level = %d\n%s", status, body)
	}
	if status, _ := post(url.Values{"action": {"set-tags"}, "tags": {"env=dev\nteam = billing\n"}}); status != http.StatusOK {
		t.Errorf("set-tags = %d", status)
	}
	if status, body := post(url.Values{"action": {"set-tags"}, "tags": {"nonsense"}}); status != http.StatusUnprocessableEntity || !strings.Contains(body, "name=value") {
		t.Errorf("set-tags with a bad line = %d\n%s", status, body)
	}
	if status, _ := post(url.Values{"action": {"set-encryption"}, "keys": {"https://vault/keys/k"}, "identity": {"id"}, "infrastructure": {"true"}}); status != http.StatusOK {
		t.Errorf("set-encryption = %d", status)
	}
	m := srv.Broker().Meta()
	if m.Tags["team"] != "billing" || m.Encryption == nil || !m.Encryption.InfrastructureEncryption || len(m.Locks) != 1 {
		t.Fatalf("Meta() = %+v", m)
	}

	status, body, _ := portalReq(t, srv, "GET", "/$portal/export", "", "", nil)
	if status != http.StatusOK || !strings.Contains(body, "Config.json") || !strings.Contains(body, "azurerm_management_lock") {
		t.Fatalf("GET export = %d\n%s", status, body)
	}
	status, body, resp := portalReq(t, srv, "GET", "/$portal/export/main.tf", "", "", nil)
	if status != http.StatusOK || !strings.Contains(resp.Header.Get("Content-Disposition"), "main.tf") || !strings.Contains(body, `lock_level = "CanNotDelete"`) {
		t.Errorf("GET export/main.tf = %d %q\n%s", status, resp.Header.Get("Content-Disposition"), body)
	}
	if status, _, _ := portalReq(t, srv, "GET", "/$portal/export/nope", "", "", nil); status != http.StatusNotFound {
		t.Errorf("GET export/nope = %d, want 404", status)
	}

	if status, _ := post(url.Values{"action": {"delete-lock"}, "name": {"keep"}}); status != http.StatusOK {
		t.Errorf("delete-lock = %d", status)
	}
	if status, _ := post(url.Values{"action": {"clear-encryption"}}); status != http.StatusOK || srv.Broker().Meta().Encryption != nil {
		t.Errorf("clear-encryption = %d", status)
	}
}
