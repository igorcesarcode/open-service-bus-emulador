package integration_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func portalReq(t *testing.T, srv *server.Server, method, path, contentType, body string, headers map[string]string) (int, string, *http.Response) {
	t.Helper()
	req, _ := http.NewRequest(method, "http://"+srv.HTTPAddr().String()+path, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp
}

func TestPortalGeoPagesAndSwitcher(t *testing.T) {
	t.Parallel()
	srv := geoDRServer(t)
	origin := map[string]string{"Origin": "http://" + srv.HTTPAddr().String()}

	// The pages of each namespace, and the switcher between them.
	_, page, _ := portalReq(t, srv, "GET", "/$portal/geo-dr", "", "", nil)
	for _, want := range []string{"<h1>primary</h1>", "Geo-Recovery", "bus", "Paired", "(primary)", `name="ns"`, ">secondary</option>", "Initiate pairing"} {
		if !strings.Contains(page, want) {
			t.Errorf("geo-dr page lacks %q", want)
		}
	}
	_, other, resp := portalReq(t, srv, "GET", "/$portal/geo-dr?ns=secondary", "", "", nil)
	if !strings.Contains(other, "<h1>secondary</h1>") || !strings.Contains(other, "(secondary)") {
		t.Errorf("the secondary's page = %s", other)
	}
	var cookie *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == "osb_ns" {
			cookie = c
		}
	}
	if cookie == nil || cookie.Value != "secondary" {
		t.Fatalf("namespace cookie = %+v", cookie)
	}
	// The choice sticks for the next pages.
	_, queues, _ := portalReq(t, srv, "GET", "/$portal/queues", "", "", map[string]string{"Cookie": "osb_ns=secondary"})
	if !strings.Contains(queues, "<h1>secondary</h1>") || !strings.Contains(queues, "orders") {
		t.Errorf("queues of the chosen namespace = %s", queues)
	}
	// The overview of the alias's host selects by Host when nothing chose.
	_, ov, _ := func() (int, string, *http.Response) {
		req, _ := http.NewRequest("GET", "http://"+srv.HTTPAddr().String()+"/$portal/", nil)
		req.Host = "bus.localhost"
		resp, _ := http.DefaultClient.Do(req)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, string(b), resp
	}()
	if !strings.Contains(ov, "<h1>primary</h1>") {
		t.Errorf("the alias's host did not select the primary: %s", ov)
	}

	// Failover from the page: the alias moves and the page says so.
	status, body, _ := portalReq(t, srv, "POST", "/$portal/geo-dr/action", "application/x-www-form-urlencoded", url.Values{"action": {"failover"}, "alias": {"bus"}}.Encode(), origin)
	if status != 200 || !strings.Contains(body, "Done: failover") || !strings.Contains(body, "Not paired") {
		t.Errorf("failover = %d %s", status, body)
	}
	if st := srv.GeoDRAliases(); len(st) != 1 || st[0].Primary != "secondary" {
		t.Errorf("after the failover GeoDRAliases() = %+v", st)
	}
	status, body, _ = portalReq(t, srv, "POST", "/$portal/geo-dr/action", "application/x-www-form-urlencoded", url.Values{"action": {"failover"}, "alias": {"bus"}}.Encode(), origin)
	if status != http.StatusUnprocessableEntity || !strings.Contains(body, "not paired") {
		t.Errorf("a second failover = %d %s", status, body)
	}
	// A form from another site is refused.
	status, _, _ = portalReq(t, srv, "POST", "/$portal/geo-dr/action", "application/x-www-form-urlencoded", url.Values{"action": {"delete"}, "alias": {"bus"}}.Encode(), nil)
	if status != http.StatusForbidden {
		t.Errorf("a post without an origin = %d, want 403", status)
	}
}

func TestPortalGeoAPI(t *testing.T) {
	t.Parallel()
	srv := geoDRServer(t)
	call := func(method, path string, body any) (int, map[string]any) {
		t.Helper()
		var payload string
		if body != nil {
			b, _ := json.Marshal(body)
			payload = string(b)
		}
		ct := ""
		if body != nil {
			ct = "application/json"
		}
		status, text, _ := portalReq(t, srv, method, path, ct, payload, nil)
		out := map[string]any{}
		json.Unmarshal([]byte(text), &out)
		return status, out
	}

	status, geo := call("GET", "/$portal/api/geo", nil)
	if status != 200 || geo["namespace"] != "primary" || len(geo["geoDR"].([]any)) != 1 || geo["geoReplication"].(map[string]any)["enabled"] != false {
		t.Fatalf("GET api/geo = %d %v", status, geo)
	}
	// Geo-DR through the API: break, delete, pair anew.
	for _, in := range []map[string]any{{"action": "break", "alias": "bus"}, {"action": "delete", "alias": "bus"}} {
		if status, out := call("POST", "/$portal/api/geo-dr", in); status != 200 {
			t.Fatalf("%v = %d %v", in, status, out)
		}
	}
	if status, out := call("POST", "/$portal/api/geo-dr", map[string]any{"action": "pair", "alias": "again", "primary": "primary", "secondary": "spare"}); status != 200 {
		t.Fatalf("pair = %d %v", status, out)
	}
	if status, out := call("POST", "/$portal/api/geo-dr", map[string]any{"action": "pair", "alias": "again", "primary": "primary", "secondary": "secondary"}); status != http.StatusBadRequest || out["error"] == nil {
		t.Errorf("pairing a paired primary = %d %v", status, out)
	}
	if status, _ := call("POST", "/$portal/api/geo-dr", map[string]any{"action": "teleport"}); status != http.StatusBadRequest {
		t.Errorf("an unknown action = %d, want 400", status)
	}
	// A plain form to the API is not accepted without an origin.
	if status, _, _ := portalReq(t, srv, "POST", "/$portal/api/geo-dr", "application/x-www-form-urlencoded", "action=delete&alias=again", nil); status != http.StatusForbidden {
		t.Errorf("a form post to the API = %d, want 403", status)
	}

	// Geo-Replication through the API: not on a paired namespace.
	if status, out := call("POST", "/$portal/api/geo-replication", map[string]any{"action": "enable", "synchronous": true}); status != http.StatusBadRequest {
		t.Errorf("enabling on a paired namespace = %d %v", status, out)
	}
	if status, out := call("POST", "/$portal/api/geo-replication", map[string]any{"action": "promote"}); status != http.StatusBadRequest {
		t.Errorf("promoting without replication = %d %v", status, out)
	}
}

func TestPortalGeoReplicationPage(t *testing.T) {
	t.Parallel()
	srv, ns := geoRepServer(t, server.GeoReplication{Synchronous: true, PrimaryRegion: "west", SecondaryRegion: "north"})
	origin := map[string]string{"Origin": "http://" + srv.HTTPAddr().String()}
	_, page, _ := portalReq(t, srv, "GET", "/$portal/geo-replication", "", "", nil)
	for _, want := range []string{"Geo-Replication", "Synchronous", "west", "north (Ready)", "Planned promotion", "Forced promotion", "pending changes"} {
		if !strings.Contains(page, want) {
			t.Errorf("geo-replication page lacks %q", want)
		}
	}
	status, body, _ := portalReq(t, srv, "POST", "/$portal/geo-replication/action", "application/x-www-form-urlencoded", url.Values{"action": {"promote"}}.Encode(), origin)
	if status != 200 || !strings.Contains(body, "Done: promote") {
		t.Fatalf("promote = %d %s", status, body)
	}
	if st := ns.GeoReplicationStatus(); st.PrimaryRegion != "north" {
		t.Errorf("after promoting the primary region is %q", st.PrimaryRegion)
	}
	waitReady(t, ns)
	_, status2, _ := portalReq(t, srv, "GET", "/$portal/geo-replication/status", "", "", nil)
	if !strings.Contains(status2, "Replication lag") || strings.Contains(status2, "<html") {
		t.Errorf("status fragment = %s", status2)
	}
	// Remove the secondary, then add one with the form.
	status, body, _ = portalReq(t, srv, "POST", "/$portal/geo-replication/action", "application/x-www-form-urlencoded", url.Values{"action": {"disable"}}.Encode(), origin)
	if status != 200 || !strings.Contains(body, "does not replicate its data") {
		t.Errorf("disable = %d %s", status, body)
	}
	form := url.Values{"action": {"enable"}, "primary": {"a"}, "secondary": {"b"}, "maxlag": {"30s"}, "delay": {"100ms"}}
	status, body, _ = portalReq(t, srv, "POST", "/$portal/geo-replication/action", "application/x-www-form-urlencoded", form.Encode(), origin)
	if status != 200 {
		t.Fatalf("enable = %d %s", status, body)
	}
	if st := ns.GeoReplicationStatus(); !st.Enabled || st.Synchronous || st.MaxLag != 30*time.Second || st.Delay != 100*time.Millisecond || st.PrimaryRegion != "a" {
		t.Errorf("status after enabling = %+v", st)
	}
	form.Set("maxlag", "soon")
	if status, body, _ := portalReq(t, srv, "POST", "/$portal/geo-replication/action", "application/x-www-form-urlencoded", form.Encode(), origin); status != http.StatusUnprocessableEntity {
		t.Errorf("a bad lag = %d %s", status, body)
	}
}
