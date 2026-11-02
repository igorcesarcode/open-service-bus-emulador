package integration_test

import (
	"crypto/tls"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/unsupported"
)

const clientAffineSubscription = `<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><IsClientAffine>true</IsClientAffine><ClientAffineProperties><ClientId>jms-1</ClientId><IsDurable>true</IsDurable><IsShared>false</IsShared></ClientAffineProperties></SubscriptionDescription></content></entry>`

func TestUnsupportedFeaturesSay(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Topics: []server.Topic{{Name: "events"}}})
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}

	// A JMS client-scoped subscription is refused, not created as an
	// ordinary one.
	req, _ := http.NewRequest(http.MethodPut, "https://"+srv.HTTPAddr().String()+"/events/Subscriptions/jms", strings.NewReader(clientAffineSubscription))
	req.Header.Set("Content-Type", "application/atom+xml")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("creating a client-scoped subscription = %d, want 400", resp.StatusCode)
	}
	if _, err := srv.Broker().Entity("events/subscriptions/jms"); err == nil {
		t.Error("the client-scoped subscription was created")
	}
	// The failure is in the activity log, with the reason.
	status, body, _ := portalReq(t, srv, "GET", "/$portal/activity", "", "", nil)
	if status != http.StatusOK || !strings.Contains(body, "JMS client-scoped subscriptions") {
		t.Errorf("the activity log does not say why (%d)", status)
	}

	// Every left-out feature is named in the portal's menu and on its page.
	status, body, _ = portalReq(t, srv, "GET", "/$portal/unsupported", "", "", nil)
	if status != http.StatusOK {
		t.Fatalf("GET unsupported = %d", status)
	}
	for _, f := range unsupported.All() {
		if !strings.Contains(body, f.Name) || !strings.Contains(body, `id="`+f.ID+`"`) {
			t.Errorf("the page lacks %s", f.Name)
		}
	}
	_, home, _ := portalReq(t, srv, "GET", "/$portal/", "", "", nil)
	if !strings.Contains(home, "Private Link") || !strings.Contains(home, "Migrate to premium") {
		t.Error("the menu does not list the unsupported features")
	}
}

// TestBinaryLogsUnsupportedFeatures starts the command on a Terraform that
// declares an IP rule and a private endpoint: it loads the rest and says
// what it left out.
func TestBinaryLogsUnsupportedFeatures(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	t.Parallel()
	dir := t.TempDir()
	tf := `
resource "azurerm_servicebus_namespace" "ns" {
  name                = "orders"
  resource_group_name = "rg"
  location            = "westeurope"
  sku                 = "Premium"
  network_rule_set {
    default_action = "Deny"
    ip_rules       = ["1.2.3.4"]
  }
}
resource "azurerm_servicebus_queue" "q" {
  name         = "work"
  namespace_id = azurerm_servicebus_namespace.ns.id
}
`
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(tf), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(buildBinary(t), "-terraform", dir, "-export", filepath.Join(dir, "out")).CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "level=WARN") || !strings.Contains(string(out), "unsupported=ip-filter") {
		t.Errorf("the log does not say the IP rule was left out:\n%s", out)
	}
	if data, err := os.ReadFile(filepath.Join(dir, "out", "Config.json")); err != nil || !strings.Contains(string(data), `"work"`) {
		t.Errorf("the queue was not loaded: %v", err)
	}
}
