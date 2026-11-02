package integration_test

import (
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/terraform"
)

// terraformOptions declares the entities of testdata/terraform with the
// given name prefix.
func terraformOptions(t *testing.T, prefix string, drop bool) server.Options {
	t.Helper()
	r, err := terraform.Load("testdata/terraform", terraform.Options{Vars: map[string]string{"prefix": prefix}, DropDefaultRule: drop})
	if err != nil {
		t.Fatalf("terraform.Load() error: %v", err)
	}
	return server.Options{Queues: r.Config.Queues, Topics: r.Config.Topics}
}

// regionEvents are sent to the events topic of testdata/terraform.
func regionEvents() []*azservicebus.Message {
	return []*azservicebus.Message{
		{Body: []byte("eu"), ApplicationProperties: map[string]any{"region": "eu"}},
		{Body: []byte("us"), ApplicationProperties: map[string]any{"region": "us"}},
		{Body: []byte("big"), Subject: to.Ptr("big"), ApplicationProperties: map[string]any{"size": "L"}},
	}
}

func TestTerraformEntities(t *testing.T) {
	t.Parallel()
	srv := startServer(t, terraformOptions(t, "go-tf", true))
	client := newClient(t, srv)
	ctx := testContext(t)

	q, err := newAdminClient(t, srv).GetQueue(ctx, "go-tf-inbox", nil)
	if err != nil || q == nil {
		t.Fatalf("GetQueue() = %v, %v", q, err)
	}
	if *q.LockDuration != "PT45S" || *q.MaxDeliveryCount != 7 {
		t.Errorf("inbox LockDuration = %s, MaxDeliveryCount = %d, want PT45S and 7", *q.LockDuration, *q.MaxDeliveryCount)
	}

	send(t, newSender(t, client, "go-tf-events"), regionEvents()...)
	eu := receive(t, newSubReceiver(t, client, "go-tf-events", "eu", nil), 1)
	if string(eu[0].Body) != "eu" || eu[0].ApplicationProperties["routed"] != true {
		t.Errorf("eu received %q with properties %v, want eu routed by the rule action", eu[0].Body, eu[0].ApplicationProperties)
	}
	if diff := cmp.Diff([]string{"big"}, bodiesOf(receive(t, newReceiver(t, client, "go-tf-inbox", nil), 1))); diff != "" {
		t.Errorf("forwarded messages mismatch (-want +got):\n%s", diff)
	}
}

// TestTerraformKeepsDefaultRule checks the Azure behavior: declared rules
// do not remove $Default, so every message reaches the subscription. The
// eu event comes twice: once for $Default and once for the rule with an
// action, which makes its own copy.
func TestTerraformKeepsDefaultRule(t *testing.T) {
	t.Parallel()
	srv := startServer(t, terraformOptions(t, "go-tfk", false))
	client := newClient(t, srv)
	send(t, newSender(t, client, "go-tfk-events"), regionEvents()...)
	r := newSubReceiver(t, client, "go-tfk-events", "eu", nil)
	if diff := cmp.Diff([]string{"big", "eu", "eu", "us"}, sortedBodies(receive(t, r, 4))); diff != "" {
		t.Errorf("eu received mismatch (-want +got):\n%s", diff)
	}
	receiveNone(t, r, 300*time.Millisecond)
}
