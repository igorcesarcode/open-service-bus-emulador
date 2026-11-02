package integration_test

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/export"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/mcp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// TestNodeSDK runs the suite in test/node against the emulator, validating
// it with the official @azure/service-bus SDK for JavaScript. It is skipped
// when Node.js or the suite's dependencies are missing; install them with
// `npm ci` in test/node.
func TestNodeSDK(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Node.js suite")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found in PATH")
	}
	dir, err := filepath.Abs("../node")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "node_modules", "@azure", "service-bus")); err != nil {
		t.Skip("test/node dependencies are not installed; run npm ci in test/node")
	}

	sessions := broker.DefaultProperties(broker.KindQueue)
	sessions.RequiresSession = true
	// The emulator's log, shown when the suite fails: a failure on a slow
	// machine is otherwise a client error with nothing to compare against.
	log := &lockedBuffer{}
	opts := server.Options{
		Logger:   slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug - 4})),
		TLSHosts: []string{"osb-test.servicebus.windows.net"},
		Namespaces: []server.NamespaceOptions{
			{Name: "js-a", Queues: queues("shared", "only-a")},
			{Name: "js-b", Queues: queues("shared")},
			{Name: "js-c", Queues: queues("rep")},
		},
		GeoDR:      []server.GeoDRConfig{{Alias: "js-bus", Primary: "js-a", Secondary: "js-b"}},
		Prometheus: true,
		Export:     export.Files,
		Locks: []broker.Lock{
			{Name: "freeze", Level: broker.LockReadOnly, Scope: "node-frozen"},
			{Name: "keep", Level: broker.LockCanNotDelete, Scope: "node-kept"},
		},
		Queues: []server.Queue{
			{Name: "node-frozen"},
			{Name: "node-kept"},
			{Name: "node-basic"},
			{Name: "node-rad"},
			{Name: "node-batch"},
			{Name: "node-abandon"},
			queue("node-dlq", func(p *broker.Properties) { p.MaxDeliveryCount = 2 }),
			queue("node-lock", func(p *broker.Properties) { p.LockDuration = 2 * time.Second }),
			{Name: "node-defer"},
			{Name: "node-schedule"},
			{Name: "node-peek"},
			{Name: "node-subscribe"},
			{Name: "node-keepalive"},
			{Name: "node-sessions", Properties: sessions},
			{Name: "node-sessions-empty", Properties: sessions},
			{Name: "node-fwd-inbox"},
			{Name: "node-ws"},
			{Name: "node-metrics"},
			{Name: "node-portal"},
			{Name: "node-portal-in"},
			{Name: "node-tdlq"},
			{Name: "node-purge"},
			queue("node-partitioned", func(p *broker.Properties) { p.EnablePartitioning = true; p.MaxSizeInMegabytes = 1 }),
			{Name: "node-cancel-dest"},
			queue("node-cancel-front", func(p *broker.Properties) { p.ForwardTo = "node-cancel-dest" }),
			queue("node-loop-a", func(p *broker.Properties) { p.ForwardTo = "node-loop-b" }),
			queue("node-loop-b", func(p *broker.Properties) { p.ForwardTo = "node-loop-a" }),
			{Name: "node-fwd-poison"},
			queue("node-fwd-front", func(p *broker.Properties) { p.ForwardTo = "node-fwd-inbox" }),
			queue("node-fwd-work", func(p *broker.Properties) { p.ForwardDeadLetteredMessagesTo = "node-fwd-poison" }),
			queue("node-dedup", func(p *broker.Properties) { p.RequiresDuplicateDetection = true }),
			queue("node-quota", func(p *broker.Properties) { p.MaxSizeInMegabytes = 1 }),
			{Name: "node-size"},
			queue("node-idle", func(p *broker.Properties) { p.AutoDeleteOnIdle = time.Second }),
		},
		Topics: []server.Topic{
			{Name: "node-doc-filters", Subscriptions: docFilterSubscriptions()},
			{
				Name:          "node-events",
				Subscriptions: []server.Subscription{{Name: "audit"}, {Name: "billing"}},
			},
			{
				Name: "node-filters",
				Subscriptions: []server.Subscription{
					subscription("approved", []broker.Rule{sqlRule("approved", "event in ('completed', 'approved') AND isApproved = TRUE", "")}, nil),
					subscription("big", []broker.Rule{sqlRule("big", "quantity > 10 AND sys.Label LIKE 'order.%'", "")}, nil),
					subscription("eu", []broker.Rule{{Name: "eu", Filter: broker.RuleFilter{Kind: broker.FilterCorrelation, Correlation: &filter.CorrelationFilter{
						Subject: "order.created", Properties: map[string]any{"region": "eu"}}}}}, nil),
				},
			},
			{Name: "node-rules", Subscriptions: []server.Subscription{{Name: "managed"}}},
			{
				Name: "node-cancel-topic",
				Subscriptions: []server.Subscription{
					subscription("plain", nil, nil),
					subscription("acting", []broker.Rule{sqlRule("a", "1 = 1", "SET tag = 'a'"), sqlRule("b", "1 = 1", "SET tag = 'b'")}, nil),
				},
			},
		},
	}
	// Terraform entities, with $Default removed (node-tf) and kept as in
	// Azure (node-tfk).
	for _, tf := range []server.Options{terraformOptions(t, "node-tf", true), terraformOptions(t, "node-tfk", false)} {
		opts.Queues = append(opts.Queues, tf.Queues...)
		opts.Topics = append(opts.Topics, tf.Topics...)
	}
	srv := startServer(t, opts)
	// A message the engine could not transfer, for the transfer dead-letter
	// scenario: the SDKs have no way to put one there.
	if _, err := srv.Broker().TransferDeadLetter(context.Background(), "node-tdlq",
		[]broker.IncomingMessage{{Payload: mustMessage(t, "stuck")}}, "TransferFailed", "the destination is disabled"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, "--test", "--test-concurrency=1", "--test-reporter=tap", "servicebus.test.mjs")
	cmd.Dir = dir
	// A second emulator with separate AMQP and HTTP ports. Node trusts the
	// self-signed certificates of both for WebSocket and HTTPS.
	split := startSplitServer(t)
	// A third one with SAS validation, for the authorization scenarios.
	authSrv := authServer(t, auth.ModeFull, func(o *server.Options) {
		o.DevIssuer = true
		o.Assignments = []server.Assignment{
			{Principal: "owner", Role: auth.RoleOwner},
			{Principal: "sender", Role: auth.RoleSender, Scope: "orders"},
			{Principal: "receiver", Role: auth.RoleReceiver},
		}
	})
	certs := filepath.Join(t.TempDir(), "certs.pem")
	if err := os.WriteFile(certs, append(append(srv.CertificatePEM(), split.CertificatePEM()...), authSrv.CertificatePEM()...), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(),
		"OSB_CONNECTION_STRING="+srv.ConnectionString(),
		"OSB_AUTH_CONNECTION_STRING="+authSrv.ConnectionString(),
		"OSB_SPLIT_AMQP_CONNECTION_STRING="+split.ConnectionString(),
		"OSB_SPLIT_HTTP_CONNECTION_STRING="+split.HTTPConnectionString(),
		"NODE_EXTRA_CA_CERTS="+certs)
	if noEmulatorFlag() {
		cmd.Env = append(cmd.Env, "OSB_NO_EMULATOR_FLAG=1", "OSB_MAPPED_HOST="+realHost,
			"NODE_OPTIONS=--require "+filepath.Join(dir, "dns-map.cjs"))
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Node.js suite failed: %v\n%s\nemulator log around the fan-out test:\n%s", err, out, around(log.String(), "/node-events type=", 120, 40))
	}
	t.Logf("Node.js suite output:\n%s", out)
}

// TestNodeMCP runs test/node/mcp.test.mjs, which drives the MCP endpoint with
// the official @modelcontextprotocol/sdk client and checks the effects with
// @azure/service-bus. It is skipped when Node.js or the dependencies are
// missing; install them with `npm ci` in test/node.
func TestNodeMCP(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Node.js suite")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found in PATH")
	}
	dir, err := filepath.Abs("../node")
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range []string{"@azure/service-bus", "@modelcontextprotocol/sdk"} {
		if _, err := os.Stat(filepath.Join(dir, "node_modules", filepath.FromSlash(pkg))); err != nil {
			t.Skip("test/node dependencies are not installed; run npm ci in test/node")
		}
	}
	srv := startServer(t, server.Options{Export: export.Files})
	full := startMCP(t, srv)
	readOnly := startMCP(t, srv, func(o *mcp.Options) { o.ReadOnly = true })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, "--test", "--test-concurrency=1", "--test-reporter=tap", "mcp.test.mjs")
	cmd.Dir = dir
	certs := filepath.Join(t.TempDir(), "certs.pem")
	if err := os.WriteFile(certs, srv.CertificatePEM(), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(),
		"OSB_CONNECTION_STRING="+srv.ConnectionString(),
		"OSB_MCP_URL="+full.url,
		"OSB_MCP_READONLY_URL="+readOnly.url,
		"NODE_EXTRA_CA_CERTS="+certs)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Node.js MCP suite failed: %v\n%s", err, out)
	}
	t.Logf("Node.js MCP suite output:\n%s", out)
}

// TestNodeTelemetry runs a JavaScript SDK round trip against the binary with
// OpenTelemetry on, and checks that spans for it reach an OTLP receiver
// without the message body.
func TestNodeTelemetry(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Node.js suite")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found in PATH")
	}
	dir, err := filepath.Abs("../node")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "node_modules", "@azure", "service-bus")); err != nil {
		t.Skip("test/node dependencies are not installed; run npm ci in test/node")
	}
	rec := newOTLPRecorder(t)
	addr := startBinary(t, "-queue", "node-telemetry", "-otel-endpoint", rec.url, "-otel-signals", "traces")

	const secret = "NODE-SECRET-BODY-77c1"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, "--test", "--test-reporter=tap", "telemetry.test.mjs")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"OSB_CONNECTION_STRING=Endpoint=sb://"+addr+";SharedAccessKeyName=k;SharedAccessKey=v;UseDevelopmentEmulator=true",
		"OSB_SECRET_BODY="+secret)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Node.js telemetry test failed: %v\n%s", err, out)
	}
	rec.waitFor(t, "broker.send", "broker.receive", "broker.complete", "amqp.transfer")
	if rec.contains(secret) {
		t.Error("the message body was exported")
	}
}

// around returns the lines of s from before lines ahead of the first one that
// contains needle to after lines past it; the last lines when none does.
func around(s, needle string, before, after int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	at := len(lines) - 1
	for i, l := range lines {
		if strings.Contains(l, needle) {
			at = i
			break
		}
	}
	return strings.Join(lines[max(0, at-before):min(len(lines), at+after)], "\n")
}
