package integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/export"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// TestDotnetSDK runs the program in test/dotnet against the emulator,
// validating the emulator with the official Azure.Messaging.ServiceBus SDK for
// .NET: transactions, send via and batch delete (which only it has), and the
// messaging, sessions, rules, admin, partitioning, namespace, lock and export
// scenarios the Go and JavaScript suites also run. It is skipped when dotnet
// is missing.
func TestDotnetSDK(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the .NET program")
	}
	// OSB_DOTNET_DOCKER=1 runs the program in the official SDK image, so no
	// .NET is needed on the machine.
	inDocker := os.Getenv("OSB_DOTNET_DOCKER") != ""
	dotnet, err := exec.LookPath("dotnet")
	if err != nil && !inDocker {
		t.Skip("dotnet not found in PATH; set OSB_DOTNET_DOCKER=1 to use a container")
	}
	dir, err := filepath.Abs("../dotnet")
	if err != nil {
		t.Fatal(err)
	}
	sessions := broker.DefaultProperties(broker.KindQueue)
	sessions.RequiresSession = true
	srv := startServer(t, server.Options{
		Export:   export.Files,
		TLSHosts: []string{"dn-bus.servicebus.windows.net", realHost},
		Queues: append(queues("dn-input", "dn-output", "dn-tx-send", "dn-purge", "dn-basic", "dn-rad", "dn-batch", "dn-abandon",
			"dn-defer", "dn-schedule", "dn-peek", "dn-processor", "dn-fwd-inbox", "dn-frozen", "dn-kept"),
			queue("dn-dlq", func(p *broker.Properties) { p.MaxDeliveryCount = 2 }),
			queue("dn-lock", func(p *broker.Properties) { p.LockDuration = 2 * time.Second }),
			queue("dn-dedup", func(p *broker.Properties) { p.RequiresDuplicateDetection = true }),
			queue("dn-quota", func(p *broker.Properties) { p.MaxSizeInMegabytes = 1 }),
			queue("dn-partitioned", func(p *broker.Properties) { p.EnablePartitioning = true; p.MaxSizeInMegabytes = 1 }),
			queue("dn-fwd-front", func(p *broker.Properties) { p.ForwardTo = "dn-fwd-inbox" }),
			server.Queue{Name: "dn-sessions", Properties: sessions},
			server.Queue{Name: "dn-sessions-empty", Properties: sessions},
		),
		Topics: []server.Topic{{Name: "dn-events", Subscriptions: []server.Subscription{{Name: "audit"}, {Name: "billing"}}}},
		Locks: []broker.Lock{
			{Name: "freeze", Level: broker.LockReadOnly, Scope: "dn-frozen"},
			{Name: "keep", Level: broker.LockCanNotDelete, Scope: "dn-kept"},
		},
		Namespaces: []server.NamespaceOptions{
			{Name: "dn-a", Queues: queues("dn-shared")},
			{Name: "dn-b", Queues: queues("dn-shared")},
		},
		GeoDR: []server.GeoDRConfig{{Alias: "dn-bus", Primary: "dn-a", Secondary: "dn-b"}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, dotnet, "run", "--project", dir)
	// The scenarios that present another namespace's host name use TLS and
	// trust the emulator's certificate.
	certFile := filepath.Join(t.TempDir(), "emulator.pem")
	if err := os.WriteFile(certFile, srv.CertificatePEM(), 0o644); err != nil {
		t.Fatal(err)
	}
	env := []string{"OSB_CONNECTION_STRING=" + srv.ConnectionString(), "DOTNET_NOLOGO=1", "SSL_CERT_FILE=" + certFile}
	if noEmulatorFlag() {
		// Connect as a client of the real service does: its name, no
		// UseDevelopmentEmulator, TLS (see Ctx.NewClient in test/dotnet).
		env = append(env, "OSB_NO_EMULATOR_FLAG=1")
	}
	if inDocker {
		// A copy of the program, so the container's build output stays out of
		// the tree. The host network reaches the emulator on loopback, and the
		// namespace's name resolves to it.
		src := filepath.Join(t.TempDir(), "src")
		if out, err := exec.Command("sh", "-c", "mkdir -p "+src+" && cd "+dir+" && tar --exclude=bin --exclude=obj -cf - . | tar -xf - -C "+src).CombinedOutput(); err != nil {
			t.Fatalf("copying %s: %v\n%s", dir, err, out)
		}
		args := []string{"run", "--rm", "--network", "host", "--add-host", realHost + ":127.0.0.1",
			"-v", src + ":/src", "-v", certFile + ":/certs/ca.pem:ro", "-v", "osb-dotnet-nuget:/root/.nuget",
			"-e", "SSL_CERT_FILE=/certs/ca.pem", "-e", "DOTNET_NOLOGO=1", "-e", "DOTNET_CLI_TELEMETRY_OPTOUT=1",
			"-e", "OSB_CONNECTION_STRING=" + srv.ConnectionString()}
		if noEmulatorFlag() {
			args = append(args, "-e", "OSB_NO_EMULATOR_FLAG=1")
		}
		args = append(args, "mcr.microsoft.com/dotnet/sdk:8.0", "sh", "-c",
			// The build output belongs to root; hand it back so the test can
			// remove it.
			fmt.Sprintf("dotnet run --project /src; rc=$?; chown -R %d:%d /src; exit $rc", os.Getuid(), os.Getgid()))
		cmd = exec.CommandContext(ctx, "docker", args...)
	} else {
		cmd.Env = append(cmd.Environ(), env...)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf(".NET program failed: %v\n%s", err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "FAIL") {
			t.Errorf("%s", line)
		}
	}
	t.Logf(".NET program output:\n%s", out)
}
