package integration_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// TestBinarySmoke builds the command, runs it and sends a message to it.
func TestBinarySmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	t.Parallel()
	client := runBinary(t, "-queue", "orders,payments")
	ctx := testContext(t)
	for _, q := range []string{"orders", "payments"} {
		sender, err := client.NewSender(q, nil)
		if err != nil {
			t.Fatalf("NewSender(%q) error: %v", q, err)
		}
		if err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("smoke")}, nil); err != nil {
			t.Errorf("SendMessage(%q) error: %v", q, err)
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error: %v", err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func waitForPort(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("binary did not listen on %s", addr)
}

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// buildBinary builds the command once and returns its path.
// exeSuffix is the extension of executables on this system: Windows will not
// run a file without .exe.
var exeSuffix = func() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}()

func buildBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "osb-bin")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "open-service-bus"+exeSuffix)
		build := exec.Command("go", "build", "-o", binPath, "../../cmd/open-service-bus")
		build.Env = append(build.Environ(), "CGO_ENABLED=0")
		if out, err := build.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build error: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

// runBinary builds the command once, runs it with args and returns a
// client connected to it.
func runBinary(t *testing.T, args ...string) *azservicebus.Client {
	t.Helper()
	addr := startBinary(t, args...)
	client, err := azservicebus.NewClientFromConnectionString(
		fmt.Sprintf("Endpoint=sb://%s;SharedAccessKeyName=k;SharedAccessKey=v;UseDevelopmentEmulator=true", addr), nil)
	if err != nil {
		t.Fatalf("NewClientFromConnectionString() error: %v", err)
	}
	t.Cleanup(func() { client.Close(context.Background()) })
	return client
}

// startBinary starts the command with args and returns the address it
// listens on, once it accepts connections.
func startBinary(t *testing.T, args ...string) string {
	t.Helper()
	bin := buildBinary(t)
	addr := freeAddr(t)
	cmd := exec.Command(bin, append([]string{"-listen", addr, "-log-level", "error"}, args...)...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting binary: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
		if t.Failed() {
			t.Logf("binary output:\n%s", stderr.String())
		}
	})
	waitForPort(t, addr)
	return addr
}

// TestBinaryPrivateModuleTerraform runs the command on the private-module
// main.tf, whose private module is read by the adapter, and checks that
// a subscription filters with its sql_filter.
func TestBinaryPrivateModuleTerraform(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	t.Parallel()
	client := runBinary(t, "-terraform", "testdata/private-module/main.tf")
	send(t, newSender(t, client, "orders"),
		&azservicebus.Message{Body: []byte("process"), ApplicationProperties: map[string]any{"event": "process"}},
		&azservicebus.Message{Body: []byte("report"), ApplicationProperties: map[string]any{"event": "report"}},
	)
	control := newSubReceiver(t, client, "orders", "control-order", nil)
	if got := bodiesOf(receive(t, control, 1)); got[0] != "process" {
		t.Errorf("control-order received %q, want process", got)
	}
	receiveNone(t, control, 300*time.Millisecond)
	if got := bodiesOf(receive(t, newSubReceiver(t, client, "orders", "report", nil), 1)); got[0] != "report" {
		t.Errorf("report received %q, want report", got)
	}
}

// TestBinaryExport exports the private-module fixture with -export, and starts
// the command again from the Config.json and from the main.tf it wrote.
func TestBinaryExport(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "out")
	if out, err := exec.Command(buildBinary(t), "-log-level", "error", "-terraform", "testdata/private-module/main.tf", "-export", dir).CombinedOutput(); err != nil {
		t.Fatalf("-export error: %v\n%s", err, out)
	}
	for _, args := range [][]string{
		{"-config", filepath.Join(dir, "Config.json")},
		{"-terraform", filepath.Join(dir, "main.tf")},
	} {
		client := runBinary(t, args...)
		send(t, newSender(t, client, "orders"),
			&azservicebus.Message{Body: []byte("process"), ApplicationProperties: map[string]any{"event": "process"}},
			&azservicebus.Message{Body: []byte("report"), ApplicationProperties: map[string]any{"event": "report"}},
		)
		if got := bodiesOf(receive(t, newSubReceiver(t, client, "orders", "report", nil), 1)); got[0] != "report" {
			t.Errorf("%v: report received %q, want report", args, got)
		}
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if binPath != "" {
		os.RemoveAll(filepath.Dir(binPath))
	}
	os.Exit(code)
}

// TestBinaryEnvironment starts the command with its entities and the log
// level taken from OSB_* variables, as a container is configured.
func TestBinaryEnvironment(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	t.Setenv("OSB_QUEUE", "env-orders,env-payments")
	t.Setenv("OSB_TOPIC", "env-events")
	t.Setenv("OSB_SUBSCRIPTION", "env-events/audit")
	client := runBinary(t)
	send(t, newSender(t, client, "env-payments"), bodies("from the environment")...)
	if got := bodiesOf(receive(t, newReceiver(t, client, "env-payments", nil), 1)); got[0] != "from the environment" {
		t.Errorf("received %q", got)
	}
	send(t, newSender(t, client, "env-events"), bodies("event")...)
	if got := bodiesOf(receive(t, newSubReceiver(t, client, "env-events", "audit", nil), 1)); got[0] != "event" {
		t.Errorf("subscription received %q", got)
	}
}

// otlpRecorder is a fake OTLP/HTTP receiver that keeps what is posted.
type otlpRecorder struct {
	url    string
	mu     sync.Mutex
	traces []byte
	all    []byte
	paths  map[string]int
}

func newOTLPRecorder(t *testing.T) *otlpRecorder {
	t.Helper()
	rec := &otlpRecorder{paths: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("Content-Encoding") == "gzip" {
			if zr, err := gzip.NewReader(bytes.NewReader(body)); err == nil {
				body, _ = io.ReadAll(zr)
			}
		}
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.paths[r.URL.Path]++
		rec.all = append(rec.all, body...)
		if r.URL.Path == "/v1/traces" {
			rec.traces = append(rec.traces, body...)
		}
	}))
	t.Cleanup(srv.Close)
	rec.url = srv.URL
	return rec
}

// waitFor waits until the traces posted mention every name.
func (r *otlpRecorder) waitFor(t *testing.T, names ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r.mu.Lock()
		missing := ""
		for _, n := range names {
			if !bytes.Contains(r.traces, []byte(n)) {
				missing = n
				break
			}
		}
		paths := fmt.Sprint(r.paths)
		r.mu.Unlock()
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no span %q arrived; posted to %v", missing, paths)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (r *otlpRecorder) contains(s string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return bytes.Contains(r.all, []byte(s))
}

// TestBinaryTelemetry starts the command with -otel-endpoint pointing at a
// fake OTLP receiver and checks that spans for the broker operations arrive,
// and that nothing of the message body does.
func TestBinaryTelemetry(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	rec := newOTLPRecorder(t)
	client := runBinary(t, "-queue", "telemetry-q", "-otel-endpoint", rec.url, "-otel-signals", "traces,logs", "-otel-service-name", "osb-under-test")
	const secret = "SECRET-BODY-4f9a1c"
	send(t, newSender(t, client, "telemetry-q"), bodies(secret)...)
	r := newReceiver(t, client, "telemetry-q", nil)
	for _, m := range receive(t, r, 1) {
		if err := r.CompleteMessage(testContext(t), m, nil); err != nil {
			t.Fatal(err)
		}
	}
	// A client that traces its sends puts the context in Diagnostic-Id; the
	// emulator's spans continue that trace.
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	send(t, newSender(t, client, "telemetry-q"), &azservicebus.Message{
		Body:                  []byte("traced"),
		ApplicationProperties: map[string]any{"Diagnostic-Id": "00-" + traceID + "-00f067aa0ba902b7-01"},
	})
	rec.waitFor(t, "broker.send", "broker.complete", "store.commit", "osb-under-test", "telemetry-q", traceID)
	if rec.contains(secret) {
		t.Error("the message body was exported")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.paths["/v1/metrics"] != 0 {
		t.Error("metrics were exported although -otel-signals leaves them out")
	}
}
