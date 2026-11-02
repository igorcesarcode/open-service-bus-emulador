// Package e2e_test runs small Go and JavaScript services against the
// emulator binary, as separate processes, the way services under test use
// it.
package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/igorcesarcode/open-service-bus-emulador/test/e2e/spec"
)

// Binaries built once by TestMain.
var (
	emulatorBin  string
	goServiceBin string
	// nodeDir is test/node, or empty when Node.js or its dependencies are
	// missing.
	nodeDir     string
	nodeMissing string
)

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	flag.Parse()
	if testing.Short() {
		fmt.Println("skipping end-to-end tests in short mode")
		return 0
	}
	dir, err := os.MkdirTemp("", "osb-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dir)
	exe := ""
	if runtime.GOOS == "windows" {
		exe = ".exe" // Windows will not run a file without it
	}
	emulatorBin = filepath.Join(dir, "open-service-bus"+exe)
	goServiceBin = filepath.Join(dir, "goservice"+exe)
	for pkg, out := range map[string]string{"../../cmd/open-service-bus": emulatorBin, "./goservice": goServiceBin} {
		build := exec.Command("go", "build", "-o", out, pkg)
		build.Env = append(os.Environ(), "CGO_ENABLED=0")
		if b, err := build.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "go build %s: %v\n%s", pkg, err, b)
			return 1
		}
	}

	nodeMissing = findNode()
	if dockerMode {
		cleanup, err := buildImages(dir)
		defer cleanup()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	return m.Run()
}

func findNode() string {
	if _, err := exec.LookPath("node"); err != nil {
		return "node not found in PATH"
	}
	dir, err := filepath.Abs("../node")
	if err != nil {
		return err.Error()
	}
	if _, err := os.Stat(filepath.Join(dir, "node_modules", "@azure", "service-bus")); err != nil {
		return "test/node dependencies are not installed; run npm ci in test/node"
	}
	nodeDir = dir
	return ""
}

var connStringRE = regexp.MustCompile(`\bconnection_string=("(?:[^"\\]|\\.)*"|\S+)`)

// startEmulator runs the emulator binary with testdata/Config.json on a
// free port and returns its connection string. Its log goes to the test
// log.
func startEmulator(t *testing.T, args ...string) string {
	t.Helper()
	if dockerMode {
		return startDockerEmulator(t, args...)
	}
	cmd := exec.Command(emulatorBin, append([]string{"-listen", "127.0.0.1:0", "-config", "testdata/Config.json"}, args...)...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting emulator: %v", err)
	}

	found := make(chan string, 1)
	logged := make(chan struct{})
	go func() {
		defer close(logged)
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			line := sc.Text()
			t.Logf("emulator: %s", line)
			if m := connStringRE.FindStringSubmatch(line); m != nil {
				s := m[1]
				if u, err := strconv.Unquote(s); err == nil {
					s = u
				}
				select {
				case found <- s:
				default:
				}
			}
		}
	}()
	t.Cleanup(func() {
		if runtime.GOOS == "windows" {
			// Windows cannot deliver SIGTERM to another process.
			cmd.Process.Kill()
			cmd.Wait()
			<-logged
			return
		}
		cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("emulator exited with %v", err)
			}
		case <-time.After(10 * time.Second):
			cmd.Process.Kill()
			<-done
			t.Error("emulator did not stop within 10s of SIGTERM")
		}
		<-logged
	})

	select {
	case s := <-found:
		return s
	case <-logged:
		t.Fatal("emulator exited before listening")
	case <-time.After(10 * time.Second):
		t.Fatal("emulator did not report its connection string within 10s")
	}
	return ""
}

// service is a running mini-service process.
type service struct {
	name   string
	cmd    *exec.Cmd
	stdout bytes.Buffer
	stderr bytes.Buffer
	done   chan struct{}
	err    error
}

// startService runs command of the mini-service written in lang ("go" or
// "js") until it exits or ctx is done.
func startService(ctx context.Context, t *testing.T, connString, lang, command string, args ...string) *service {
	t.Helper()
	return startServiceEnv(ctx, t, connString, nil, lang, command, args...)
}

// startServiceEnv is startService with extra environment variables, such
// as OSB_TRANSPORT.
func startServiceEnv(ctx context.Context, t *testing.T, connString string, env []string, lang, command string, args ...string) *service {
	t.Helper()
	var cmd *exec.Cmd
	inDocker := false
	if c := dockerCommand(ctx, t, connString, env, lang, command, args); c != nil {
		cmd, inDocker = c, true
	}
	switch {
	case inDocker:
	case lang == "go":
		cmd = exec.CommandContext(ctx, goServiceBin, append([]string{command}, args...)...)
	case lang == "js":
		if nodeMissing != "" {
			t.Skip(nodeMissing)
		}
		cmd = exec.CommandContext(ctx, "node", append([]string{"e2e/service.mjs", command}, jsArgs(args)...)...)
		cmd.Dir = nodeDir
	default:
		t.Fatalf("unknown language %q", lang)
	}
	if inDocker {
		cmd.Env = os.Environ()
	} else {
		cmd.Env = append(append(os.Environ(), "OSB_CONNECTION_STRING="+connString), env...)
	}
	s := &service{name: fmt.Sprintf("%s %s %v", lang, command, args), cmd: cmd, done: make(chan struct{})}
	cmd.Stdout = &s.stdout
	cmd.Stderr = &s.stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", s.name, err)
	}
	go func() {
		s.err = cmd.Wait()
		close(s.done)
	}()
	t.Cleanup(func() {
		cmd.Process.Kill()
		<-s.done
	})
	return s
}

// jsArgs converts Go style flags (-name v) to Node style ones (--name v),
// and durations to milliseconds.
func jsArgs(args []string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		if len(a) > 1 && a[0] == '-' && a[1] != '-' {
			a = "-" + a
		} else if d, err := time.ParseDuration(a); err == nil {
			a = strconv.FormatInt(d.Milliseconds(), 10)
		}
		out[i] = a
	}
	return out
}

// wait waits for the service to exit successfully and returns the events
// it reported.
func (s *service) wait(t *testing.T) []spec.Event {
	t.Helper()
	<-s.done
	if s.stderr.Len() > 0 {
		t.Logf("%s stderr:\n%s", s.name, s.stderr.String())
	}
	if s.err != nil {
		t.Fatalf("%s failed: %v\nstdout:\n%s", s.name, s.err, s.stdout.String())
	}
	var events []spec.Event
	dec := json.NewDecoder(&s.stdout)
	for {
		var e spec.Event
		if err := dec.Decode(&e); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("%s wrote invalid event: %v", s.name, err)
		}
		events = append(events, e)
	}
	return events
}

func runService(ctx context.Context, t *testing.T, connString, lang, command string, args ...string) []spec.Event {
	t.Helper()
	return startService(ctx, t, connString, lang, command, args...).wait(t)
}

func waitAll(t *testing.T, services ...*service) []spec.Event {
	t.Helper()
	var events []spec.Event
	for _, s := range services {
		events = append(events, s.wait(t)...)
	}
	return events
}

// newClient connects the test itself, which checks the final state.
func newClient(t *testing.T, connString string) *azservicebus.Client {
	t.Helper()
	client, err := azservicebus.NewClientFromConnectionString(connString, nil)
	if err != nil {
		t.Fatalf("NewClientFromConnectionString() error: %v", err)
	}
	t.Cleanup(func() { client.Close(context.Background()) })
	return client
}

// drain receives and deletes messages until want arrive, then checks no
// more are left.
func drain(ctx context.Context, t *testing.T, r *azservicebus.Receiver, want int) []*azservicebus.ReceivedMessage {
	t.Helper()
	var out []*azservicebus.ReceivedMessage
	for len(out) < want {
		msgs, err := r.ReceiveMessages(ctx, want-len(out), nil)
		if err != nil {
			t.Fatalf("received %d of %d messages: %v", len(out), want, err)
		}
		out = append(out, msgs...)
	}
	extra, err := r.PeekMessages(ctx, 10, nil)
	if err != nil {
		t.Fatalf("PeekMessages() error: %v", err)
	}
	for _, m := range extra {
		t.Errorf("unexpected message %s (DeliveryCount %d, DeadLetterReason %q) left after the expected %d",
			m.MessageID, m.DeliveryCount, deref(m.DeadLetterReason), want)
	}
	return out
}
