package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// Docker mode (OSB_E2E_DOCKER=1) runs the same scenarios with the emulator
// and the mini-services in containers, the way a service meets the real
// Service Bus: the namespace's servicebus.windows.net name, found through
// DNS on a Docker network, the real ports (5671 for AMQP and 443 for HTTPS
// and WebSocket), TLS with a certificate the services trust through a CA
// file, and no UseDevelopmentEmulator in their connection strings. The
// emulator image is the project's Dockerfile, a static binary on scratch.
// The test process reaches the emulator through published loopback ports,
// to check the final state.

const (
	dockerHost = "osb-test.servicebus.windows.net"
	dockerConn = "Endpoint=sb://" + dockerHost + "/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE"
	certInside = "/certs/ca.pem"
)

var (
	dockerMode  = os.Getenv("OSB_E2E_DOCKER") != ""
	imageSuffix = fmt.Sprint(os.Getpid())

	// dockerEmulators maps the connection string startEmulator returned to
	// its container.
	dockerEmulators sync.Map
	dockerSeq       sync.Mutex
	dockerCount     int
)

// runLabel marks every container of this test run, so a sweep at the end can
// remove the ones a timed out docker client left behind: the daemon may
// create a container after the client that asked for it was killed.
var runLabel = "osb-e2e-run=" + imageSuffix

func removeRunContainers() {
	out, _ := exec.Command("docker", "ps", "-aq", "--filter", "label="+runLabel).Output()
	if ids := strings.Fields(string(out)); len(ids) > 0 {
		exec.Command("docker", append([]string{"rm", "-f"}, ids...)...).Run()
	}
}

func imageName(kind string) string { return "osb-e2e-" + kind + ":" + imageSuffix }

// dockerEmulator is one emulator container and its network.
type dockerEmulator struct {
	network string
	name    string
	certDir string
}

// buildImages builds the emulator, Go service and Node service images from
// binaries already compiled for linux, and returns a function that removes
// them. The Node image carries test/node, with its dependencies.
func buildImages(dir string) (func(), error) {
	build := func(kind, dockerfile string, files map[string]string) error {
		ctx := filepath.Join(dir, "ctx-"+kind)
		if err := os.MkdirAll(ctx, 0o755); err != nil {
			return err
		}
		for dst, src := range files {
			if b, err := exec.Command("cp", "-r", src, filepath.Join(ctx, dst)).CombinedOutput(); err != nil {
				return fmt.Errorf("cp %s: %v\n%s", src, err, b)
			}
		}
		if err := os.WriteFile(filepath.Join(ctx, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
			return err
		}
		if b, err := exec.Command("docker", "build", "-q", "-t", imageName(kind), ctx).CombinedOutput(); err != nil {
			return fmt.Errorf("docker build %s: %v\n%s", kind, err, b)
		}
		return nil
	}
	cleanup := func() {
		removeRunContainers()
		for _, k := range []string{"emulator", "go", "node"} {
			exec.Command("docker", "rmi", "-f", imageName(k)).Run()
		}
	}
	dockerfile, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		return cleanup, err
	}
	if err := build("emulator", string(dockerfile), map[string]string{"open-service-bus": emulatorBin}); err != nil {
		return cleanup, err
	}
	if err := build("go", "FROM scratch\nCOPY goservice /goservice\nENTRYPOINT [\"/goservice\"]\n",
		map[string]string{"goservice": goServiceBin}); err != nil {
		return cleanup, err
	}
	if nodeMissing == "" {
		if err := build("node", "FROM node:22-alpine\nWORKDIR /app\nCOPY node /app\nENTRYPOINT [\"node\"]\n",
			map[string]string{"node": nodeDir}); err != nil {
			return cleanup, err
		}
	}
	return cleanup, nil
}

// docker runs the docker CLI and returns its output.
func docker(args ...string) (string, error) {
	var out, errb bytes.Buffer
	cmd := exec.Command("docker", args...)
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if err != nil {
		err = fmt.Errorf("docker %s: %v\n%s", strings.Join(args, " "), err, errb.String())
	}
	return strings.TrimSpace(out.String()), err
}

// containerLog returns what a container wrote to stdout and stderr.
func containerLog(name string) string {
	b, _ := exec.Command("docker", "logs", name).CombinedOutput()
	return string(b)
}

// startDockerEmulator is startEmulator in docker mode. It returns a
// connection string for the test process, on the published AMQP port.
func startDockerEmulator(t *testing.T, args ...string) string {
	t.Helper()
	dockerSeq.Lock()
	dockerCount++
	id := fmt.Sprintf("osb-e2e-%d-%d", os.Getpid(), dockerCount)
	dockerSeq.Unlock()

	certDir := t.TempDir()
	if err := os.Chmod(certDir, 0o777); err != nil {
		t.Fatal(err)
	}
	d := &dockerEmulator{network: id, name: id + "-bus", certDir: certDir}
	if _, err := docker("network", "create", d.network); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { docker("network", "rm", d.network) })

	// The emulator's own flags: the real ports, and the extra arguments of
	// the test, with -tls-cert-out redirected to the shared directory.
	emuArgs := []string{"-listen", "", "-amqp-addr", ":5671", "-http-addr", ":443",
		"-config", "testdata/Config.json", "-tls-hosts", dockerHost, "-tls-cert-out", certInside}
	var hostCert string
	for i := 0; i < len(args); i++ {
		if args[i] == "-tls-cert-out" && i+1 < len(args) {
			hostCert = args[i+1]
			i++
			continue
		}
		emuArgs = append(emuArgs, args[i])
	}
	testdata, err := filepath.Abs("testdata")
	if err != nil {
		t.Fatal(err)
	}
	run := append([]string{"run", "-d", "--label", runLabel, "--name", d.name, "--network", d.network,
		"--network-alias", dockerHost, "-p", "127.0.0.1::5671",
		"-v", testdata + ":/work/testdata:ro", "-v", certDir + ":/certs", "-w", "/work",
		imageName("emulator")}, emuArgs...)
	if _, err := docker(run...); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			logs := containerLog(d.name)
			t.Logf("emulator container log:\n%s", logs)
		}
		docker("rm", "-f", d.name)
	})

	deadline := time.Now().Add(20 * time.Second)
	for {
		logs := containerLog(d.name)
		if strings.Contains(logs, "connection_string=") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("emulator container did not start:\n%s", logs)
		}
		time.Sleep(100 * time.Millisecond)
	}
	port, err := docker("port", d.name, "5671/tcp")
	if err != nil {
		t.Fatal(err)
	}
	_, p, err := net.SplitHostPort(strings.Fields(port)[0])
	if err != nil {
		t.Fatalf("docker port %q: %v", port, err)
	}
	if hostCert != "" {
		if b, err := os.ReadFile(filepath.Join(certDir, "ca.pem")); err == nil {
			os.WriteFile(hostCert, b, 0o644)
		}
	}
	conn := "Endpoint=sb://127.0.0.1:" + p + ";SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true"
	dockerEmulators.Store(conn, d)
	return conn
}

// dockerCommand is the command that runs a mini-service in a container on
// the emulator's network, or nil when connString is not a docker emulator's.
func dockerCommand(ctx context.Context, t *testing.T, connString string, env []string, lang, command string, args []string) *exec.Cmd {
	v, ok := dockerEmulators.Load(connString)
	if !ok {
		return nil
	}
	d := v.(*dockerEmulator)
	name := fmt.Sprintf("%s-%s-%d", d.network, lang, time.Now().UnixNano()%1e9)
	run := []string{"run", "--rm", "--label", runLabel, "--name", name, "--network", d.network,
		"-v", d.certDir + ":/certs:ro",
		"-e", "OSB_CONNECTION_STRING=" + dockerConn,
		"-e", "SSL_CERT_FILE=" + certInside, "-e", "NODE_EXTRA_CA_CERTS=" + certInside}
	for _, e := range env {
		// Certificate paths of the host mean nothing inside.
		if strings.HasPrefix(e, "SSL_CERT_FILE=") || strings.HasPrefix(e, "NODE_EXTRA_CA_CERTS=") {
			continue
		}
		run = append(run, "-e", e)
	}
	args = withIdleGrace(args)
	switch lang {
	case "go":
		run = append(run, imageName("go"), command)
		run = append(run, args...)
	case "js":
		if nodeMissing != "" {
			t.Skip(nodeMissing)
		}
		run = append(run, imageName("node"), "e2e/service.mjs", command)
		run = append(run, jsArgs(args)...)
	}
	t.Cleanup(func() { docker("rm", "-f", name) })
	return exec.CommandContext(ctx, "docker", run...)
}

// dockerIdleGrace is added to the -idle of a service in a container. The
// idle time counts from the start of the service, and a container starts
// slower than a process, Node's above all, so a responder with a short idle
// time would give up before its requester sent anything.
const dockerIdleGrace = 10 * time.Second

func withIdleGrace(args []string) []string {
	out := slices.Clone(args)
	for i := 0; i+1 < len(out); i++ {
		if out[i] != "-idle" {
			continue
		}
		if d, err := time.ParseDuration(out[i+1]); err == nil {
			out[i+1] = (d + dockerIdleGrace).String()
		}
	}
	return out
}

// dockerSlowdown stretches the time limits of the scenarios in docker mode,
// where every service is a container and starting one takes seconds.
const dockerSlowdown = 4

// timeoutContext is context.WithTimeout on a fresh background context, with
// d stretched in docker mode.
func timeoutContext(d time.Duration) (context.Context, context.CancelFunc) {
	if dockerMode {
		d *= dockerSlowdown
	}
	return context.WithTimeout(context.Background(), d)
}
