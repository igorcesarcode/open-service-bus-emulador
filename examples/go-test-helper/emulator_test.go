// Package gotesthelper shows an integration test that starts the emulator as
// a child process on a free port, uses it, and stops it.
//
// The test needs the binary: put open-service-bus on the PATH, or point
// OSB_BIN at it. Without it the test is skipped.
//
//	OSB_BIN=/path/to/open-service-bus go test ./examples/go-test-helper
package gotesthelper

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// startEmulator runs the emulator with the given extra flags and returns the
// connection string to it. The process is stopped when the test ends.
func startEmulator(t *testing.T, flags ...string) string {
	t.Helper()
	bin := os.Getenv("OSB_BIN")
	if bin == "" {
		bin = "open-service-bus"
	}
	path, err := exec.LookPath(bin)
	if err != nil {
		t.Skipf("emulator binary not found (set OSB_BIN): %v", err)
	}

	// Ask the system for a free port, then hand it to the emulator.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cmd := exec.Command(path, append([]string{"-listen", addr, "-log-level", "error"}, flags...)...)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})

	// Wait until the port accepts connections.
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("emulator did not start on %s", addr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Sprintf("Endpoint=sb://%s;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true", addr)
}

func TestSendAndReceive(t *testing.T) {
	cs := startEmulator(t, "-queue", "orders")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := azservicebus.NewClientFromConnectionString(cs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)

	sender, err := client.NewSender("orders", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close(ctx)
	if err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("hello")}, nil); err != nil {
		t.Fatal(err)
	}

	receiver, err := client.NewReceiverForQueue("orders", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close(ctx)
	msgs, err := receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(msgs[0].Body); got != "hello" {
		t.Errorf("body = %q, want %q", got, "hello")
	}
	if err := receiver.CompleteMessage(ctx, msgs[0], nil); err != nil {
		t.Fatal(err)
	}
}
