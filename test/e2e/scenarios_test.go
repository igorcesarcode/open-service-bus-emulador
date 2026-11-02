package e2e_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/test/e2e/spec"
)

var langs = []string{"go", "js"}

const paymentCount = 8

// TestDuplicatePayments sends every payment four times: one producer sends
// them one by one and again in a batch, as a service retrying after
// timeouts would, and then a second producer, maybe in the other
// language, does the same with the same MessageIds. The payments queue
// detects duplicates, so the consumer gets each payment once.
func TestDuplicatePayments(t *testing.T) {
	for _, first := range langs {
		for _, second := range langs {
			t.Run(fmt.Sprintf("%s_then_%s", first, second), func(t *testing.T) {
				t.Parallel()
				ctx, cancel := timeoutContext(time.Minute)
				defer cancel()
				conn := startEmulator(t)

				for _, lang := range []string{first, second} {
					if sent := runService(ctx, t, conn, lang, "send-payments", "-count", fmt.Sprint(paymentCount)); len(sent) != 2*paymentCount {
						t.Fatalf("%s sent %d payments, want %d", lang, len(sent), 2*paymentCount)
					}
				}
				var got, want []string
				for _, e := range runService(ctx, t, conn, first, "collect-payments", "-idle", "1s") {
					got = append(got, e.ID)
				}
				for i := range paymentCount {
					want = append(want, spec.PaymentID(i))
				}
				slices.Sort(got)
				slices.Sort(want)
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("received payments mismatch (-want +got):\n%s", diff)
				}
			})
		}
	}
}

const alertCount = 9

// TestTerraformAlerts runs the emulator with -terraform on
// testdata/terraform, whose subscriptions are created with for_each over a
// variable given with -tf-var. As in Azure, the subscriptions keep $Default
// next to their rules and receive every alert, unless the emulator runs
// with -tf-drop-default-rule.
func TestTerraformAlerts(t *testing.T) {
	channels, err := json.Marshal(spec.AlertChannels)
	if err != nil {
		t.Fatal(err)
	}
	for _, drop := range []bool{false, true} {
		for _, producer := range langs {
			for _, consumer := range langs {
				t.Run(fmt.Sprintf("drop=%t/%s_to_%s", drop, producer, consumer), func(t *testing.T) {
					t.Parallel()
					ctx, cancel := timeoutContext(time.Minute)
					defer cancel()
					args := []string{"-terraform", "testdata/terraform", "-tf-var", "channels=" + string(channels)}
					if drop {
						args = append(args, "-tf-drop-default-rule")
					}
					conn := startEmulator(t, args...)

					runService(ctx, t, conn, producer, "publish-alerts", "-count", fmt.Sprint(alertCount))
					got := map[string][]string{}
					for _, e := range runService(ctx, t, conn, consumer, "consume-alerts", "-idle", "1s") {
						got[e.Entity] = append(got[e.Entity], e.ID)
					}
					want := map[string][]string{}
					for i := range alertCount {
						id, channel := spec.Alert(i)
						for _, sub := range spec.AlertRoutes(channel, drop) {
							want[sub] = append(want[sub], id)
						}
					}
					for _, ids := range got {
						slices.Sort(ids)
					}
					for _, ids := range want {
						slices.Sort(ids)
					}
					if diff := cmp.Diff(want, got); diff != "" {
						t.Errorf("routed alerts mismatch (-want +got):\n%s", diff)
					}
				})
			}
		}
	}
}

// TestRequestReplyOverWebSocket runs the request/reply scenario with both
// services connected over AMQP over WebSocket. They trust the emulator
// through the certificate written by -tls-cert-out: Go through
// SSL_CERT_FILE, Node through NODE_EXTRA_CA_CERTS.
func TestRequestReplyOverWebSocket(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SSL_CERT_FILE is only read on Linux")
	}
	for _, requester := range langs {
		for _, responder := range langs {
			t.Run(fmt.Sprintf("%s_to_%s", requester, responder), func(t *testing.T) {
				t.Parallel()
				ctx, cancel := timeoutContext(time.Minute)
				defer cancel()
				pemFile := filepath.Join(t.TempDir(), "osb.pem")
				conn := startEmulator(t, "-tls-cert-out", pemFile)
				env := []string{"OSB_TRANSPORT=websocket", "SSL_CERT_FILE=" + pemFile, "NODE_EXTRA_CA_CERTS=" + pemFile}

				resp := startServiceEnv(ctx, t, conn, env, responder, "respond", "-idle", "3s")
				replies := startServiceEnv(ctx, t, conn, env, requester, "request", "-count", fmt.Sprint(requestCount)).wait(t)
				if got := len(resp.wait(t)); got != requestCount {
					t.Errorf("responder answered %d requests, want %d", got, requestCount)
				}
				checkReplies(t, replies, requester, responder)
			})
		}
	}
}
