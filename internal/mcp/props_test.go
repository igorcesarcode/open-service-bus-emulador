package mcp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
)

func TestApplyProps(t *testing.T) {
	p := broker.DefaultProperties(broker.KindQueue)
	err := applyProps(&p, map[string]any{
		"lockduration":             "PT45M", // names are case-insensitive
		"maxDeliveryCount":         float64(500),
		"defaultMessageTimeToLive": float64(90),
		"requiresSession":          true,
		"forwardTo":                "other",
		"status":                   "SendDisabled",
	})
	if err != nil {
		t.Fatalf("applyProps() error: %v", err)
	}
	if p.LockDuration != 45*time.Minute || p.MaxDeliveryCount != 500 || p.DefaultMessageTimeToLive != 90*time.Second ||
		!p.RequiresSession || p.ForwardTo != "other" || p.Status != broker.StatusSendDisabled {
		t.Errorf("applyProps() = %+v", p)
	}
}

func TestApplyPropsErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   map[string]any
		want string
	}{
		{"unknown", map[string]any{"lockDurration": "PT1M"}, "unknown property"},
		{"duration", map[string]any{"lockDuration": "soon"}, "lockDuration"},
		{"duration type", map[string]any{"lockDuration": true}, "ISO 8601"},
		{"bool", map[string]any{"requiresSession": "yes"}, "true or false"},
		{"fraction", map[string]any{"maxDeliveryCount": 1.5}, "whole number"},
		{"range", map[string]any{"maxDeliveryCount": float64(1 << 40)}, "out of range"},
		{"text", map[string]any{"forwardTo": float64(3)}, "text"},
	} {
		p := broker.DefaultProperties(broker.KindQueue)
		err := applyProps(&p, tc.in)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: applyProps() error = %v, want it to hold %q", tc.name, err, tc.want)
		}
	}
}

func TestPropsViewHidesWhatATopicLacks(t *testing.T) {
	p := broker.DefaultProperties(broker.KindTopic)
	v := propsView(p, broker.KindTopic)
	if _, ok := v["lockDuration"]; ok {
		t.Errorf("topic view has lockDuration: %v", v)
	}
	if v["defaultMessageTimeToLive"] == nil {
		t.Errorf("topic view lacks the time to live: %v", v)
	}
}

func TestSchemaOf(t *testing.T) {
	type in struct {
		Common
		Name  string         `json:"name" desc:"A name." required:"true"`
		Kind  string         `json:"kind,omitempty" enum:"a,b"`
		Count int            `json:"count,omitempty"`
		Tags  []string       `json:"tags,omitempty"`
		Props map[string]any `json:"props,omitempty"`
		TTL   Duration       `json:"ttl,omitempty"`
		Skip  string         `json:"-"`
	}
	var zero in
	got, _ := json.Marshal(schemaOf(reflect.TypeOf(zero)))
	want := `{"additionalProperties":false,"properties":{` +
		`"count":{"type":"integer"},` +
		`"kind":{"enum":["a","b"],"type":"string"},` +
		`"name":{"description":"A name.","type":"string"},` +
		`"namespace":{"description":"Namespace name; the default namespace when left out.","type":"string"},` +
		`"props":{"type":"object"},` +
		`"tags":{"items":{"type":"string"},"type":"array"},` +
		`"ttl":{"type":["string","number"]}},` +
		`"required":["name"],"type":"object"}`
	if diff := cmp.Diff(want, string(got)); diff != "" {
		t.Errorf("schema mismatch (-want +got):\n%s", diff)
	}
}

func TestDurationAcceptsTextAndSeconds(t *testing.T) {
	var v struct {
		A, B Duration
	}
	if err := json.Unmarshal([]byte(`{"A":"PT5M","B":90}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.A != "PT5M" || v.B != "90" {
		t.Errorf("Duration = %q, %q, want PT5M and 90", v.A, v.B)
	}
	for _, d := range []struct {
		in   Duration
		want time.Duration
	}{{"PT5M", 5 * time.Minute}, {"90", 90 * time.Second}, {"0.5", 500 * time.Millisecond}, {"", time.Hour}} {
		got, err := durationArg(d.in, time.Hour)
		if err != nil || got != d.want {
			t.Errorf("durationArg(%q) = %v, %v, want %v", d.in, got, err, d.want)
		}
	}
	if err := json.Unmarshal([]byte(`{"A":[1]}`), &v); err == nil {
		t.Error("an array was accepted as a Duration")
	}
	if _, err := durationArg("soon", time.Hour); err == nil {
		t.Error("durationArg(soon) did not fail")
	}
}

func TestLoopbackOrigin(t *testing.T) {
	for _, tc := range []struct {
		origin string
		want   bool
	}{
		{"http://localhost:3000", true},
		{"http://127.0.0.1:1234", true},
		{"http://[::1]:80", true},
		{"http://dev.localhost", true},
		{"https://evil.example", false},
		{"https://localhost.evil.example", false},
		{"http://10.0.0.5:5680", false},
		{"null", false},
	} {
		if got := loopbackOrigin(tc.origin); got != tc.want {
			t.Errorf("loopbackOrigin(%q) = %v, want %v", tc.origin, got, tc.want)
		}
	}
}

// TestRebindingIsRefused sends what a page on a name that resolves to this
// machine sends: an Origin and a Host that agree, both the attacker's.
func TestRebindingIsRefused(t *testing.T) {
	s := &Server{opts: Options{}, tools: map[string]*tool{}}
	req := httptest.NewRequest(http.MethodPost, "http://evil.example:5680/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	req.Header.Set("Origin", "http://evil.example:5680")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", rec.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "http://127.0.0.1:5680/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("a program (no Origin) got status %d, want 200", rec.Code)
	}
}
