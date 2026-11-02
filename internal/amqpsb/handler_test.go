package amqpsb

import (
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
)

func TestAudienceEntity(t *testing.T) {
	tests := []struct {
		audience string
		want     string
	}{
		{"amqp://127.0.0.1:5672/orders", "orders"},
		{"amqps://ns.servicebus.windows.net/orders/$management", "orders"},
		{"sb://ns.servicebus.windows.net/", ""},
		{"sb://ns.servicebus.windows.net", ""},
		{"amqp://host/topic/Subscriptions/sub", "topic/Subscriptions/sub"},
		{"amqp://host/orders?query=1", "orders"},
		{"orders", "orders"},
	}
	for _, tc := range tests {
		if got := audienceEntity(tc.audience); got != tc.want {
			t.Errorf("audienceEntity(%q) = %q, want %q", tc.audience, got, tc.want)
		}
	}
}

func TestAcceptTimeout(t *testing.T) {
	tests := []struct {
		props encoding.Map
		want  time.Duration
	}{
		{nil, defaultAcceptTimeout},
		{encoding.Map{{Key: encoding.Symbol("com.microsoft:timeout"), Value: uint32(1000)}}, 900 * time.Millisecond},
		{encoding.Map{{Key: encoding.Symbol("com.microsoft:timeout"), Value: uint32(60000)}}, 59 * time.Second},
		{encoding.Map{{Key: encoding.Symbol("com.microsoft:timeout"), Value: "bad"}}, defaultAcceptTimeout},
	}
	for _, tc := range tests {
		if got := acceptTimeout(&frames.Attach{Properties: tc.props}); got != tc.want {
			t.Errorf("acceptTimeout(%v) = %v, want %v", tc.props, got, tc.want)
		}
	}
}
