package duration

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	tests := []struct {
		in   string
		want time.Duration
	}{
		{"PT30S", 30 * time.Second},
		{"PT1M", time.Minute},
		{"PT1H30M", 90 * time.Minute},
		{"P14D", 14 * 24 * time.Hour},
		{"P1W", 7 * 24 * time.Hour},
		{"P1DT2H3M4.5S", 26*time.Hour + 3*time.Minute + 4500*time.Millisecond},
		{"PT0,25S", 250 * time.Millisecond},
		{"pt5s", 5 * time.Second},
		{"P10675199DT2H48M5.4775807S", Infinite},
		{"P99999999D", Infinite},
	}
	for _, tt := range tests {
		got, err := Parse(tt.in)
		if err != nil {
			t.Errorf("Parse(%q) error: %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Parse(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestParseDurationErrors(t *testing.T) {
	for _, in := range []string{"", "P", "PT", "1M", "P1M", "P1Y", "PT1D", "PT1S1M", "PT1M1M", "P1DT", "PTS", "PT1", "PT1.2.3S", "00:01:00"} {
		if got, err := Parse(in); err == nil {
			t.Errorf("Parse(%q) = %v, want error", in, got)
		}
	}
}

func TestFormat(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{0, "PT0S"},
		{30 * time.Second, "PT30S"},
		{time.Minute, "PT1M"},
		{90 * time.Minute, "PT1H30M"},
		{14 * 24 * time.Hour, "P14D"},
		{26*time.Hour + 3*time.Minute + 4500*time.Millisecond, "P1DT2H3M4.5S"},
		{250 * time.Millisecond, "PT0.25S"},
		{Infinite, "P10675199DT2H48M5.4775807S"},
	}
	for _, tt := range tests {
		if got := Format(tt.d); got != tt.want {
			t.Errorf("Format(%v) = %q, want %q", tt.d, got, tt.want)
		}
		if tt.d > 0 {
			if back, err := Parse(tt.want); err != nil || back != tt.d {
				t.Errorf("Parse(%q) = %v, %v; want %v", tt.want, back, err, tt.d)
			}
		}
	}
}
