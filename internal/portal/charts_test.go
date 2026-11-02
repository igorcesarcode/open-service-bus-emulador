package portal

import (
	"strings"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
)

func TestNiceMax(t *testing.T) {
	for in, want := range map[int64]int64{0: 4, 3: 4, 4: 4, 5: 8, 16: 20, 17: 20, 99: 100, 101: 200, 1234: 2000} {
		if got := niceMax(in); got != want {
			t.Errorf("niceMax(%d) = %d, want %d", in, got, want)
		}
		if got := niceMax(in); got%4 != 0 || got < in {
			t.Errorf("niceMax(%d) = %d: not a multiple of 4 above the value", in, got)
		}
	}
}

func TestBuildChart(t *testing.T) {
	from := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	pts := []metrics.Point{{Time: from, Value: 0}, {Time: from.Add(30 * time.Minute), Value: 8}, {Time: to, Value: 4}}
	c := buildChart("T", []lineSeries{
		{name: "Sent", points: pts},
		{name: "Waiting", points: pts, gauge: true},
	}, from, to, time.UTC)
	if len(c.Series) != 2 || c.Series[0].Name != "Sent (Sum)" || c.Series[0].Total != "12" {
		t.Errorf("counter series = %+v", c.Series[0])
	}
	if c.Series[1].Name != "Waiting (Last)" || c.Series[1].Total != "4" {
		t.Errorf("gauge series = %+v", c.Series[1])
	}
	if !strings.HasPrefix(c.Series[0].Path, "M44.0,232.0 L") || c.Series[0].Class != "s0" || c.Series[1].Class != "s1" {
		t.Errorf("path = %q", c.Series[0].Path)
	}
	if len(c.YTicks) != 5 || c.YTicks[4].Label != "8" || c.XTicks[0].Label != "10:00" || c.XTicks[4].Label != "11:00" {
		t.Errorf("ticks = %v %v", c.YTicks, c.XTicks)
	}
}

func TestFormatBytes(t *testing.T) {
	for in, want := range map[int64]string{0: "0 B", 1023: "1023 B", 1024: "1.0 KB", 5 << 20: "5.0 MB", 3 << 30: "3.0 GB"} {
		if got := formatBytes(in); got != want {
			t.Errorf("formatBytes(%d) = %q, want %q", in, got, want)
		}
	}
}
