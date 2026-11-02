package portal

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

// timeRange is an interval of the "Show data for the last" selector, and the
// step its points are grouped by.
type timeRange struct {
	Key, Label string
	Span, Step time.Duration
}

var ranges = []timeRange{
	{"1h", "1 hour", time.Hour, time.Minute},
	{"6h", "6 hours", 6 * time.Hour, 5 * time.Minute},
	{"12h", "12 hours", 12 * time.Hour, 10 * time.Minute},
	{"1d", "1 day", 24 * time.Hour, 30 * time.Minute},
	{"7d", "7 days", 7 * 24 * time.Hour, 2 * time.Hour},
	{"30d", "30 days", 30 * 24 * time.Hour, 12 * time.Hour},
}

func findRange(key string) timeRange {
	for _, r := range ranges {
		if r.Key == key {
			return r
		}
	}
	return ranges[0]
}

func rangeOptions(current timeRange) []views.RangeOption {
	out := make([]views.RangeOption, len(ranges))
	for i, r := range ranges {
		out[i] = views.RangeOption{Key: r.Key, Label: r.Label, Active: r.Key == current.Key}
	}
	return out
}

// lineSeries is the data of one chart line.
type lineSeries struct {
	name   string
	points []metrics.Point
	gauge  bool
}

const (
	chartWidth, chartHeight = 800, 260
	marginLeft              = 44
	marginRight             = 12
	marginTop               = 12
	marginBottom            = 28
)

// buildChart draws series as SVG lines. Counters show their sum in the
// legend, gauges their latest value.
func buildChart(title string, series []lineSeries, from, to time.Time, loc *time.Location) views.Chart {
	c := views.Chart{
		Title: title, Width: chartWidth, Height: chartHeight,
		Left: marginLeft, Right: marginRight, Top: marginTop, Bottom: marginBottom,
	}
	var maxV int64
	for _, s := range series {
		for _, p := range s.points {
			maxV = max(maxV, p.Value)
		}
	}
	top := niceMax(maxV)
	plotW := float64(chartWidth - marginLeft - marginRight)
	plotH := float64(chartHeight - marginTop - marginBottom)
	span := to.Sub(from).Seconds()
	x := func(t time.Time) float64 {
		if span <= 0 {
			return marginLeft
		}
		return marginLeft + plotW*t.Sub(from).Seconds()/span
	}
	y := func(v int64) float64 { return marginTop + plotH*(1-float64(v)/float64(top)) }

	for i := 0; i <= 4; i++ {
		v := top * int64(i) / 4
		c.YTicks = append(c.YTicks, views.Tick{Pos: y(v), Label: shortNumber(v)})
	}
	layout := "15:04"
	if span > 36*3600 {
		layout = "Jan 2"
	}
	for i := 0; i <= 4; i++ {
		t := from.Add(time.Duration(float64(to.Sub(from)) * float64(i) / 4))
		c.XTicks = append(c.XTicks, views.Tick{Pos: x(t), Label: t.In(loc).Format(layout)})
	}
	for i, s := range series {
		var d strings.Builder
		var total int64
		for j, p := range s.points {
			if j == 0 {
				d.WriteString("M")
			} else {
				d.WriteString(" L")
			}
			fmt.Fprintf(&d, "%.1f,%.1f", x(p.Time), y(p.Value))
			if s.gauge {
				total = p.Value
			} else {
				total += p.Value
			}
		}
		label := " (Sum)"
		if s.gauge {
			label = " (Last)"
		}
		c.Series = append(c.Series, views.Series{
			Name: s.name + label, Class: "s" + strconv.Itoa(i%5), Total: shortNumber(total), Path: d.String(),
		})
	}
	return c
}

// niceMax rounds v up to a value that divides into four even ticks, at
// least 4.
func niceMax(v int64) int64 {
	if v < 4 {
		return 4
	}
	mag := math.Pow(10, math.Floor(math.Log10(float64(v))))
	for _, m := range []float64{1, 2, 2.5, 5, 10} {
		if n := int64(math.Ceil(m * mag)); n >= v && n%4 == 0 {
			return n
		}
	}
	return int64(math.Ceil(float64(v)/4)) * 4
}

// shortNumber abbreviates large counts: 1.2k, 3.4M.
func shortNumber(v int64) string {
	switch {
	case v >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(v)/1e6)
	case v >= 10_000:
		return fmt.Sprintf("%.1fk", float64(v)/1e3)
	}
	return strconv.FormatInt(v, 10)
}

// formatBytes writes a size in the unit the Azure portal would use.
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	f, exp := float64(n), 0
	for f >= unit && exp < 4 {
		f /= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", f, "KMGT"[exp-1])
}
