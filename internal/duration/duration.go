// Package duration reads and writes the ISO 8601 durations Service Bus
// uses for entity properties, such as "PT1M" or "P14D".
package duration

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Infinite is TimeSpan.MaxValue, the duration Service Bus uses for "never".
const Infinite = time.Duration(math.MaxInt64)

// Parse parses an ISO 8601 duration of days, hours, minutes and
// seconds, such as "PT30S", "PT1H30M" or "P10675199DT2H48M5.4775807S".
// Weeks are accepted as 7 days. Years and months are rejected, since their
// length varies. Durations too long for time.Duration become
// Infinite, which is how Service Bus writes TimeSpan.MaxValue.
func Parse(s string) (time.Duration, error) {
	rest, ok := strings.CutPrefix(strings.ToUpper(s), "P")
	if !ok || rest == "" || rest == "T" {
		return 0, fmt.Errorf("invalid ISO 8601 duration %q", s)
	}
	// Units must appear at most once each, in this order.
	units := "WD"
	scale := map[byte]float64{
		'W': float64(7 * 24 * time.Hour),
		'D': float64(24 * time.Hour),
	}
	var total float64
	inTime := false
	for rest != "" {
		if rest[0] == 'T' {
			if inTime || len(rest) == 1 {
				return 0, fmt.Errorf("invalid ISO 8601 duration %q", s)
			}
			inTime = true
			units = "HMS"
			scale = map[byte]float64{
				'H': float64(time.Hour),
				'M': float64(time.Minute),
				'S': float64(time.Second),
			}
			rest = rest[1:]
			continue
		}
		i := 0
		for i < len(rest) && (rest[i] >= '0' && rest[i] <= '9' || rest[i] == '.' || rest[i] == ',') {
			i++
		}
		if i == 0 || i == len(rest) {
			return 0, fmt.Errorf("invalid ISO 8601 duration %q", s)
		}
		n, err := strconv.ParseFloat(strings.ReplaceAll(rest[:i], ",", "."), 64)
		if err != nil {
			return 0, fmt.Errorf("invalid ISO 8601 duration %q", s)
		}
		pos := strings.IndexByte(units, rest[i])
		if pos < 0 {
			return 0, fmt.Errorf("invalid ISO 8601 duration %q: unexpected unit %q", s, rest[i])
		}
		total += n * scale[rest[i]]
		units = units[pos+1:]
		rest = rest[i+1:]
	}
	if total >= float64(Infinite) {
		return Infinite, nil
	}
	return time.Duration(total), nil
}

// Format writes d as an ISO 8601 duration of days, hours, minutes and
// seconds. Infinite is written as TimeSpan.MaxValue,
// "P10675199DT2H48M5.4775807S", as Service Bus does.
func Format(d time.Duration) string {
	if d >= Infinite {
		return "P10675199DT2H48M5.4775807S"
	}
	if d <= 0 {
		return "PT0S"
	}
	var b strings.Builder
	b.WriteString("P")
	if days := d / (24 * time.Hour); days > 0 {
		fmt.Fprintf(&b, "%dD", days)
		d -= days * 24 * time.Hour
	}
	if d == 0 {
		return b.String()
	}
	b.WriteString("T")
	if h := d / time.Hour; h > 0 {
		fmt.Fprintf(&b, "%dH", h)
		d -= h * time.Hour
	}
	if m := d / time.Minute; m > 0 {
		fmt.Fprintf(&b, "%dM", m)
		d -= m * time.Minute
	}
	if d > 0 {
		b.WriteString(strconv.FormatFloat(d.Seconds(), 'f', -1, 64))
		b.WriteString("S")
	}
	return b.String()
}
