// Package filter implements Service Bus subscription rules: SQL filters,
// SQL rule actions and correlation filters.
//
// Expressions are evaluated with three-valued logic, as Service Bus does: a
// missing user property, a null value or an operation on mismatched types
// is unknown, and a filter matches only when it evaluates to true.
package filter

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Scope selects system or user (application) properties.
type Scope int

const (
	User Scope = iota
	System
)

// GUID is a UUID value, such as the result of newid().
type GUID [16]byte

func (g GUID) String() string {
	var b [36]byte
	hex.Encode(b[0:8], g[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], g[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], g[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], g[8:10])
	b[23] = '-'
	hex.Encode(b[24:], g[10:])
	return string(b[:])
}

func parseGUID(s string) (GUID, bool) {
	var g GUID
	s = strings.Trim(s, "{}")
	raw := strings.ReplaceAll(s, "-", "")
	if len(raw) != 32 {
		return g, false
	}
	if _, err := hex.Decode(g[:], []byte(raw)); err != nil {
		return g, false
	}
	return g, true
}

// Message gives an expression access to message properties.
type Message interface {
	// Property returns a property value and whether it is set. For the
	// system scope, name is a canonical name from SystemProperty. Values
	// may be any Go integer or float type, string, bool, time.Time or
	// GUID; other types are opaque and compare as unknown.
	Property(scope Scope, name string) (any, bool)
}

// MutableMessage is a message that rule actions can change.
type MutableMessage interface {
	Message
	// SetProperty sets a property. Values are nil, bool, int64, float64,
	// string, time.Time or GUID. For the system scope, name is canonical.
	SetProperty(scope Scope, name string, v any) error
	// RemoveProperty removes a user property.
	RemoveProperty(name string)
}

// EvalError reports an expression that cannot be evaluated, which Service
// Bus raises as a FilterException.
type EvalError struct {
	msg string
}

func (e *EvalError) Error() string { return "filter: " + e.msg }

func evalErrorf(format string, args ...any) error {
	return &EvalError{msg: fmt.Sprintf(format, args...)}
}

// IsEvalError reports whether err is an evaluation error.
func IsEvalError(err error) bool {
	var e *EvalError
	return errors.As(err, &e)
}

// unknownValue is the result of an expression over a missing property.
type unknownValue struct{}

var unknown any = unknownValue{}

// opaque wraps a property of a type expressions cannot operate on.
type opaque struct{ v any }

// normalize converts a property value to the types expressions use: nil,
// bool, int64, float64, string, time.Time, GUID or opaque.
func normalize(v any) any {
	switch v := v.(type) {
	case nil, bool, int64, float64, string, GUID, time.Duration:
		return v
	case time.Time:
		return v.UTC()
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case uint:
		return uintValue(uint64(v))
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return uintValue(v)
	case float32:
		return float64(v)
	case [16]byte:
		return GUID(v)
	}
	return opaque{v}
}

func uintValue(v uint64) any {
	if v > math.MaxInt64 {
		return float64(v)
	}
	return int64(v)
}

// formatValue formats a value the way .NET's ToString does for string
// concatenation.
func formatValue(v any) string {
	switch v := v.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		if v {
			return "True"
		}
		return "False"
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'G', -1, 64)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case time.Duration:
		return formatTimeSpan(v)
	case GUID:
		return v.String()
	}
	return fmt.Sprint(v)
}

// systemProperties maps lower-case names to canonical system property
// names. Label and Subject are the same property.
var systemProperties = map[string]string{}

// settable lists the system properties rule actions can set.
var settable = map[string]bool{
	"MessageId":               true,
	"CorrelationId":           true,
	"To":                      true,
	"ReplyTo":                 true,
	"Label":                   true,
	"SessionId":               true,
	"ReplyToSessionId":        true,
	"ContentType":             true,
	"ScheduledEnqueueTimeUtc": true,
}

func init() {
	for _, name := range []string{
		"MessageId", "CorrelationId", "To", "ReplyTo", "Label", "SessionId",
		"ReplyToSessionId", "ContentType", "TimeToLive", "ScheduledEnqueueTimeUtc",
		"SequenceNumber", "EnqueuedSequenceNumber", "EnqueuedTimeUtc", "ExpiresAtUtc",
		"DeliveryCount", "Size", "LockedUntilUtc", "LockToken", "DeadLetterSource",
		"PartitionKey", "ViaPartitionKey", "State",
	} {
		systemProperties[strings.ToLower(name)] = name
	}
	systemProperties["subject"] = "Label"
}

// SystemProperty returns the canonical name of a system property, matched
// case-insensitively.
func SystemProperty(name string) (string, bool) {
	c, ok := systemProperties[strings.ToLower(name)]
	return c, ok
}

// formatTimeSpan formats d like .NET's TimeSpan.ToString(): [-][d.]hh:mm:ss[.fffffff].
func formatTimeSpan(d time.Duration) string {
	sign := ""
	if d < 0 {
		sign, d = "-", -d
	}
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	h, m, s := d/time.Hour, d/time.Minute%60, d/time.Second%60
	out := sign
	if days > 0 {
		out += fmt.Sprintf("%d.", days)
	}
	out += fmt.Sprintf("%02d:%02d:%02d", h, m, s)
	if frac := d % time.Second; frac > 0 {
		out += "." + strings.TrimRight(fmt.Sprintf("%07d", frac/100), "0")
	}
	return out
}
