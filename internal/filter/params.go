package filter

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
)

// Values are typed values by name: the parameters of SQL filters and
// actions, and the properties of correlation filters. They keep their
// types through JSON.
type Values map[string]any

type jsonValue struct {
	Name  string          `json:"name"`
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

// MarshalJSON encodes the values with their types.
func (v Values) MarshalJSON() ([]byte, error) {
	out := make([]jsonValue, 0, len(v))
	for name, raw := range v {
		var (
			typ string
			val any
		)
		switch x := normalize(raw).(type) {
		case nil:
			typ = "null"
		case string:
			typ, val = "string", x
		case int64:
			typ, val = "int", x
		case float64:
			typ, val = "double", x
		case bool:
			typ, val = "bool", x
		case time.Duration:
			typ, val = "duration", duration.Format(x)
		case time.Time:
			typ, val = "time", x.Format(time.RFC3339Nano)
		case GUID:
			typ, val = "guid", x.String()
		default:
			return nil, fmt.Errorf("filter: value %s has unsupported type %T", name, raw)
		}
		b, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}
		out = append(out, jsonValue{Name: name, Type: typ, Value: b})
	}
	return json.Marshal(out)
}

// UnmarshalJSON decodes values encoded by MarshalJSON. A plain JSON object
// is accepted too, with numbers as float64.
func (v *Values) UnmarshalJSON(b []byte) error {
	if strings.HasPrefix(strings.TrimSpace(string(b)), "{") {
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			return err
		}
		*v = m
		return nil
	}
	var in []jsonValue
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	out := make(Values, len(in))
	for _, e := range in {
		var err error
		switch e.Type {
		case "null":
			out[e.Name] = nil
		case "string":
			var s string
			err = json.Unmarshal(e.Value, &s)
			out[e.Name] = s
		case "int":
			var n int64
			err = json.Unmarshal(e.Value, &n)
			out[e.Name] = n
		case "double":
			var f float64
			err = json.Unmarshal(e.Value, &f)
			out[e.Name] = f
		case "bool":
			var x bool
			err = json.Unmarshal(e.Value, &x)
			out[e.Name] = x
		case "time":
			var s string
			if err = json.Unmarshal(e.Value, &s); err == nil {
				var t time.Time
				t, err = time.Parse(time.RFC3339Nano, s)
				out[e.Name] = t
			}
		case "duration":
			var s string
			if err = json.Unmarshal(e.Value, &s); err == nil {
				var d time.Duration
				d, err = duration.Parse(s)
				out[e.Name] = d
			}
		case "guid":
			var s string
			if err = json.Unmarshal(e.Value, &s); err == nil {
				g, ok := parseGUID(s)
				if !ok {
					err = fmt.Errorf("invalid GUID %q", s)
				}
				out[e.Name] = g
			}
		default:
			err = fmt.Errorf("unknown type %q", e.Type)
		}
		if err != nil {
			return fmt.Errorf("filter: value %s: %w", e.Name, err)
		}
	}
	*v = out
	return nil
}

type paramNode struct{ name string }

func (n *paramNode) eval(m Message) (any, error) {
	if p, ok := m.(interface{ param(string) (any, bool) }); ok {
		if v, ok := p.param(n.name); ok {
			return normalize(v), nil
		}
	}
	return nil, evalErrorf("parameter %s is not defined", n.name)
}

// lookupParam finds a parameter by name, case-insensitively, with or
// without its @.
func lookupParam(params Values, name string) (any, bool) {
	if v, ok := params[name]; ok {
		return v, true
	}
	for k, v := range params {
		if strings.EqualFold(strings.TrimPrefix(k, "@"), strings.TrimPrefix(name, "@")) {
			return v, true
		}
	}
	return nil, false
}

type paramMessage struct {
	Message
	params Values
}

func (m *paramMessage) param(name string) (any, bool) { return lookupParam(m.params, name) }

type paramMutableMessage struct {
	MutableMessage
	params Values
}

func (m *paramMutableMessage) param(name string) (any, bool) { return lookupParam(m.params, name) }
