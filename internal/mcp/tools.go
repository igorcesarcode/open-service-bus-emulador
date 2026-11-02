package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"reflect"
	"strconv"
	"strings"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// access classifies a tool, which decides whether it is registered.
type access struct {
	// write changes the state of the emulator.
	write bool
	// destroy deletes data that cannot be brought back; it asks for confirm.
	destroy bool
	// data reads or writes messages, which -mcp-allow-data enables.
	data bool
}

var (
	read        = access{}
	write       = access{write: true}
	destroy     = access{write: true, destroy: true}
	readData    = access{data: true}
	writeData   = access{write: true, data: true}
	destroyData = access{write: true, destroy: true, data: true}
)

type tool struct {
	name, title, desc string
	acc               access
	schema            map[string]any
	run               func(ctx context.Context, raw json.RawMessage) (any, error)
}

// Common is embedded in the input of every tool that works on a namespace.
type Common struct {
	Namespace string `json:"namespace,omitempty" desc:"Namespace name; the default namespace when left out."`
}

// Duration is an ISO 8601 duration or a number of seconds, as text.
type Duration string

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		*d = Duration(s)
		return nil
	}
	var f float64
	if err := json.Unmarshal(b, &f); err != nil {
		return errors.New("want an ISO 8601 duration such as PT30S, or a number of seconds")
	}
	*d = Duration(strconv.FormatFloat(f, 'g', -1, 64))
	return nil
}

// Confirm is embedded in the input of the tools that destroy data.
type Confirm struct {
	Confirm bool `json:"confirm" desc:"Must be true: the operation deletes data for good." required:"true"`
}

func (c Confirm) check() error {
	if !c.Confirm {
		return errors.New("this operation deletes data for good; call it again with confirm=true")
	}
	return nil
}

// add registers a tool whose input is a T. The input schema is built from
// the fields of T: the json tag names a field, desc describes it, enum lists
// its values and required="true" requires it.
func add[T any](s *Server, name, desc string, acc access, h func(ctx context.Context, in T) (any, error)) {
	if s.opts.ReadOnly && acc.write || acc.data && !s.opts.AllowData {
		return
	}
	var zero T
	t := &tool{name: name, desc: desc, acc: acc, schema: schemaOf(reflect.TypeOf(zero))}
	t.run = func(ctx context.Context, raw json.RawMessage) (any, error) {
		var in T
		if len(bytes.TrimSpace(raw)) > 0 && string(bytes.TrimSpace(raw)) != "null" {
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&in); err != nil {
				return nil, fmt.Errorf("invalid arguments: %w", err)
			}
		}
		return h(ctx, in)
	}
	s.tools[name] = t
	s.order = append(s.order, name)
}

func (s *Server) listTools() any {
	var out []map[string]any
	for _, name := range s.order {
		t := s.tools[name]
		out = append(out, map[string]any{
			"name": t.name, "description": t.desc, "inputSchema": t.schema,
			"annotations": map[string]any{
				"readOnlyHint": !t.acc.write, "destructiveHint": t.acc.destroy,
				"idempotentHint": !t.acc.write, "openWorldHint": false,
			},
		})
	}
	return map[string]any{"tools": out}
}

func (s *Server) callTool(ctx context.Context, params json.RawMessage) (any, error) {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, invalidParams("invalid params")
	}
	t, ok := s.tools[p.Name]
	if !ok {
		return nil, invalidParams("unknown tool %q", p.Name)
	}
	ctx, done := telemetry.Operation(ctx, "mcp.tool "+p.Name, attribute.String("mcp.tool", p.Name))
	res, err := t.run(ctx, p.Arguments)
	done(&err)
	telemetry.Add(ctx, telemetry.MCPToolCalls, 1, attribute.String("mcp.tool", p.Name), attribute.Bool("error", err != nil))
	if err != nil {
		return toolError(err), nil
	}
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return toolError(err), nil
	}
	out := map[string]any{"content": []map[string]any{{"type": "text", "text": string(b)}}}
	if len(b) > 0 && b[0] == '{' {
		out["structuredContent"] = json.RawMessage(b)
	}
	return out, nil
}

// toolError reports a failed call to the model, which can read it and try
// again; it is not a protocol error.
func toolError(err error) any {
	msg := strings.TrimPrefix(err.Error(), "broker: ")
	return map[string]any{"isError": true, "content": []map[string]any{{"type": "text", "text": msg}}}
}

// namespace returns the namespace called name, or the default one.
func (s *Server) namespace(name string) (*server.Namespace, error) {
	if name == "" {
		return s.opts.Emulator.Namespaces()[0], nil
	}
	n, ok := s.opts.Emulator.Namespace(name)
	if !ok {
		var names []string
		for _, n := range s.opts.Emulator.Namespaces() {
			names = append(names, n.Name())
		}
		return nil, fmt.Errorf("namespace %q not found; there are: %s", name, strings.Join(names, ", "))
	}
	return n, nil
}

// schemaOf describes t as a JSON Schema object.
func schemaOf(t reflect.Type) map[string]any {
	props := map[string]any{}
	var required []string
	collectFields(t, props, &required)
	s := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func collectFields(t reflect.Type, props map[string]any, required *[]string) {
	for i := range t.NumField() {
		f := t.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			collectFields(f.Type, props, required)
			continue
		}
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			continue
		}
		s := typeSchema(f.Type)
		if d := f.Tag.Get("desc"); d != "" {
			s["description"] = d
		}
		if e := f.Tag.Get("enum"); e != "" {
			s["enum"] = strings.Split(e, ",")
		}
		props[name] = s
		if f.Tag.Get("required") == "true" {
			*required = append(*required, name)
		}
	}
}

func typeSchema(t reflect.Type) map[string]any {
	if t == reflect.TypeFor[Duration]() {
		return map[string]any{"type": []string{"string", "number"}}
	}
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int32, reflect.Int64:
		return map[string]any{"type": "integer"}
	case reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Pointer:
		return typeSchema(t.Elem())
	case reflect.Slice:
		return map[string]any{"type": "array", "items": typeSchema(t.Elem())}
	case reflect.Struct:
		return schemaOf(t)
	case reflect.Map:
		return map[string]any{"type": "object"}
	}
	return map[string]any{} // any value
}
