package terraform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Terraform's JSON syntax (.tf.json and .tfvars.json) is read by writing the
// same configuration in the native syntax and parsing that, so the rest of
// the package sees one kind of body. A JSON string is a template, as a
// quoted string is in the native syntax, so "${var.x}" keeps its meaning.

// jsonBlockKeys are the keys of a resource body that are nested blocks
// rather than arguments with an object value.
var jsonBlockKeys = map[string]bool{
	"dynamic": true, "lifecycle": true, "timeouts": true, "provisioner": true, "connection": true,
	"correlation_filter": true, "customer_managed_key": true, "identity": true, "content": true,
}

// jsonTopLevel maps the top-level keys to how many labels their blocks have.
var jsonTopLevel = map[string]int{
	"terraform": 0, "provider": 1, "variable": 1, "module": 1, "resource": 2, "data": 2, "output": 1,
}

// convertJSON writes the Terraform JSON configuration in data as native
// syntax. With vars, data is the object of a .tfvars.json file.
func convertJSON(data []byte, vars bool) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	var w jsonWriter
	if vars {
		for _, k := range sortedKeys(doc) {
			w.attr(k, doc[k])
		}
		return w.bytes()
	}
	for _, key := range sortedKeys(doc) {
		switch key {
		case "locals":
			for _, body := range asObjects(doc[key], &w, key) {
				w.line("locals {")
				for _, k := range sortedKeys(body) {
					w.attr(k, body[k])
				}
				w.line("}")
			}
		case "//", "//comment":
		default:
			labels, ok := jsonTopLevel[key]
			if !ok {
				continue // Unknown top-level keys are ignored by the loader.
			}
			w.labelled(key, labels, nil, doc[key], false)
		}
	}
	return w.bytes()
}

type jsonWriter struct {
	b   strings.Builder
	err error
}

func (w *jsonWriter) bytes() ([]byte, error) { return []byte(w.b.String()), w.err }

func (w *jsonWriter) line(format string, args ...any) {
	fmt.Fprintf(&w.b, format+"\n", args...)
}

// asObjects returns v as a list of objects: an object, or an array of them.
func asObjects(v any, w *jsonWriter, what string) []map[string]any {
	switch x := v.(type) {
	case map[string]any:
		return []map[string]any{x}
	case []any:
		var out []map[string]any
		for _, e := range x {
			if m, ok := e.(map[string]any); ok {
				out = append(out, m)
				continue
			}
			w.fail("%s: want an object, got %T", what, e)
		}
		return out
	}
	w.fail("%s: want an object, got %T", what, v)
	return nil
}

func (w *jsonWriter) fail(format string, args ...any) {
	if w.err == nil {
		w.err = fmt.Errorf(format, args...)
	}
}

// labelled writes the blocks of type typ whose labels are the next n keys
// of v. dynamicBody marks a body whose keys are arguments or blocks of a
// resource.
func (w *jsonWriter) labelled(typ string, n int, labels []string, v any, resource bool) {
	if n == 0 {
		for _, body := range asObjects(v, w, typ) {
			w.block(typ, labels, body, typ == "resource" || typ == "data" || typ == "module" || resource)
		}
		return
	}
	for _, obj := range asObjects(v, w, typ) {
		for _, k := range sortedKeys(obj) {
			w.labelled(typ, n-1, append(append([]string(nil), labels...), k), obj[k], resource)
		}
	}
}

func (w *jsonWriter) block(typ string, labels []string, body map[string]any, resource bool) {
	head := typ
	for _, l := range labels {
		head += " " + quoteHCL(l, false)
	}
	w.line("%s {", head)
	for _, k := range sortedKeys(body) {
		v := body[k]
		switch {
		case typ == "variable" && k == "type":
			if s, ok := v.(string); ok {
				w.line("type = %s", strings.TrimSuffix(strings.TrimPrefix(s, "${"), "}"))
				continue
			}
			w.attr(k, v)
		case k == "dynamic" && resource:
			w.labelled("dynamic", 1, nil, v, true)
		case resource && jsonBlockKeys[k]:
			if _, isObj := v.(map[string]any); isObj {
				w.block(k, nil, v.(map[string]any), true)
			} else if list, isList := v.([]any); isList {
				for _, e := range asObjects(list, w, k) {
					w.block(k, nil, e, true)
				}
			} else {
				w.attr(k, v)
			}
		default:
			w.attr(k, v)
		}
	}
	w.line("}")
}

func (w *jsonWriter) attr(name string, v any) {
	w.line("%s = %s", quoteKey(name), w.expr(v))
}

func quoteKey(k string) string {
	for i, r := range k {
		ok := r == '_' || r == '-' && i > 0 || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' && i > 0
		if !ok {
			return quoteHCL(k, false)
		}
	}
	return k
}

func (w *jsonWriter) expr(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case bool:
		return fmt.Sprint(x)
	case json.Number:
		return x.String()
	case string:
		return quoteHCL(x, true)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = w.expr(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		var b strings.Builder
		b.WriteString("{\n")
		for _, k := range sortedKeys(x) {
			fmt.Fprintf(&b, "%s = %s\n", quoteHCL(k, false), w.expr(x[k]))
		}
		b.WriteString("}")
		return b.String()
	}
	w.fail("unsupported JSON value %T", v)
	return "null"
}

// quoteHCL writes s as a quoted string. With template, ${ and %{ stay
// live; otherwise they are escaped.
func quoteHCL(s string, template bool) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\':
			b.WriteString(`\\`)
		case c == '"':
			b.WriteString(`\"`)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '\r':
			b.WriteString(`\r`)
		case c == '\t':
			b.WriteString(`\t`)
		case (c == '$' || c == '%') && !template && i+1 < len(s) && s[i+1] == '{':
			b.WriteByte(c)
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}
