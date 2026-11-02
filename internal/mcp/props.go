package mcp

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
)

var durationType = reflect.TypeFor[time.Duration]()

// propertyFields maps the JSON name of each entity property to its field.
func propertyFields() map[string]int {
	t := reflect.TypeFor[broker.Properties]()
	m := make(map[string]int, t.NumField())
	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		m[strings.ToLower(name)] = i
	}
	return m
}

// propertyNames lists the property names, for messages and descriptions.
func propertyNames() string {
	t := reflect.TypeFor[broker.Properties]()
	var names []string
	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// propsView returns the properties as JSON-friendly values: durations in
// ISO 8601, unset text left out.
func propsView(p broker.Properties, kind broker.EntityKind) map[string]any {
	out := map[string]any{}
	v := reflect.ValueOf(p)
	for i := range v.NumField() {
		f := v.Type().Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		fv := v.Field(i)
		switch {
		case f.Type == durationType:
			d := time.Duration(fv.Int())
			if d == 0 {
				continue
			}
			out[name] = duration.Format(d)
		case fv.Kind() == reflect.String:
			if fv.String() == "" {
				continue
			}
			out[name] = fv.String()
		case fv.Kind() == reflect.Int32 || fv.Kind() == reflect.Int64:
			if fv.Int() == 0 {
				continue
			}
			out[name] = fv.Int()
		default:
			out[name] = fv.Interface()
		}
	}
	if kind == broker.KindTopic {
		delete(out, "lockDuration")
		delete(out, "maxDeliveryCount")
	}
	return out
}

// applyProps sets on base the properties named in in, which uses the JSON
// names of broker.Properties. Durations are ISO 8601 text or seconds.
func applyProps(base *broker.Properties, in map[string]any) error {
	fields := propertyFields()
	v := reflect.ValueOf(base).Elem()
	for key, val := range in {
		i, ok := fields[strings.ToLower(key)]
		if !ok {
			return fmt.Errorf("unknown property %q; the properties are: %s", key, propertyNames())
		}
		f := v.Field(i)
		if err := setField(f, val); err != nil {
			return fmt.Errorf("property %s: %w", key, err)
		}
	}
	return nil
}

func setField(f reflect.Value, val any) error {
	switch {
	case f.Type() == durationType:
		d, err := durationOf(val)
		if err != nil {
			return err
		}
		f.SetInt(int64(d))
	case f.Kind() == reflect.Bool:
		b, ok := val.(bool)
		if !ok {
			return fmt.Errorf("want true or false, got %v", val)
		}
		f.SetBool(b)
	case f.Kind() == reflect.String:
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("want text, got %v", val)
		}
		f.SetString(s)
	case f.Kind() == reflect.Int32 || f.Kind() == reflect.Int64:
		n, ok := val.(float64)
		if !ok || n != math.Trunc(n) {
			return fmt.Errorf("want a whole number, got %v", val)
		}
		if f.OverflowInt(int64(n)) {
			return fmt.Errorf("%v is out of range", val)
		}
		f.SetInt(int64(n))
	default:
		return fmt.Errorf("unsupported type %s", f.Type())
	}
	return nil
}

// durationOf reads an ISO 8601 duration or a number of seconds.
func durationOf(val any) (time.Duration, error) {
	switch v := val.(type) {
	case string:
		return duration.Parse(v)
	case float64:
		return time.Duration(v * float64(time.Second)), nil
	}
	return 0, fmt.Errorf("want an ISO 8601 duration such as PT30S, or seconds, got %v", val)
}
