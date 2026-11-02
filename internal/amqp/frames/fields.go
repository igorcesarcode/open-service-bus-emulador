package frames

import (
	"fmt"
	"reflect"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
)

// fields reads the positional fields of a decoded composite list.
// Missing trailing fields read as null, as the AMQP spec allows.
type fields struct {
	name string
	list []any
	err  error
}

func newFields(name string, v any) (*fields, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: composite value is %T, want list", name, v)
	}
	return &fields{name: name, list: list}, nil
}

func (f *fields) at(i int) any {
	if i < len(f.list) {
		return f.list[i]
	}
	return nil
}

func (f *fields) fail(i int, want string) {
	if f.err == nil {
		f.err = fmt.Errorf("%s: field %d is %T, want %s", f.name, i, f.at(i), want)
	}
}

func (f *fields) present(i int) bool { return f.at(i) != nil }

func (f *fields) string(i int) string {
	switch v := f.at(i).(type) {
	case nil:
		return ""
	case string:
		return v
	case encoding.Symbol:
		return string(v)
	}
	f.fail(i, "string")
	return ""
}

func (f *fields) symbol(i int) encoding.Symbol {
	return encoding.Symbol(f.string(i))
}

// symbols reads a multiple="true" symbol field, which may be a single
// symbol or an array of symbols.
func (f *fields) symbols(i int) []encoding.Symbol {
	switch v := f.at(i).(type) {
	case nil:
		return nil
	case encoding.Symbol:
		return []encoding.Symbol{v}
	case encoding.Array:
		out := make([]encoding.Symbol, 0, len(v))
		for _, e := range v {
			s, ok := e.(encoding.Symbol)
			if !ok {
				f.fail(i, "symbol array")
				return nil
			}
			out = append(out, s)
		}
		return out
	}
	f.fail(i, "symbol array")
	return nil
}

func (f *fields) bool(i int) bool {
	switch v := f.at(i).(type) {
	case nil:
		return false
	case bool:
		return v
	}
	f.fail(i, "boolean")
	return false
}

func (f *fields) uint64(i int) uint64 {
	v, ok := toUint64(f.at(i))
	if !ok {
		f.fail(i, "unsigned integer")
	}
	return v
}

func (f *fields) uint32(i int) uint32 {
	v := f.uint64(i)
	if v > 1<<32-1 {
		f.fail(i, "uint")
	}
	return uint32(v)
}

func (f *fields) uint32Ptr(i int) *uint32 {
	if !f.present(i) {
		return nil
	}
	v := f.uint32(i)
	return &v
}

func (f *fields) uint16Ptr(i int) *uint16 {
	if !f.present(i) {
		return nil
	}
	v := f.uint64(i)
	if v > 1<<16-1 {
		f.fail(i, "ushort")
	}
	u := uint16(v)
	return &u
}

func (f *fields) uint8Ptr(i int) *uint8 {
	if !f.present(i) {
		return nil
	}
	v := f.uint64(i)
	if v > 1<<8-1 {
		f.fail(i, "ubyte")
	}
	u := uint8(v)
	return &u
}

func (f *fields) binary(i int) []byte {
	switch v := f.at(i).(type) {
	case nil:
		return nil
	case []byte:
		return v
	}
	f.fail(i, "binary")
	return nil
}

func (f *fields) timestamp(i int) time.Time {
	switch v := f.at(i).(type) {
	case nil:
		return time.Time{}
	case time.Time:
		return v
	}
	f.fail(i, "timestamp")
	return time.Time{}
}

func (f *fields) fieldsMap(i int) encoding.Map {
	switch v := f.at(i).(type) {
	case nil:
		return nil
	case encoding.Map:
		return v
	}
	f.fail(i, "map")
	return nil
}

func toUint64(v any) (uint64, bool) {
	switch v := v.(type) {
	case nil:
		return 0, true
	case uint8:
		return uint64(v), true
	case uint16:
		return uint64(v), true
	case uint32:
		return uint64(v), true
	case uint64:
		return v, true
	}
	return 0, false
}

// composite encodes code with the given fields as a described list.
// Optional fields that are unset encode as null, and trailing nulls are
// dropped as the spec allows.
func composite(code uint64, list ...any) encoding.Described {
	for i, v := range list {
		list[i] = opt(v)
	}
	n := len(list)
	for n > 0 && list[n-1] == nil {
		n--
	}
	return encoding.Described{Descriptor: code, Value: list[:n]}
}

// opt maps unset optional values (nil pointers, nil collections, empty
// strings and zero times) to nil and dereferences pointers to primitives.
func opt(v any) any {
	if v == nil {
		return nil
	}
	if t, ok := v.(time.Time); ok {
		if t.IsZero() {
			return nil
		}
		return t
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		if _, ok := v.(encoding.Marshaler); ok {
			return v
		}
		return rv.Elem().Interface()
	case reflect.Map, reflect.Slice:
		if rv.IsNil() {
			return nil
		}
	case reflect.String:
		if rv.Len() == 0 {
			return nil
		}
	}
	return v
}
