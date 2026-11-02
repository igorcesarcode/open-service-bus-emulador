// Package encoding implements the AMQP 1.0 type system (OASIS AMQP 1.0,
// part 1: types): primitive values, described types and composite values.
//
// Decoded values map to Go types as follows:
//
//	null       nil
//	boolean    bool
//	ubyte      uint8
//	ushort     uint16
//	uint       uint32
//	ulong      uint64
//	byte       int8
//	short      int16
//	int        int32
//	long       int64
//	float      float32
//	double     float64
//	decimal32  Decimal32
//	decimal64  Decimal64
//	decimal128 Decimal128
//	char       Char
//	timestamp  time.Time (UTC, millisecond precision)
//	uuid       UUID
//	binary     []byte
//	string     string
//	symbol     Symbol
//	list       []any
//	map        Map
//	array      Array
//	described  Described
//
// The encoder accepts the same types, plus int (encoded as long), uint
// (encoded as ulong) and []Symbol (encoded as an array of symbols).
package encoding

import (
	"encoding/hex"
	"fmt"
	"reflect"
)

// Symbol is an AMQP symbolic value from a constrained domain, such as a
// field name or an error condition.
type Symbol string

// UUID is an AMQP universally unique identifier, stored in network byte order.
type UUID [16]byte

// String formats the UUID in the canonical 8-4-4-4-12 hexadecimal form.
func (u UUID) String() string {
	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], u[10:])
	return string(buf[:])
}

// Char is a single Unicode character (UTF-32BE on the wire).
type Char rune

// Decimal32 is an IEEE 754-2008 decimal32 value kept in its wire form.
type Decimal32 [4]byte

// Decimal64 is an IEEE 754-2008 decimal64 value kept in its wire form.
type Decimal64 [8]byte

// Decimal128 is an IEEE 754-2008 decimal128 value kept in its wire form.
type Decimal128 [16]byte

// Described is a value annotated with a descriptor. The descriptor is
// usually a uint64 code or a Symbol name.
type Described struct {
	Descriptor any
	Value      any
}

// MapEntry is one key/value pair of a Map.
type MapEntry struct {
	Key   any
	Value any
}

// Map is an AMQP map. It keeps wire order and allows keys of any AMQP type,
// including types that are not comparable in Go.
type Map []MapEntry

// Get returns the value stored under key.
func (m Map) Get(key any) (any, bool) {
	for _, e := range m {
		if keysEqual(e.Key, key) {
			return e.Value, true
		}
	}
	return nil, false
}

// Set replaces the value stored under key, or appends a new entry.
func (m *Map) Set(key, value any) {
	for i, e := range *m {
		if keysEqual(e.Key, key) {
			(*m)[i].Value = value
			return
		}
	}
	*m = append(*m, MapEntry{Key: key, Value: value})
}

// Delete removes the entry stored under key, if any.
func (m *Map) Delete(key any) {
	for i, e := range *m {
		if keysEqual(e.Key, key) {
			*m = append((*m)[:i], (*m)[i+1:]...)
			return
		}
	}
}

func keysEqual(a, b any) bool {
	ta, tb := reflect.TypeOf(a), reflect.TypeOf(b)
	if ta != tb {
		return false
	}
	if ta != nil && ta.Comparable() {
		return a == b
	}
	return reflect.DeepEqual(a, b)
}

// Array is an AMQP array: a sequence of values that share one type.
type Array []any

// Error reports malformed or unsupported encoded data.
type Error struct {
	Offset int
	Msg    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("amqp encoding: offset %d: %s", e.Offset, e.Msg)
}
