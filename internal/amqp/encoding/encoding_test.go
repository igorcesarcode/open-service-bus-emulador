package encoding

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestRoundTrip(t *testing.T) {
	ts := time.UnixMilli(1735689600123).UTC()
	tests := []struct {
		name string
		in   any
	}{
		{"null", nil},
		{"true", true},
		{"false", false},
		{"ubyte", uint8(200)},
		{"ushort", uint16(60000)},
		{"uint zero", uint32(0)},
		{"uint small", uint32(255)},
		{"uint large", uint32(math.MaxUint32)},
		{"ulong zero", uint64(0)},
		{"ulong small", uint64(7)},
		{"ulong large", uint64(math.MaxUint64)},
		{"byte", int8(-128)},
		{"short", int16(-30000)},
		{"int small", int32(-100)},
		{"int large", int32(math.MinInt32)},
		{"long small", int64(127)},
		{"long large", int64(math.MaxInt64)},
		{"float", float32(1.5)},
		{"double", 3.25},
		{"decimal32", Decimal32{1, 2, 3, 4}},
		{"decimal64", Decimal64{1, 2, 3, 4, 5, 6, 7, 8}},
		{"decimal128", Decimal128{15: 9}},
		{"char", Char('é')},
		{"timestamp", ts},
		{"uuid", UUID{0: 0xde, 15: 0xad}},
		{"binary short", []byte{1, 2, 3}},
		{"binary long", bytes.Repeat([]byte{7}, 300)},
		{"string short", "olá"},
		{"string long", strings.Repeat("x", 1000)},
		{"symbol", Symbol("amqp:not-found")},
		{"empty list", []any{}},
		{"list", []any{uint32(1), "a", nil, true}},
		{"long list", func() []any {
			l := make([]any, 300)
			for i := range l {
				l[i] = int32(i)
			}
			return l
		}()},
		{"map", Map{{Key: Symbol("k"), Value: "v"}, {Key: "n", Value: int64(1)}}},
		{"empty map", Map{}},
		{"symbol array", Array{Symbol("a"), Symbol("bb")}},
		{"long symbol array", Array{Symbol(strings.Repeat("s", 300))}},
		{"int array", Array{int32(1), int32(-1), int32(1 << 20)}},
		{"uint array", Array{uint32(0), uint32(1), uint32(1 << 30)}},
		{"ulong array", Array{uint64(0), uint64(math.MaxUint64)}},
		{"long array", Array{int64(-1), int64(1 << 40)}},
		{"bool array", Array{true, false}},
		{"uuid array", Array{UUID{1}, UUID{2}}},
		{"timestamp array", Array{ts, ts}},
		{"list array", Array{[]any{"a"}, []any{}}},
		{"map array", Array{Map{{Key: "a", Value: "b"}}}},
		{"empty array", Array{}},
		{"described", Described{Descriptor: uint64(0x77), Value: "body"}},
		{"described symbol", Described{Descriptor: Symbol("amqp:value:*"), Value: []any{uint32(1)}}},
		{"nested", Map{{Key: "list", Value: []any{Map{{Key: uint64(1), Value: Array{Symbol("x")}}}}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := Marshal(tc.in)
			if err != nil {
				t.Fatalf("Marshal(%v) error: %v", tc.in, err)
			}
			got, err := Unmarshal(b)
			if err != nil {
				t.Fatalf("Unmarshal(% x) error: %v", b, err)
			}
			if diff := cmp.Diff(tc.in, got); diff != "" {
				t.Errorf("round trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMarshalCompactForms(t *testing.T) {
	tests := []struct {
		in   any
		want []byte
	}{
		{nil, []byte{0x40}},
		{true, []byte{0x41}},
		{uint32(0), []byte{0x43}},
		{uint32(10), []byte{0x52, 10}},
		{uint64(0), []byte{0x44}},
		{uint64(0x10), []byte{0x53, 0x10}},
		{int32(-1), []byte{0x54, 0xff}},
		{int64(5), []byte{0x55, 5}},
		{[]any{}, []byte{0x45}},
		{Symbol("ab"), []byte{0xa3, 2, 'a', 'b'}},
		{"ab", []byte{0xa1, 2, 'a', 'b'}},
		{[]any{true}, []byte{0xc0, 2, 1, 0x41}},
		{Map{{Key: Symbol("a"), Value: nil}}, []byte{0xc1, 5, 2, 0xa3, 1, 'a', 0x40}},
		{[]Symbol{"a", "b"}, []byte{0xe0, 6, 2, 0xa3, 1, 'a', 1, 'b'}},
		{Described{Descriptor: uint64(0x24), Value: []any{}}, []byte{0x00, 0x53, 0x24, 0x45}},
	}
	for _, tc := range tests {
		got, err := Marshal(tc.in)
		if err != nil {
			t.Fatalf("Marshal(%#v) error: %v", tc.in, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Errorf("Marshal(%#v) = % x, want % x", tc.in, got, tc.want)
		}
	}
}

func TestUnmarshalWideForms(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want any
	}{
		{"bool byte", []byte{0x56, 1}, true},
		{"uint32 wide", []byte{0x70, 0, 0, 0, 5}, uint32(5)},
		{"ulong wide", []byte{0x80, 0, 0, 0, 0, 0, 0, 0, 5}, uint64(5)},
		{"str32", []byte{0xb1, 0, 0, 0, 1, 'z'}, "z"},
		{"sym32", []byte{0xb3, 0, 0, 0, 1, 'z'}, Symbol("z")},
		{"vbin32", []byte{0xb0, 0, 0, 0, 1, 9}, []byte{9}},
		{"list32", []byte{0xd0, 0, 0, 0, 5, 0, 0, 0, 1, 0x40}, []any{nil}},
		{"map32", []byte{0xd1, 0, 0, 0, 6, 0, 0, 0, 2, 0x40, 0x40}, Map{{}}},
		{"array32", []byte{0xf0, 0, 0, 0, 6, 0, 0, 0, 1, 0x50, 3}, Array{uint8(3)}},
		{"described array", []byte{0xe0, 5, 2, 0x00, 0x53, 0x01, 0x41}, Array{
			Described{Descriptor: uint64(1), Value: true},
			Described{Descriptor: uint64(1), Value: true},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Unmarshal(tc.in)
			if err != nil {
				t.Fatalf("Unmarshal(% x) error: %v", tc.in, err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Unmarshal(% x) mismatch (-want +got):\n%s", tc.in, diff)
			}
		})
	}
}

func TestUnmarshalErrors(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		{"unknown code", []byte{0xff}},
		{"truncated uint", []byte{0x70, 0, 0}},
		{"truncated string", []byte{0xa1, 5, 'a'}},
		{"invalid utf8", []byte{0xa1, 1, 0xff}},
		{"invalid bool", []byte{0x56, 2}},
		{"list size beyond data", []byte{0xc0, 10, 1, 0x40}},
		{"list count beyond size", []byte{0xc0, 2, 200, 0x40}},
		{"map odd count", []byte{0xc1, 2, 1, 0x40}},
		{"huge zero width array", []byte{0xf0, 0, 0, 0, 5, 0xff, 0xff, 0xff, 0xff, 0x40}},
		{"trailing bytes", []byte{0x40, 0x40}},
		{"described missing value", []byte{0x00, 0x53, 0x10}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := Unmarshal(tc.in); err == nil {
				t.Errorf("Unmarshal(% x) = %v, want error", tc.in, got)
			}
		})
	}
}

func TestMarshalErrors(t *testing.T) {
	tests := []struct {
		name string
		in   any
	}{
		{"unsupported type", struct{}{}},
		{"invalid utf8", string([]byte{0xff})},
		{"mixed array", Array{int32(1), "a"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Marshal(tc.in); err == nil {
				t.Errorf("Marshal(%#v) succeeded, want error", tc.in)
			}
		})
	}
}

func TestMap(t *testing.T) {
	var m Map
	m.Set(Symbol("a"), 1)
	m.Set("a", 2)
	m.Set(Symbol("a"), 3)
	if got, _ := m.Get(Symbol("a")); got != 3 {
		t.Errorf("Get(Symbol(a)) = %v, want 3", got)
	}
	if got, _ := m.Get("a"); got != 2 {
		t.Errorf("Get(a) = %v, want 2: string and symbol keys must differ", got)
	}
	m.Delete("a")
	if _, ok := m.Get("a"); ok {
		t.Errorf("Get(a) after Delete found a value")
	}
	m.Set([]byte{1}, "binary key")
	if got, _ := m.Get([]byte{1}); got != "binary key" {
		t.Errorf("Get([]byte{1}) = %v, want binary key", got)
	}
}

func TestUUIDString(t *testing.T) {
	u := UUID{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	if got, want := u.String(), "12345678-9abc-def0-0123-456789abcdef"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
