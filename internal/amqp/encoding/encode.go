package encoding

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unicode/utf8"
)

// Marshal encodes v using the most compact AMQP encoding for its type.
func Marshal(v any) ([]byte, error) {
	var w Writer
	if err := w.Write(v); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// Writer accumulates AMQP-encoded values.
// The zero value is an empty writer ready to use.
type Writer struct {
	buf []byte
}

// Bytes returns the encoded data. The slice aliases the writer's buffer.
func (w *Writer) Bytes() []byte { return w.buf }

// Len returns the number of encoded bytes.
func (w *Writer) Len() int { return len(w.buf) }

// Reset discards all encoded data while keeping the allocated buffer.
func (w *Writer) Reset() { w.buf = w.buf[:0] }

// WriteRaw appends already encoded bytes.
func (w *Writer) WriteRaw(b []byte) { w.buf = append(w.buf, b...) }

// Write appends the encoding of v.
func (w *Writer) Write(v any) error {
	switch v := v.(type) {
	case nil:
		w.buf = append(w.buf, codeNull)
	case bool:
		if v {
			w.buf = append(w.buf, codeBoolTrue)
		} else {
			w.buf = append(w.buf, codeBoolFalse)
		}
	case uint8:
		w.buf = append(w.buf, codeUbyte, v)
	case uint16:
		w.buf = append(w.buf, codeUshort)
		w.buf = binary.BigEndian.AppendUint16(w.buf, v)
	case uint32:
		w.writeUint32(v)
	case uint64:
		w.writeUint64(v)
	case uint:
		w.writeUint64(uint64(v))
	case int8:
		w.buf = append(w.buf, codeByte, byte(v))
	case int16:
		w.buf = append(w.buf, codeShort)
		w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(v))
	case int32:
		w.writeInt32(v)
	case int64:
		w.writeInt64(v)
	case int:
		w.writeInt64(int64(v))
	case float32:
		w.buf = append(w.buf, codeFloat)
		w.buf = binary.BigEndian.AppendUint32(w.buf, math.Float32bits(v))
	case float64:
		w.buf = append(w.buf, codeDouble)
		w.buf = binary.BigEndian.AppendUint64(w.buf, math.Float64bits(v))
	case Decimal32:
		w.buf = append(w.buf, codeDecimal32)
		w.buf = append(w.buf, v[:]...)
	case Decimal64:
		w.buf = append(w.buf, codeDecimal64)
		w.buf = append(w.buf, v[:]...)
	case Decimal128:
		w.buf = append(w.buf, codeDecimal128)
		w.buf = append(w.buf, v[:]...)
	case Char:
		w.buf = append(w.buf, codeChar)
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v))
	case time.Time:
		w.buf = append(w.buf, codeTimestamp)
		w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(v.UnixMilli()))
	case UUID:
		w.buf = append(w.buf, codeUUID)
		w.buf = append(w.buf, v[:]...)
	case []byte:
		w.writeVariable(codeVbin8, codeVbin32, v)
	case string:
		if !utf8.ValidString(v) {
			return fmt.Errorf("amqp encoding: string is not valid UTF-8")
		}
		w.writeVariable(codeStr8, codeStr32, []byte(v))
	case Symbol:
		w.writeVariable(codeSym8, codeSym32, []byte(v))
	case []any:
		return w.writeList(v)
	case Map:
		return w.writeMap(v)
	case Array:
		return w.writeArray(v)
	case []Symbol:
		a := make(Array, len(v))
		for i, s := range v {
			a[i] = s
		}
		return w.writeArray(a)
	case Described:
		w.buf = append(w.buf, codeDescribed)
		if err := w.Write(v.Descriptor); err != nil {
			return err
		}
		return w.Write(v.Value)
	case *Described:
		if v == nil {
			w.buf = append(w.buf, codeNull)
			return nil
		}
		return w.Write(*v)
	case Marshaler:
		return v.MarshalAMQP(w)
	default:
		return fmt.Errorf("amqp encoding: unsupported type %T", v)
	}
	return nil
}

// Marshaler is implemented by types that encode themselves, such as
// composite types with a fixed descriptor.
type Marshaler interface {
	MarshalAMQP(w *Writer) error
}

func (w *Writer) writeUint32(v uint32) {
	switch {
	case v == 0:
		w.buf = append(w.buf, codeUint0)
	case v <= math.MaxUint8:
		w.buf = append(w.buf, codeSmallUint, byte(v))
	default:
		w.buf = append(w.buf, codeUint)
		w.buf = binary.BigEndian.AppendUint32(w.buf, v)
	}
}

func (w *Writer) writeUint64(v uint64) {
	switch {
	case v == 0:
		w.buf = append(w.buf, codeUlong0)
	case v <= math.MaxUint8:
		w.buf = append(w.buf, codeSmallUlong, byte(v))
	default:
		w.buf = append(w.buf, codeUlong)
		w.buf = binary.BigEndian.AppendUint64(w.buf, v)
	}
}

func (w *Writer) writeInt32(v int32) {
	if v >= math.MinInt8 && v <= math.MaxInt8 {
		w.buf = append(w.buf, codeSmallInt, byte(int8(v)))
		return
	}
	w.buf = append(w.buf, codeInt)
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v))
}

func (w *Writer) writeInt64(v int64) {
	if v >= math.MinInt8 && v <= math.MaxInt8 {
		w.buf = append(w.buf, codeSmallLong, byte(int8(v)))
		return
	}
	w.buf = append(w.buf, codeLong)
	w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(v))
}

func (w *Writer) writeVariable(code8, code32 byte, b []byte) {
	if len(b) <= math.MaxUint8 {
		w.buf = append(w.buf, code8, byte(len(b)))
	} else {
		w.buf = append(w.buf, code32)
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(len(b)))
	}
	w.buf = append(w.buf, b...)
}

// writeList encodes elems as an AMQP list.
func (w *Writer) writeList(elems []any) error {
	if len(elems) == 0 {
		w.buf = append(w.buf, codeList0)
		return nil
	}
	var body Writer
	for _, e := range elems {
		if err := body.Write(e); err != nil {
			return err
		}
	}
	w.writeCompound(codeList8, codeList32, len(elems), body.buf)
	return nil
}

func (w *Writer) writeMap(m Map) error {
	var body Writer
	for _, e := range m {
		if err := body.Write(e.Key); err != nil {
			return err
		}
		if err := body.Write(e.Value); err != nil {
			return err
		}
	}
	w.writeCompound(codeMap8, codeMap32, 2*len(m), body.buf)
	return nil
}

// writeCompound writes the size/count header shared by lists and maps.
// The size field covers the count field plus the element bytes.
func (w *Writer) writeCompound(code8, code32 byte, count int, body []byte) {
	if count <= math.MaxUint8 && len(body)+1 <= math.MaxUint8 {
		w.buf = append(w.buf, code8, byte(len(body)+1), byte(count))
	} else {
		w.buf = append(w.buf, code32)
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(len(body)+4))
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(count))
	}
	w.buf = append(w.buf, body...)
}

// writeArray encodes a homogeneous array. The element constructor is
// derived from the first element; an empty array uses the null constructor.
func (w *Writer) writeArray(a Array) error {
	var body Writer
	if len(a) == 0 {
		body.buf = append(body.buf, codeNull)
	} else {
		ctor, err := arrayConstructor(a)
		if err != nil {
			return err
		}
		body.buf = append(body.buf, ctor...)
		for _, e := range a {
			if err := body.writeArrayElement(ctor[len(ctor)-1], e); err != nil {
				return err
			}
		}
	}
	if len(a) <= math.MaxUint8 && body.Len()+1 <= math.MaxUint8 {
		w.buf = append(w.buf, codeArray8, byte(body.Len()+1), byte(len(a)))
	} else {
		w.buf = append(w.buf, codeArray32)
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(body.Len()+4))
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(len(a)))
	}
	w.buf = append(w.buf, body.buf...)
	return nil
}

// arrayConstructor picks one fixed-width constructor that can hold every
// element of a. Variable-width types use the 8-bit form when all elements
// fit, and the 32-bit form otherwise.
func arrayConstructor(a Array) ([]byte, error) {
	first := a[0]
	for _, e := range a[1:] {
		if fmt.Sprintf("%T", e) != fmt.Sprintf("%T", first) {
			return nil, fmt.Errorf("amqp encoding: array mixes %T and %T", first, e)
		}
	}
	maxLen := 0
	for _, e := range a {
		switch e := e.(type) {
		case []byte:
			maxLen = max(maxLen, len(e))
		case string:
			maxLen = max(maxLen, len(e))
		case Symbol:
			maxLen = max(maxLen, len(e))
		}
	}
	pick := func(code8, code32 byte) []byte {
		if maxLen <= math.MaxUint8 {
			return []byte{code8}
		}
		return []byte{code32}
	}
	switch first.(type) {
	case bool:
		return []byte{codeBool}, nil
	case uint8:
		return []byte{codeUbyte}, nil
	case uint16:
		return []byte{codeUshort}, nil
	case uint32:
		return []byte{codeUint}, nil
	case uint64, uint:
		return []byte{codeUlong}, nil
	case int8:
		return []byte{codeByte}, nil
	case int16:
		return []byte{codeShort}, nil
	case int32:
		return []byte{codeInt}, nil
	case int64, int:
		return []byte{codeLong}, nil
	case float32:
		return []byte{codeFloat}, nil
	case float64:
		return []byte{codeDouble}, nil
	case Char:
		return []byte{codeChar}, nil
	case time.Time:
		return []byte{codeTimestamp}, nil
	case UUID:
		return []byte{codeUUID}, nil
	case []byte:
		return pick(codeVbin8, codeVbin32), nil
	case string:
		return pick(codeStr8, codeStr32), nil
	case Symbol:
		return pick(codeSym8, codeSym32), nil
	case []any:
		return []byte{codeList32}, nil
	case Map:
		return []byte{codeMap32}, nil
	default:
		return nil, fmt.Errorf("amqp encoding: unsupported array element type %T", first)
	}
}

// writeArrayElement writes e without its constructor, using the given code.
func (w *Writer) writeArrayElement(code byte, e any) error {
	var tmp Writer
	switch code {
	case codeBool:
		if e.(bool) {
			w.buf = append(w.buf, 1)
		} else {
			w.buf = append(w.buf, 0)
		}
		return nil
	case codeUint:
		w.buf = binary.BigEndian.AppendUint32(w.buf, e.(uint32))
		return nil
	case codeUlong:
		switch e := e.(type) {
		case uint64:
			w.buf = binary.BigEndian.AppendUint64(w.buf, e)
		case uint:
			w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(e))
		}
		return nil
	case codeInt:
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(e.(int32)))
		return nil
	case codeLong:
		switch e := e.(type) {
		case int64:
			w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(e))
		case int:
			w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(e))
		}
		return nil
	case codeVbin8, codeStr8, codeSym8:
		w.buf = append(w.buf, byte(len(variableBytes(e))))
		w.buf = append(w.buf, variableBytes(e)...)
		return nil
	case codeVbin32, codeStr32, codeSym32:
		b := variableBytes(e)
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(len(b)))
		w.buf = append(w.buf, b...)
		return nil
	case codeList32:
		var body Writer
		elems := e.([]any)
		for _, x := range elems {
			if err := body.Write(x); err != nil {
				return err
			}
		}
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(body.Len()+4))
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(len(elems)))
		w.buf = append(w.buf, body.buf...)
		return nil
	case codeMap32:
		var body Writer
		m := e.(Map)
		for _, x := range m {
			if err := body.Write(x.Key); err != nil {
				return err
			}
			if err := body.Write(x.Value); err != nil {
				return err
			}
		}
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(body.Len()+4))
		w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(2*len(m)))
		w.buf = append(w.buf, body.buf...)
		return nil
	}
	// Fixed-width types share their encoding with the standalone form, so
	// encode normally and drop the leading format code.
	if err := tmp.Write(e); err != nil {
		return err
	}
	w.buf = append(w.buf, tmp.buf[1:]...)
	return nil
}

func variableBytes(e any) []byte {
	switch e := e.(type) {
	case []byte:
		return e
	case string:
		return []byte(e)
	case Symbol:
		return []byte(e)
	}
	return nil
}
