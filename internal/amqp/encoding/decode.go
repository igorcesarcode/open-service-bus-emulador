package encoding

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unicode/utf8"
)

// Unmarshal decodes a single value that must span all of data.
func Unmarshal(data []byte) (any, error) {
	r := NewReader(data)
	v, err := r.Read()
	if err != nil {
		return nil, err
	}
	if r.Len() != 0 {
		return nil, r.errorf("%d trailing bytes", r.Len())
	}
	return v, nil
}

// Reader decodes consecutive AMQP values from a byte slice.
// Decoded []byte values alias the input.
type Reader struct {
	buf []byte
	off int
}

// NewReader returns a Reader that decodes data.
func NewReader(data []byte) *Reader {
	return &Reader{buf: data}
}

// Len returns the number of unread bytes.
func (r *Reader) Len() int { return len(r.buf) - r.off }

// Offset returns the number of bytes consumed so far.
func (r *Reader) Offset() int { return r.off }

// Remaining returns the unread bytes without consuming them.
func (r *Reader) Remaining() []byte { return r.buf[r.off:] }

// PeekCode returns the next format code without consuming it.
func (r *Reader) PeekCode() (byte, bool) {
	if r.Len() == 0 {
		return 0, false
	}
	return r.buf[r.off], true
}

func (r *Reader) errorf(format string, args ...any) error {
	return &Error{Offset: r.off, Msg: fmt.Sprintf(format, args...)}
}

func (r *Reader) next(n int) ([]byte, error) {
	if n < 0 || r.Len() < n {
		return nil, r.errorf("need %d bytes, have %d", n, r.Len())
	}
	b := r.buf[r.off : r.off+n]
	r.off += n
	return b, nil
}

func (r *Reader) byte() (byte, error) {
	b, err := r.next(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *Reader) uint32() (uint32, error) {
	b, err := r.next(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

// Read decodes the next value.
func (r *Reader) Read() (any, error) {
	code, err := r.byte()
	if err != nil {
		return nil, err
	}
	if code == codeDescribed {
		desc, err := r.Read()
		if err != nil {
			return nil, err
		}
		val, err := r.Read()
		if err != nil {
			return nil, err
		}
		return Described{Descriptor: desc, Value: val}, nil
	}
	return r.readValue(code)
}

// readValue decodes the value that follows the format code.
func (r *Reader) readValue(code byte) (any, error) {
	switch code {
	case codeNull:
		return nil, nil
	case codeBoolTrue:
		return true, nil
	case codeBoolFalse:
		return false, nil
	case codeBool:
		b, err := r.byte()
		if err != nil {
			return nil, err
		}
		switch b {
		case 0:
			return false, nil
		case 1:
			return true, nil
		}
		return nil, r.errorf("invalid boolean byte 0x%02x", b)
	case codeUint0:
		return uint32(0), nil
	case codeUlong0:
		return uint64(0), nil
	case codeList0:
		return []any{}, nil
	case codeUbyte:
		return r.byte()
	case codeByte:
		b, err := r.byte()
		return int8(b), err
	case codeSmallUint:
		b, err := r.byte()
		return uint32(b), err
	case codeSmallUlong:
		b, err := r.byte()
		return uint64(b), err
	case codeSmallInt:
		b, err := r.byte()
		return int32(int8(b)), err
	case codeSmallLong:
		b, err := r.byte()
		return int64(int8(b)), err
	case codeUshort:
		b, err := r.next(2)
		if err != nil {
			return nil, err
		}
		return binary.BigEndian.Uint16(b), nil
	case codeShort:
		b, err := r.next(2)
		if err != nil {
			return nil, err
		}
		return int16(binary.BigEndian.Uint16(b)), nil
	case codeUint:
		return r.uint32()
	case codeInt:
		v, err := r.uint32()
		return int32(v), err
	case codeFloat:
		v, err := r.uint32()
		return math.Float32frombits(v), err
	case codeChar:
		v, err := r.uint32()
		return Char(v), err
	case codeDecimal32:
		b, err := r.next(4)
		if err != nil {
			return nil, err
		}
		return Decimal32(b), nil
	case codeUlong, codeLong, codeDouble, codeTimestamp:
		b, err := r.next(8)
		if err != nil {
			return nil, err
		}
		v := binary.BigEndian.Uint64(b)
		switch code {
		case codeUlong:
			return v, nil
		case codeLong:
			return int64(v), nil
		case codeDouble:
			return math.Float64frombits(v), nil
		default:
			return time.UnixMilli(int64(v)).UTC(), nil
		}
	case codeDecimal64:
		b, err := r.next(8)
		if err != nil {
			return nil, err
		}
		return Decimal64(b), nil
	case codeDecimal128:
		b, err := r.next(16)
		if err != nil {
			return nil, err
		}
		return Decimal128(b), nil
	case codeUUID:
		b, err := r.next(16)
		if err != nil {
			return nil, err
		}
		return UUID(b), nil
	case codeVbin8, codeStr8, codeSym8:
		n, err := r.byte()
		if err != nil {
			return nil, err
		}
		return r.variable(code, int(n))
	case codeVbin32, codeStr32, codeSym32:
		n, err := r.uint32()
		if err != nil {
			return nil, err
		}
		return r.variable(code, int(n))
	case codeList8, codeList32:
		size, count, err := r.compoundHeader(code == codeList8, true)
		if err != nil {
			return nil, err
		}
		return r.readList(size, count)
	case codeMap8, codeMap32:
		size, count, err := r.compoundHeader(code == codeMap8, true)
		if err != nil {
			return nil, err
		}
		return r.readMap(size, count)
	case codeArray8, codeArray32:
		size, count, err := r.compoundHeader(code == codeArray8, false)
		if err != nil {
			return nil, err
		}
		return r.readArray(size, count)
	}
	return nil, r.errorf("unknown format code 0x%02x", code)
}

func (r *Reader) variable(code byte, n int) (any, error) {
	b, err := r.next(n)
	if err != nil {
		return nil, err
	}
	switch code {
	case codeVbin8, codeVbin32:
		return b, nil
	case codeStr8, codeStr32:
		if !utf8.Valid(b) {
			return nil, r.errorf("string is not valid UTF-8")
		}
		return string(b), nil
	default:
		return Symbol(b), nil
	}
}

// compoundHeader reads the size and count fields of a list, map or array.
// The returned size excludes the count field. When boundCount is set, the
// count is checked against the size, since every list or map element takes
// at least one byte.
func (r *Reader) compoundHeader(small, boundCount bool) (size, count int, err error) {
	if small {
		s, err := r.byte()
		if err != nil {
			return 0, 0, err
		}
		if s < 1 {
			return 0, 0, r.errorf("compound size %d too small", s)
		}
		c, err := r.byte()
		if err != nil {
			return 0, 0, err
		}
		size, count = int(s)-1, int(c)
	} else {
		s, err := r.uint32()
		if err != nil {
			return 0, 0, err
		}
		if s < 4 {
			return 0, 0, r.errorf("compound size %d too small", s)
		}
		c, err := r.uint32()
		if err != nil {
			return 0, 0, err
		}
		size, count = int(s)-4, int(c)
	}
	if size > r.Len() {
		return 0, 0, r.errorf("compound size %d exceeds %d remaining bytes", size, r.Len())
	}
	if boundCount && count > size {
		return 0, 0, r.errorf("compound count %d exceeds size %d", count, size)
	}
	return size, count, nil
}

// sub returns a Reader over the next size bytes and advances past them.
func (r *Reader) sub(size int) (*Reader, error) {
	b, err := r.next(size)
	if err != nil {
		return nil, err
	}
	return &Reader{buf: r.buf[:r.off], off: r.off - len(b)}, nil
}

func (r *Reader) readList(size, count int) ([]any, error) {
	s, err := r.sub(size)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, count)
	for range count {
		v, err := s.Read()
		if err != nil {
			return nil, err
		}
		list = append(list, v)
	}
	if s.Len() != 0 {
		return nil, s.errorf("%d unread bytes in list", s.Len())
	}
	return list, nil
}

func (r *Reader) readMap(size, count int) (Map, error) {
	if count%2 != 0 {
		return nil, r.errorf("map has odd element count %d", count)
	}
	s, err := r.sub(size)
	if err != nil {
		return nil, err
	}
	m := make(Map, 0, count/2)
	for range count / 2 {
		k, err := s.Read()
		if err != nil {
			return nil, err
		}
		v, err := s.Read()
		if err != nil {
			return nil, err
		}
		m = append(m, MapEntry{Key: k, Value: v})
	}
	if s.Len() != 0 {
		return nil, s.errorf("%d unread bytes in map", s.Len())
	}
	return m, nil
}

func (r *Reader) readArray(size, count int) (Array, error) {
	s, err := r.sub(size)
	if err != nil {
		return nil, err
	}
	code, err := s.byte()
	if err != nil {
		return nil, err
	}
	var descriptor any
	described := code == codeDescribed
	if described {
		if descriptor, err = s.Read(); err != nil {
			return nil, err
		}
		if code, err = s.byte(); err != nil {
			return nil, err
		}
	}
	if zeroWidth(code) && count > maxZeroWidthArray || !zeroWidth(code) && count > s.Len() {
		return nil, s.errorf("array count %d exceeds size %d", count, size)
	}
	a := make(Array, 0, min(count, s.Len()+1))
	for range count {
		v, err := s.readValue(code)
		if err != nil {
			return nil, err
		}
		if described {
			v = Described{Descriptor: descriptor, Value: v}
		}
		a = append(a, v)
	}
	if s.Len() != 0 {
		return nil, s.errorf("%d unread bytes in array", s.Len())
	}
	return a, nil
}

// maxZeroWidthArray bounds arrays whose elements take no bytes, such as an
// array of nulls, so a tiny frame cannot request a huge allocation.
const maxZeroWidthArray = 1 << 16

func zeroWidth(code byte) bool {
	switch code {
	case codeNull, codeBoolTrue, codeBoolFalse, codeUint0, codeUlong0, codeList0:
		return true
	}
	return false
}
