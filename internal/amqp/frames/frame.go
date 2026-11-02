package frames

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
)

// Frame types.
const (
	TypeAMQP uint8 = 0
	TypeSASL uint8 = 1
)

// HeaderSize is the size of the fixed frame header.
const HeaderSize = 8

// MinMaxFrameSize is the smallest max-frame-size a peer may negotiate.
const MinMaxFrameSize = 512

// Protocol headers exchanged before any frame.
var (
	ProtocolHeaderAMQP = [8]byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}
	ProtocolHeaderSASL = [8]byte{'A', 'M', 'Q', 'P', 3, 1, 0, 0}
)

// Frame is a decoded frame. A nil Body is an empty (heartbeat) frame.
type Frame struct {
	Type    uint8
	Channel uint16
	Body    Body
}

// ErrFrameTooLarge reports a frame larger than the negotiated maximum.
var ErrFrameTooLarge = errors.New("amqp: frame exceeds max-frame-size")

// ReadFrame reads one frame from r. maxSize bounds the total frame size.
func ReadFrame(r io.Reader, maxSize uint32) (Frame, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	size := binary.BigEndian.Uint32(hdr[0:4])
	doff := hdr[4]
	f := Frame{Type: hdr[5], Channel: binary.BigEndian.Uint16(hdr[6:8])}
	if size < HeaderSize || doff < 2 || uint32(doff)*4 > size {
		return Frame{}, fmt.Errorf("amqp: malformed frame header % x", hdr)
	}
	if size > maxSize {
		return Frame{}, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, size, maxSize)
	}
	buf := make([]byte, size-HeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Frame{}, err
	}
	// Skip the extended header, which no current frame type defines.
	body := buf[int(doff)*4-HeaderSize:]
	if len(body) == 0 {
		return f, nil
	}
	b, err := Decode(body)
	if err != nil {
		return Frame{}, fmt.Errorf("amqp: decoding frame on channel %d: %w", f.Channel, err)
	}
	f.Body = b
	return f, nil
}

// AppendFrame appends the encoding of a frame with the given body and
// payload to buf. A nil body produces an empty frame.
func AppendFrame(buf []byte, typ uint8, channel uint16, body Body, payload []byte) ([]byte, error) {
	start := len(buf)
	buf = append(buf, 0, 0, 0, 0, 2, typ, byte(channel>>8), byte(channel))
	if body != nil {
		var w encoding.Writer
		if err := body.MarshalAMQP(&w); err != nil {
			return nil, err
		}
		buf = append(buf, w.Bytes()...)
		buf = append(buf, payload...)
	}
	size := len(buf) - start
	if size > 1<<32-1 {
		return nil, ErrFrameTooLarge
	}
	binary.BigEndian.PutUint32(buf[start:], uint32(size))
	return buf, nil
}

// PerformativeSize returns the encoded size of body, used to compute how
// much payload fits in a frame.
func PerformativeSize(body Body) (int, error) {
	var w encoding.Writer
	if err := body.MarshalAMQP(&w); err != nil {
		return 0, err
	}
	return w.Len(), nil
}
