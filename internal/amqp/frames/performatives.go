// Package frames defines the AMQP 1.0 frame layout, the transport
// performatives, delivery states, terminus types and the SASL frames.
package frames

import (
	"fmt"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
)

// Descriptor codes of the composite types in AMQP 1.0, part 2 and 3.
const (
	codeOpen        = 0x10
	codeBegin       = 0x11
	codeAttach      = 0x12
	codeFlow        = 0x13
	codeTransfer    = 0x14
	codeDisposition = 0x15
	codeDetach      = 0x16
	codeEnd         = 0x17
	codeClose       = 0x18
	codeError       = 0x1d
	codeReceived    = 0x23
	codeAccepted    = 0x24
	codeRejected    = 0x25
	codeReleased    = 0x26
	codeModified    = 0x27
	codeSource      = 0x28
	codeTarget      = 0x29

	codeSASLMechanisms = 0x40
	codeSASLInit       = 0x41
	codeSASLChallenge  = 0x42
	codeSASLResponse   = 0x43
	codeSASLOutcome    = 0x44
)

// symbolicCodes maps descriptor names to their numeric codes, for peers
// that use symbolic descriptors.
var symbolicCodes = map[encoding.Symbol]uint64{
	"amqp:open:list":            codeOpen,
	"amqp:begin:list":           codeBegin,
	"amqp:attach:list":          codeAttach,
	"amqp:flow:list":            codeFlow,
	"amqp:transfer:list":        codeTransfer,
	"amqp:disposition:list":     codeDisposition,
	"amqp:detach:list":          codeDetach,
	"amqp:end:list":             codeEnd,
	"amqp:close:list":           codeClose,
	"amqp:error:list":           codeError,
	"amqp:received:list":        codeReceived,
	"amqp:accepted:list":        codeAccepted,
	"amqp:rejected:list":        codeRejected,
	"amqp:released:list":        codeReleased,
	"amqp:modified:list":        codeModified,
	"amqp:source:list":          codeSource,
	"amqp:target:list":          codeTarget,
	"amqp:sasl-mechanisms:list": codeSASLMechanisms,
	"amqp:sasl-init:list":       codeSASLInit,
	"amqp:sasl-challenge:list":  codeSASLChallenge,
	"amqp:sasl-response:list":   codeSASLResponse,
	"amqp:sasl-outcome:list":    codeSASLOutcome,
}

// DescriptorCode returns the numeric code of a descriptor, resolving
// symbolic descriptors.
func DescriptorCode(d any) (uint64, bool) {
	switch d := d.(type) {
	case uint64:
		return d, true
	case encoding.Symbol:
		c, ok := symbolicCodes[d]
		return c, ok
	}
	return 0, false
}

// Role identifies the side of a link.
type Role bool

const (
	// RoleSender is the side that sends messages.
	RoleSender Role = false
	// RoleReceiver is the side that receives messages.
	RoleReceiver Role = true
)

func (r Role) String() string {
	if r == RoleReceiver {
		return "receiver"
	}
	return "sender"
}

// SenderSettleMode is the settlement policy of a sender.
type SenderSettleMode uint8

const (
	// SenderSettleModeUnsettled sends all deliveries unsettled.
	SenderSettleModeUnsettled SenderSettleMode = 0
	// SenderSettleModeSettled sends all deliveries settled.
	SenderSettleModeSettled SenderSettleMode = 1
	// SenderSettleModeMixed lets the sender choose per delivery.
	SenderSettleModeMixed SenderSettleMode = 2
)

// ReceiverSettleMode is the settlement policy of a receiver.
type ReceiverSettleMode uint8

const (
	// ReceiverSettleModeFirst settles spontaneously on receipt.
	ReceiverSettleModeFirst ReceiverSettleMode = 0
	// ReceiverSettleModeSecond settles only after the sender settles.
	ReceiverSettleModeSecond ReceiverSettleMode = 1
)

// Body is a performative or SASL frame body.
type Body interface {
	encoding.Marshaler
	isBody()
}

// Open negotiates connection parameters.
type Open struct {
	ContainerID         string
	Hostname            string
	MaxFrameSize        *uint32
	ChannelMax          *uint16
	IdleTimeout         time.Duration
	OutgoingLocales     []encoding.Symbol
	IncomingLocales     []encoding.Symbol
	OfferedCapabilities []encoding.Symbol
	DesiredCapabilities []encoding.Symbol
	Properties          encoding.Map
}

func (*Open) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (o *Open) MarshalAMQP(w *encoding.Writer) error {
	var idle *uint32
	if o.IdleTimeout > 0 {
		ms := uint32(o.IdleTimeout / time.Millisecond)
		idle = &ms
	}
	return w.Write(composite(codeOpen,
		o.ContainerID, o.Hostname, o.MaxFrameSize, o.ChannelMax, idle,
		o.OutgoingLocales, o.IncomingLocales, o.OfferedCapabilities,
		o.DesiredCapabilities, o.Properties))
}

func decodeOpen(v any) (*Open, error) {
	f, err := newFields("open", v)
	if err != nil {
		return nil, err
	}
	o := &Open{
		ContainerID:         f.string(0),
		Hostname:            f.string(1),
		MaxFrameSize:        f.uint32Ptr(2),
		ChannelMax:          f.uint16Ptr(3),
		OutgoingLocales:     f.symbols(5),
		IncomingLocales:     f.symbols(6),
		OfferedCapabilities: f.symbols(7),
		DesiredCapabilities: f.symbols(8),
		Properties:          f.fieldsMap(9),
	}
	if ms := f.uint32Ptr(4); ms != nil {
		o.IdleTimeout = time.Duration(*ms) * time.Millisecond
	}
	return o, f.err
}

// Begin starts a session.
type Begin struct {
	RemoteChannel       *uint16
	NextOutgoingID      uint32
	IncomingWindow      uint32
	OutgoingWindow      uint32
	HandleMax           *uint32
	OfferedCapabilities []encoding.Symbol
	DesiredCapabilities []encoding.Symbol
	Properties          encoding.Map
}

func (*Begin) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (b *Begin) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeBegin,
		b.RemoteChannel, b.NextOutgoingID, b.IncomingWindow, b.OutgoingWindow,
		b.HandleMax, b.OfferedCapabilities, b.DesiredCapabilities, b.Properties))
}

func decodeBegin(v any) (*Begin, error) {
	f, err := newFields("begin", v)
	if err != nil {
		return nil, err
	}
	return &Begin{
		RemoteChannel:       f.uint16Ptr(0),
		NextOutgoingID:      f.uint32(1),
		IncomingWindow:      f.uint32(2),
		OutgoingWindow:      f.uint32(3),
		HandleMax:           f.uint32Ptr(4),
		OfferedCapabilities: f.symbols(5),
		DesiredCapabilities: f.symbols(6),
		Properties:          f.fieldsMap(7),
	}, f.err
}

// Attach attaches a link to a session.
type Attach struct {
	Name               string
	Handle             uint32
	Role               Role
	SenderSettleMode   *uint8
	ReceiverSettleMode *uint8
	Source             *Source
	Target             *Target
	// Coordinator replaces Target on a link to a transaction coordinator.
	Coordinator          *Coordinator
	Unsettled            encoding.Map
	IncompleteUnsettled  bool
	InitialDeliveryCount *uint32
	MaxMessageSize       uint64
	OfferedCapabilities  []encoding.Symbol
	DesiredCapabilities  []encoding.Symbol
	Properties           encoding.Map
}

func (*Attach) isBody() {}

// SenderSettle returns the sender settle mode, applying the spec default.
func (a *Attach) SenderSettle() SenderSettleMode {
	if a.SenderSettleMode == nil {
		return SenderSettleModeMixed
	}
	return SenderSettleMode(*a.SenderSettleMode)
}

// ReceiverSettle returns the receiver settle mode, applying the spec default.
func (a *Attach) ReceiverSettle() ReceiverSettleMode {
	if a.ReceiverSettleMode == nil {
		return ReceiverSettleModeFirst
	}
	return ReceiverSettleMode(*a.ReceiverSettleMode)
}

// MarshalAMQP implements encoding.Marshaler.
func (a *Attach) MarshalAMQP(w *encoding.Writer) error {
	var maxSize any
	if a.MaxMessageSize > 0 {
		maxSize = a.MaxMessageSize
	}
	var incomplete any
	if a.IncompleteUnsettled {
		incomplete = true
	}
	var target any
	switch {
	case a.Coordinator != nil:
		target = a.Coordinator
	case a.Target != nil:
		target = a.Target
	}
	return w.Write(composite(codeAttach,
		a.Name, a.Handle, bool(a.Role), a.SenderSettleMode, a.ReceiverSettleMode,
		a.Source, target, a.Unsettled, incomplete, a.InitialDeliveryCount,
		maxSize, a.OfferedCapabilities, a.DesiredCapabilities, a.Properties))
}

func decodeAttach(v any) (*Attach, error) {
	f, err := newFields("attach", v)
	if err != nil {
		return nil, err
	}
	a := &Attach{
		Name:                 f.string(0),
		Handle:               f.uint32(1),
		Role:                 Role(f.bool(2)),
		SenderSettleMode:     f.uint8Ptr(3),
		ReceiverSettleMode:   f.uint8Ptr(4),
		Unsettled:            f.fieldsMap(7),
		IncompleteUnsettled:  f.bool(8),
		InitialDeliveryCount: f.uint32Ptr(9),
		MaxMessageSize:       f.uint64(10),
		OfferedCapabilities:  f.symbols(11),
		DesiredCapabilities:  f.symbols(12),
		Properties:           f.fieldsMap(13),
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.present(5) {
		if a.Source, err = decodeSource(f.at(5)); err != nil {
			return nil, err
		}
	}
	if f.present(6) {
		if d, ok := isCoordinator(f.at(6)); ok {
			if a.Coordinator, err = decodeCoordinator(d); err != nil {
				return nil, err
			}
		} else if a.Target, err = decodeTarget(f.at(6)); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// Flow updates session and link flow state.
type Flow struct {
	NextIncomingID *uint32
	IncomingWindow uint32
	NextOutgoingID uint32
	OutgoingWindow uint32
	Handle         *uint32
	DeliveryCount  *uint32
	LinkCredit     *uint32
	Available      *uint32
	Drain          bool
	Echo           bool
	Properties     encoding.Map
}

func (*Flow) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (fl *Flow) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeFlow,
		fl.NextIncomingID, fl.IncomingWindow, fl.NextOutgoingID, fl.OutgoingWindow,
		fl.Handle, fl.DeliveryCount, fl.LinkCredit, fl.Available,
		falseAsNull(fl.Drain), falseAsNull(fl.Echo), fl.Properties))
}

func decodeFlow(v any) (*Flow, error) {
	f, err := newFields("flow", v)
	if err != nil {
		return nil, err
	}
	return &Flow{
		NextIncomingID: f.uint32Ptr(0),
		IncomingWindow: f.uint32(1),
		NextOutgoingID: f.uint32(2),
		OutgoingWindow: f.uint32(3),
		Handle:         f.uint32Ptr(4),
		DeliveryCount:  f.uint32Ptr(5),
		LinkCredit:     f.uint32Ptr(6),
		Available:      f.uint32Ptr(7),
		Drain:          f.bool(8),
		Echo:           f.bool(9),
		Properties:     f.fieldsMap(10),
	}, f.err
}

// Transfer carries one frame of a message over a link. Payload holds the
// message bytes that follow the performative in the same frame.
type Transfer struct {
	Handle             uint32
	DeliveryID         *uint32
	DeliveryTag        []byte
	MessageFormat      *uint32
	Settled            bool
	More               bool
	ReceiverSettleMode *uint8
	State              DeliveryState
	Resume             bool
	Aborted            bool
	Batchable          bool

	Payload []byte
}

func (*Transfer) isBody() {}

// MarshalAMQP implements encoding.Marshaler. The payload is not included.
func (t *Transfer) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeTransfer,
		t.Handle, t.DeliveryID, t.DeliveryTag, t.MessageFormat,
		falseAsNull(t.Settled), falseAsNull(t.More), t.ReceiverSettleMode,
		stateValue(t.State), falseAsNull(t.Resume), falseAsNull(t.Aborted),
		falseAsNull(t.Batchable)))
}

func decodeTransfer(v any) (*Transfer, error) {
	f, err := newFields("transfer", v)
	if err != nil {
		return nil, err
	}
	t := &Transfer{
		Handle:             f.uint32(0),
		DeliveryID:         f.uint32Ptr(1),
		DeliveryTag:        f.binary(2),
		MessageFormat:      f.uint32Ptr(3),
		Settled:            f.bool(4),
		More:               f.bool(5),
		ReceiverSettleMode: f.uint8Ptr(6),
		Resume:             f.bool(8),
		Aborted:            f.bool(9),
		Batchable:          f.bool(10),
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.present(7) {
		if t.State, err = DecodeDeliveryState(f.at(7)); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// Disposition informs the peer of delivery state changes.
type Disposition struct {
	Role      Role
	First     uint32
	Last      *uint32
	Settled   bool
	State     DeliveryState
	Batchable bool
}

func (*Disposition) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (d *Disposition) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeDisposition,
		bool(d.Role), d.First, d.Last, falseAsNull(d.Settled), stateValue(d.State),
		falseAsNull(d.Batchable)))
}

func decodeDisposition(v any) (*Disposition, error) {
	f, err := newFields("disposition", v)
	if err != nil {
		return nil, err
	}
	d := &Disposition{
		Role:      Role(f.bool(0)),
		First:     f.uint32(1),
		Last:      f.uint32Ptr(2),
		Settled:   f.bool(3),
		Batchable: f.bool(5),
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.present(4) {
		if d.State, err = DecodeDeliveryState(f.at(4)); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// Detach detaches a link endpoint from a session.
type Detach struct {
	Handle uint32
	Closed bool
	Error  *Error
}

func (*Detach) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (d *Detach) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeDetach, d.Handle, falseAsNull(d.Closed), d.Error))
}

func decodeDetach(v any) (*Detach, error) {
	f, err := newFields("detach", v)
	if err != nil {
		return nil, err
	}
	d := &Detach{Handle: f.uint32(0), Closed: f.bool(1)}
	if f.err != nil {
		return nil, f.err
	}
	if f.present(2) {
		if d.Error, err = decodeError(f.at(2)); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// End ends a session.
type End struct {
	Error *Error
}

func (*End) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (e *End) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeEnd, e.Error))
}

// Close closes the connection.
type Close struct {
	Error *Error
}

func (*Close) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (c *Close) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeClose, c.Error))
}

func decodeErrorOnly(name string, v any) (*Error, error) {
	f, err := newFields(name, v)
	if err != nil {
		return nil, err
	}
	if !f.present(0) {
		return nil, nil
	}
	return decodeError(f.at(0))
}

// Error describes a link, session or connection error.
type Error struct {
	Condition   encoding.Symbol
	Description string
	Info        encoding.Map
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.Description == "" {
		return string(e.Condition)
	}
	return fmt.Sprintf("%s: %s", e.Condition, e.Description)
}

// MarshalAMQP implements encoding.Marshaler.
func (e *Error) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeError, e.Condition, e.Description, e.Info))
}

func decodeError(v any) (*Error, error) {
	d, ok := v.(encoding.Described)
	if !ok {
		return nil, fmt.Errorf("error: value is %T, want described list", v)
	}
	if code, _ := DescriptorCode(d.Descriptor); code != codeError {
		return nil, fmt.Errorf("error: unexpected descriptor %v", d.Descriptor)
	}
	f, err := newFields("error", d.Value)
	if err != nil {
		return nil, err
	}
	return &Error{
		Condition:   f.symbol(0),
		Description: f.string(1),
		Info:        f.fieldsMap(2),
	}, f.err
}

func falseAsNull(b bool) any {
	if b {
		return true
	}
	return nil
}

// Decode decodes the body of a frame: a described performative followed,
// for transfers, by the message payload.
func Decode(body []byte) (Body, error) {
	r := encoding.NewReader(body)
	v, err := r.Read()
	if err != nil {
		return nil, err
	}
	d, ok := v.(encoding.Described)
	if !ok {
		return nil, fmt.Errorf("frame body is %T, want described type", v)
	}
	code, ok := DescriptorCode(d.Descriptor)
	if !ok {
		return nil, fmt.Errorf("unknown frame descriptor %v", d.Descriptor)
	}
	var b Body
	switch code {
	case codeOpen:
		b, err = decodeOpen(d.Value)
	case codeBegin:
		b, err = decodeBegin(d.Value)
	case codeAttach:
		b, err = decodeAttach(d.Value)
	case codeFlow:
		b, err = decodeFlow(d.Value)
	case codeTransfer:
		var t *Transfer
		if t, err = decodeTransfer(d.Value); err == nil {
			t.Payload = r.Remaining()
			return t, nil
		}
	case codeDisposition:
		b, err = decodeDisposition(d.Value)
	case codeDetach:
		b, err = decodeDetach(d.Value)
	case codeEnd:
		var e *Error
		if e, err = decodeErrorOnly("end", d.Value); err == nil {
			b = &End{Error: e}
		}
	case codeClose:
		var e *Error
		if e, err = decodeErrorOnly("close", d.Value); err == nil {
			b = &Close{Error: e}
		}
	case codeSASLMechanisms:
		b, err = decodeSASLMechanisms(d.Value)
	case codeSASLInit:
		b, err = decodeSASLInit(d.Value)
	case codeSASLResponse:
		b, err = decodeSASLResponse(d.Value)
	case codeSASLOutcome:
		b, err = decodeSASLOutcome(d.Value)
	default:
		return nil, fmt.Errorf("unexpected frame descriptor 0x%x", code)
	}
	if err != nil {
		return nil, err
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("%d trailing bytes after performative", r.Len())
	}
	return b, nil
}
