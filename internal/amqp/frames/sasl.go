package frames

import (
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
)

// SASL outcome codes.
const (
	SASLCodeOK      uint8 = 0
	SASLCodeAuth    uint8 = 1
	SASLCodeSys     uint8 = 2
	SASLCodeSysPerm uint8 = 3
	SASLCodeSysTemp uint8 = 4
)

// SASLMechanisms advertises the mechanisms the server supports.
type SASLMechanisms struct {
	Mechanisms []encoding.Symbol
}

func (*SASLMechanisms) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (m *SASLMechanisms) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeSASLMechanisms, m.Mechanisms))
}

func decodeSASLMechanisms(v any) (*SASLMechanisms, error) {
	f, err := newFields("sasl-mechanisms", v)
	if err != nil {
		return nil, err
	}
	return &SASLMechanisms{Mechanisms: f.symbols(0)}, f.err
}

// SASLInit selects a mechanism and carries the initial response.
type SASLInit struct {
	Mechanism       encoding.Symbol
	InitialResponse []byte
	Hostname        string
}

func (*SASLInit) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (i *SASLInit) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeSASLInit, i.Mechanism, i.InitialResponse, i.Hostname))
}

func decodeSASLInit(v any) (*SASLInit, error) {
	f, err := newFields("sasl-init", v)
	if err != nil {
		return nil, err
	}
	return &SASLInit{
		Mechanism:       f.symbol(0),
		InitialResponse: f.binary(1),
		Hostname:        f.string(2),
	}, f.err
}

// SASLResponse carries a response to a SASL challenge.
type SASLResponse struct {
	Response []byte
}

func (*SASLResponse) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (r *SASLResponse) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeSASLResponse, r.Response))
}

func decodeSASLResponse(v any) (*SASLResponse, error) {
	f, err := newFields("sasl-response", v)
	if err != nil {
		return nil, err
	}
	return &SASLResponse{Response: f.binary(0)}, f.err
}

// SASLOutcome reports the result of the SASL exchange.
type SASLOutcome struct {
	Code           uint8
	AdditionalData []byte
}

func (*SASLOutcome) isBody() {}

// MarshalAMQP implements encoding.Marshaler.
func (o *SASLOutcome) MarshalAMQP(w *encoding.Writer) error {
	// The code is mandatory, so it is never encoded as null.
	return w.Write(encoding.Described{
		Descriptor: uint64(codeSASLOutcome),
		Value:      trimNulls([]any{o.Code, opt(o.AdditionalData)}),
	})
}

func decodeSASLOutcome(v any) (*SASLOutcome, error) {
	f, err := newFields("sasl-outcome", v)
	if err != nil {
		return nil, err
	}
	o := &SASLOutcome{AdditionalData: f.binary(1)}
	if c := f.uint8Ptr(0); c != nil {
		o.Code = *c
	}
	return o, f.err
}

func trimNulls(list []any) []any {
	n := len(list)
	for n > 0 && list[n-1] == nil {
		n--
	}
	return list[:n]
}
