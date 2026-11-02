package frames

import (
	"fmt"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
)

// Envelope is the editable part of an encoded message: the header, the
// annotation sections and the application properties. Every other section
// is kept byte for byte by RewriteMessage.
type Envelope struct {
	Header                *Header
	DeliveryAnnotations   encoding.Map
	MessageAnnotations    encoding.Map
	ApplicationProperties encoding.Map
	// ApplicationPropertiesModified must be set when ApplicationProperties
	// was changed; otherwise the original encoding is kept.
	ApplicationPropertiesModified bool
	// Properties is the properties section, nil if absent. Set
	// PropertiesModified after changing it; otherwise the original
	// encoding is kept.
	Properties         *Properties
	PropertiesModified bool
}

type rawSection struct {
	code uint64
	raw  []byte
}

// RewriteMessage decodes the envelope of payload, lets edit change it and
// returns the re-encoded message. Sections outside the envelope are copied
// unchanged, so the bare message keeps its exact original encoding.
func RewriteMessage(payload []byte, edit func(*Envelope) error) ([]byte, error) {
	env, rest, err := decodeEnvelope(payload)
	if err != nil {
		return nil, err
	}
	if err := edit(&env); err != nil {
		return nil, err
	}
	return encodeEnvelope(&env, rest)
}

// DecodeEnvelope decodes the envelope of payload without the body, for
// reading properties.
func DecodeEnvelope(payload []byte) (*Envelope, error) {
	env, _, err := decodeEnvelope(payload)
	if err != nil {
		return nil, err
	}
	return &env, nil
}

func decodeEnvelope(payload []byte) (Envelope, []rawSection, error) {
	var (
		env  Envelope
		rest []rawSection
	)
	r := encoding.NewReader(payload)
	for r.Len() > 0 {
		start := r.Offset()
		v, err := r.Read()
		if err != nil {
			return env, nil, fmt.Errorf("message: %w", err)
		}
		d, ok := v.(encoding.Described)
		if !ok {
			return env, nil, fmt.Errorf("message: section is %T, want described type", v)
		}
		code, _ := DescriptorCode(d.Descriptor)
		switch code {
		case codeHeader:
			if env.Header, err = decodeHeader(d.Value); err != nil {
				return env, nil, err
			}
		case codeDeliveryAnnotations:
			env.DeliveryAnnotations, err = sectionMap("delivery-annotations", d.Value)
		case codeMessageAnnotations:
			env.MessageAnnotations, err = sectionMap("message-annotations", d.Value)
		case codeApplicationProperties:
			env.ApplicationProperties, err = sectionMap("application-properties", d.Value)
			rest = append(rest, rawSection{code: code, raw: payload[start:r.Offset()]})
		case codeProperties:
			env.Properties, err = decodeProperties(d.Value)
			rest = append(rest, rawSection{code: code, raw: payload[start:r.Offset()]})
		default:
			rest = append(rest, rawSection{code: code, raw: payload[start:r.Offset()]})
		}
		if err != nil {
			return env, nil, err
		}
	}
	return env, rest, nil
}

func encodeEnvelope(env *Envelope, rest []rawSection) ([]byte, error) {
	head := &Message{
		Header:              env.Header,
		DeliveryAnnotations: env.DeliveryAnnotations,
		MessageAnnotations:  env.MessageAnnotations,
	}
	if len(head.DeliveryAnnotations) == 0 {
		head.DeliveryAnnotations = nil
	}
	if len(head.MessageAnnotations) == 0 {
		head.MessageAnnotations = nil
	}
	var w encoding.Writer
	if err := head.MarshalAMQP(&w); err != nil {
		return nil, err
	}

	writeAppProps := func(original []byte) error {
		if !env.ApplicationPropertiesModified {
			w.WriteRaw(original)
			return nil
		}
		return writeMapSection(&w, codeApplicationProperties, env.ApplicationProperties)
	}
	writeProps := func(original []byte) error {
		if !env.PropertiesModified {
			w.WriteRaw(original)
			return nil
		}
		if env.Properties == nil {
			return nil
		}
		return (&Message{Properties: env.Properties}).MarshalAMQP(&w)
	}
	// A new properties section goes first among the bare message sections.
	if env.PropertiesModified && !hasSection(rest, codeProperties) {
		if err := writeProps(nil); err != nil {
			return nil, err
		}
	}
	// A message without application properties gets the new section right
	// after its properties section, or first among the remaining sections.
	insertAt := -1
	if env.ApplicationPropertiesModified && !hasSection(rest, codeApplicationProperties) {
		insertAt = 0
		for i, s := range rest {
			if s.code == codeProperties {
				insertAt = i + 1
			}
		}
	}
	for i, s := range rest {
		if i == insertAt {
			if err := writeAppProps(nil); err != nil {
				return nil, err
			}
		}
		switch s.code {
		case codeApplicationProperties:
			if err := writeAppProps(s.raw); err != nil {
				return nil, err
			}
		case codeProperties:
			if err := writeProps(s.raw); err != nil {
				return nil, err
			}
		default:
			w.WriteRaw(s.raw)
		}
	}
	if insertAt == len(rest) {
		if err := writeAppProps(nil); err != nil {
			return nil, err
		}
	}
	return w.Bytes(), nil
}

func hasSection(sections []rawSection, code uint64) bool {
	for _, s := range sections {
		if s.code == code {
			return true
		}
	}
	return false
}
