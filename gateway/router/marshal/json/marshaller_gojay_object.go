package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"unsafe"
)

// gojayObjectMarshaller delegates to gojay's Marshaler/UnmarshalerJSONObject when available,
// and falls back to the generic struct marshaller for the other direction.
type gojayObjectMarshaller struct {
	valueType    *xunsafe.Type
	addrType     *xunsafe.Type
	fallback     marshaler
	useMarshal   bool
	useUnmarshal bool
}

func newGojayObjectMarshaller(valueType *xunsafe.Type, addrType *xunsafe.Type, fallback marshaler, useMarshal, useUnmarshal bool) *gojayObjectMarshaller {
	return &gojayObjectMarshaller{
		valueType:    valueType,
		addrType:     addrType,
		fallback:     fallback,
		useMarshal:   useMarshal,
		useUnmarshal: useUnmarshal,
	}
}

func (g *gojayObjectMarshaller) MarshallObject(ptr unsafe.Pointer, session *MarshallSession) error {
	if ptr == nil {
		session.Write(nullBytes)
		return nil
	}

	if g.useMarshal {
		// Prefer pointer receiver if (*T) implements MarshalerJSONObject
		if m, ok := g.addrType.Value(ptr).(gojay.MarshalerJSONObject); ok {
			enc := gojay.NewEncoder(session.Buffer)
			return enc.EncodeObject(m)
		}
		// Fallback to value receiver if (T) implements MarshalerJSONObject
		if m, ok := g.valueType.Interface(ptr).(gojay.MarshalerJSONObject); ok {
			enc := gojay.NewEncoder(session.Buffer)
			return enc.EncodeObject(m)
		}
		// If neither matched at runtime, fallback to generic marshaller
	}
	return g.fallback.MarshallObject(ptr, session)
}

func (g *gojayObjectMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	if !g.useUnmarshal {
		return g.fallback.UnmarshallObject(pointer, decoder, auxiliaryDecoder, session)
	}

	d := decoder
	if auxiliaryDecoder != nil {
		d = auxiliaryDecoder
	}

	// Prefer pointer receiver only; value receiver cannot mutate destination reliably.
	if u, ok := g.addrType.Value(pointer).(gojay.UnmarshalerJSONObject); ok {
		return d.Object(u)
	}

	// If neither matched at runtime, fallback to generic unmarshaller
	return g.fallback.UnmarshallObject(pointer, decoder, auxiliaryDecoder, session)
}
