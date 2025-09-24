package json

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"unsafe"
)

// deferredMarshaller is a placeholder used to break recursive type graphs during construction.
// It forwards calls to the actual target once it is set.
type deferredMarshaller struct {
	target marshaler
}

func (d *deferredMarshaller) setTarget(m marshaler) {
	d.target = m
}

func (d *deferredMarshaller) MarshallObject(ptr unsafe.Pointer, session *MarshallSession) error {
	if d.target == nil {
		return fmt.Errorf("marshaller not initialized")
	}
	return d.target.MarshallObject(ptr, session)
}

func (d *deferredMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	if d.target == nil {
		return fmt.Errorf("marshaller not initialized")
	}
	return d.target.UnmarshallObject(pointer, decoder, auxiliaryDecoder, session)
}
