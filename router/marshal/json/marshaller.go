package json

import (
	"github.com/francoispqt/gojay"
	"unsafe"
)

type Marshaler interface {
	MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error
	UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error
}
