package json

import (
	"github.com/francoispqt/gojay"
	"unsafe"
)

type marshaler interface {
	MarshallObject(ptr unsafe.Pointer, session *MarshallSession) error
	UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error
}
