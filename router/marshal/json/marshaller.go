package json

import (
	"github.com/francoispqt/gojay"
	"unsafe"
)

type Marshaler interface {
	MarshallObject(ptr unsafe.Pointer, sb *Session) error
	UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error
}
