package json

import (
	"github.com/francoispqt/gojay"
	"reflect"
	"unsafe"
)

type Marshaler interface {
	MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error
	UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error
}
