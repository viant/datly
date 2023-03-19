package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"reflect"
	"unsafe"
)

type Marshaler interface {
	MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters, opts ...Option) error
	UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder, opts ...Option) error
}
