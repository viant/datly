package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"reflect"
	"unsafe"
)

type Marshaler interface {
	MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters, opts ...MarshallOption) error
	UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error
}

type MarshallOption interface{}
type MarshallOptions []MarshallOption

func (o MarshallOptions) Tag() *Tag {
	for _, candidate := range o {
		if value, ok := candidate.(*Tag); ok {
			return value
		}
	}
	return nil
}

func (o MarshallOptions) DefaultTag() *DefaultTag {
	for _, candidate := range o {
		if value, ok := candidate.(*DefaultTag); ok {
			return value
		}
	}
	return nil
}
