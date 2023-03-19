package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type RawMessageMarshaller struct{}

func NewRawMessageMarshaller() *RawMessageMarshaller {
	return &RawMessageMarshaller{}
}

func (r *RawMessageMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	bytesPtr := xunsafe.AsBytesPtr(pointer)
	dst := ""
	if err := mainDecoder.String(&dst); err != nil {
		return err
	}

	*bytesPtr = []byte(dst)
	return nil
}

func (r *RawMessageMarshaller) MarshallObject(_ reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters) error {
	aBytes := xunsafe.AsBytesPtr(ptr)
	if aBytes == nil {
		sb.Write(nullBytes)
		return nil
	}

	sb.Write(*aBytes)
	return nil
}
