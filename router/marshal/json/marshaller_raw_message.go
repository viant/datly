package json

import (
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
	if err := mainDecoder.DecodeString(&dst); err != nil {
		return err
	}

	*bytesPtr = []byte(dst)
	return nil
}

func (r *RawMessageMarshaller) MarshallObject(_ reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	aBytes := (*[]byte)(ptr)
	if aBytes == nil {
		sb.Write(nullBytes)
		return nil
	}

	sb.Write(*aBytes)
	return nil
}