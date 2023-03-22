package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"unsafe"
)

type rawMessageMarshaller struct{}

func newRawMessageMarshaller() *rawMessageMarshaller {
	return &rawMessageMarshaller{}
}

func (r *rawMessageMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshallSession) error {
	bytesPtr := xunsafe.AsBytesPtr(pointer)
	dst := ""
	if err := decoder.DecodeString(&dst); err != nil {
		return err
	}

	*bytesPtr = []byte(dst)
	return nil
}

func (r *rawMessageMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	aBytes := (*[]byte)(ptr)
	if aBytes == nil {
		sb.Write(nullBytes)
		return nil
	}

	sb.Write(*aBytes)
	return nil
}
