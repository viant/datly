package json

import (
	stdjson "encoding/json"
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"unsafe"
)

type rawMessageMarshaller struct{}

func newRawMessageMarshaller() *rawMessageMarshaller {
	return &rawMessageMarshaller{}
}

func (r *rawMessageMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	bytesPtr := xunsafe.AsBytesPtr(pointer)
	// Decode arbitrary JSON value into interface{}, then re-marshal to raw bytes.
	var val interface{}
	if err := decoder.AddInterface(&val); err != nil {
		return err
	}
	data, err := stdjson.Marshal(val)
	if err != nil {
		return err
	}
	*bytesPtr = data
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
