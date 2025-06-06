package json

import (
	"bytes"
	"encoding/json"
	"github.com/francoispqt/gojay"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"unsafe"
)

var rawMessageType = reflect.TypeOf(json.RawMessage{})
var unmarshallerIntoType = reflect.TypeOf((*UnmarshalerInto)(nil)).Elem()
var mapStringIfaceType = reflect.TypeOf(map[string]interface{}{})
var decData *xunsafe.Field
var decCur *xunsafe.Field
var decErr *xunsafe.Field
var decCalled *xunsafe.Field

func init() {
	ResetCache()
	if decDataField, ok := reflect.TypeOf(gojay.Decoder{}).FieldByName("data"); ok {
		decData = xunsafe.NewField(decDataField)
	}
	if decCurField, ok := reflect.TypeOf(gojay.Decoder{}).FieldByName("cursor"); ok {
		decCur = xunsafe.NewField(decCurField)
	}
	if decCurField, ok := reflect.TypeOf(gojay.Decoder{}).FieldByName("err"); ok {
		decErr = xunsafe.NewField(decCurField)
	}
	if decCalledField, ok := reflect.TypeOf(gojay.Decoder{}).FieldByName("called"); ok {
		decCalled = xunsafe.NewField(decCalledField)
	}
}

func decoderError(decoder *gojay.Decoder) error {
	if decErr == nil {
		return nil
	}
	decPtr := unsafe.Pointer(decoder)
	return decErr.Error(decPtr)
}

func ResetCache() {
	buffersPool = &buffers{}
	buffersPool.pool = &sync.Pool{
		New: func() interface{} {
			buffer := bytes.Buffer{}
			return &buffer
		},
	}

	buffersPool.put(buffersPool.get())
	types = &typesPool{
		xtypesMap: sync.Map{},
	}

	namesIndex = &namesCaseIndex{registry: map[text.CaseFormat]map[string]string{}}
}

func skipNull(decoder *gojay.Decoder) bool {
	if decData == nil || decCur == nil {
		return false
	}
	decPtr := unsafe.Pointer(decoder)
	data := decData.Bytes(decPtr)
	cur := decCur.IntAddr(decPtr)
	if *cur < len(data) && data[*cur] == 'n' { //handles nil arrays case, origianl gojay has poor implementation
		var i *int
		_ = decoder.AddIntNull(&i)
		return true
	}
	return false
}
