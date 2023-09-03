package json

import (
	"bytes"
	"encoding/json"
	"github.com/viant/toolbox/format"
	"reflect"
	"sync"
)

var rawMessageType = reflect.TypeOf(json.RawMessage{})
var unmarshallerIntoType = reflect.TypeOf((*UnmarshalerInto)(nil)).Elem()
var mapStringIfaceType = reflect.TypeOf(map[string]interface{}{})

func init() {
	ResetCache()
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

	namesIndex = &namesCaseIndex{registry: map[format.Case]map[string]string{}}
}
