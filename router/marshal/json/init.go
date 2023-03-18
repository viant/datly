package json

import (
	"bytes"
	"encoding/json"
	"github.com/viant/toolbox/format"
	"reflect"
	"sync"
)

var rawMessageType = reflect.TypeOf(json.RawMessage{})

func init() {
	ResetCache()
}

func ResetCache() {
	bufferPool = &BufferPool{}
	bufferPool.pool = &sync.Pool{
		New: func() interface{} {
			buffer := bytes.Buffer{}
			return &buffer
		},
	}

	bufferPool.Put(bufferPool.Get())
	typesPool = &TypesPool{
		xtypesMap: sync.Map{},
	}

	namesCaseIndex = &NamesCaseIndex{registry: map[format.Case]map[string]string{}}
}
