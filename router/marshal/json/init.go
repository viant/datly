package json

import (
	"bytes"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
)

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
	typesPool = map[reflect.Type]*xunsafe.Type{}

}
