package json

import (
	"bytes"
	"github.com/viant/toolbox/format"
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
	typesPool = &TypesRegistry{
		aMap: sync.Map{},
	}

	namesCaseIndex = &NamesCaseIndex{registry: map[format.Case]map[string]string{}}
}
