package json

import (
	"bytes"
	"sync"
)

func init() {
	bufferPool = &BufferPool{}
	bufferPool.pool = &sync.Pool{
		New: func() interface{} {
			buffer := bytes.Buffer{}
			return &buffer
		},
	}

	bufferPool.Put(bufferPool.Get())
}
