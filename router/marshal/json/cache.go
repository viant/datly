package json

import (
	"bytes"
	"reflect"
	"sync"
	"time"
)

var timeType = reflect.TypeOf(time.Time{})
var bufferPool *BufferPool
var sliceStringifier = sync.Map{}

type BufferPool struct {
	pool *sync.Pool
}

func (p *BufferPool) Get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

func (p *BufferPool) Put(buffer *bytes.Buffer) {
	buffer.Reset()
	p.pool.Put(buffer)
}
