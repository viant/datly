package json

import (
	"bytes"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"time"
)

var timeType = reflect.TypeOf(time.Time{})
var bufferPool *BufferPool
var typesPool = map[reflect.Type]*xunsafe.Type{}

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

func GetXType(rType reflect.Type) *xunsafe.Type {
	load, ok := typesPool[rType]
	if ok {
		return load
	}

	xType := xunsafe.NewType(rType)
	typesPool[rType] = xType
	return xType
}
