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
var typesPool *TypesRegistry

type TypesRegistry struct {
	aMap sync.Map
}

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
	load, ok := typesPool.aMap.Load(rType)
	if ok {
		xType, ok := load.(*xunsafe.Type)
		if ok {
			return xType
		}
	}

	xType := xunsafe.NewType(rType)
	typesPool.aMap.Store(rType, xType)
	return xType
}
