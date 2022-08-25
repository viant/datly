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
var typesPool *TypesPool

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

type TypesPool struct {
	xtypesMap sync.Map
}

func GetXType(rType reflect.Type) *xunsafe.Type {
	load, ok := typesPool.xtypesMap.Load(rType)
	if ok {
		return load.(*xunsafe.Type)
	}

	xType := xunsafe.NewType(rType)
	typesPool.xtypesMap.Store(rType, xType)
	return xType
}
