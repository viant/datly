package json

import (
	"bytes"
	"github.com/viant/datly/utils"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"time"
)

var timeType = reflect.TypeOf(time.Time{})
var bufferPool *BufferPool
var typesPool *TypesRegistry
var namesCaseIndex *NamesCaseIndex

type TypesRegistry struct {
	aMap sync.Map
}

type NamesCaseIndex struct {
	mux      sync.Mutex
	registry map[format.Case]map[string]string
}

func (n *NamesCaseIndex) FormatTo(value string, dstFormat format.Case) string {
	n.mux.Lock()
	defer n.mux.Unlock()

	registry, ok := n.registry[dstFormat]
	if !ok {
		registry = map[string]string{}
		n.registry[dstFormat] = registry
	}

	formated, ok := registry[value]
	if !ok {
		srcFormat, err := format.NewCase(utils.DetectCase(value))
		if err == nil {
			formated = srcFormat.Format(value, dstFormat)
		} else {
			formated = value
		}
		registry[value] = formated
	}

	return formated
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
