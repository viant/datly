package json

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
)

var bufferPool *BufferPool
var typesPool *TypesPool
var namesCaseIndex *NamesCaseIndex

type (
	TypesRegistry struct {
		aMap sync.Map
	}

	NamesCaseIndex struct {
		mux      sync.Mutex
		registry map[format.Case]map[string]string
	}

	Cache struct {
		pathCaches sync.Map // path -> PathCache
	}

	PathCache struct {
		parent *Cache
		path   string
		cache  sync.Map // rType -> Marshaler
	}
)

func NewCache() *Cache {
	return &Cache{pathCaches: sync.Map{}}
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
		srcFormat, err := format.NewCase(formatter.DetectCase(value))
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

func (m *Cache) LoadMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, defaultTag *DefaultTag) (Marshaler, error) {
	pathCache := m.pathCache(path)
	marshaller, err := pathCache.GetMarshaller(rType, config, path, outputPath, defaultTag)
	if err != nil {
		return nil, err
	}

	return marshaller, nil
}

func (c *PathCache) GetMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag) (Marshaler, error) {
	value, ok := c.cache.Load(rType)
	if ok {
		return value.(Marshaler), nil
	}

	marshaler, err := c.getMarshaler(rType, config, path, outputPath, tag)
	if err != nil {
		return nil, err
	}

	c.cache.Store(rType, marshaler)
	return marshaler, nil
}

func (c *PathCache) getMarshaler(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag) (Marshaler, error) {
	switch rType.Kind() {
	case reflect.Ptr:
		marshaller, err := NewPtrMarshaller(rType, config, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, err

	case reflect.Slice:
		marshaller, err := NewSliceMarshaller(rType, config, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, nil

	case reflect.Struct:
		if rType == xreflect.TimeType {
			return NewTimeMarshaller(tag, config), nil
		}

		if rType == rawMessageType {
			return NewRawMessageMarshaller(), nil
		}

		marshaller, err := NewStructMarshaller(config, rType, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, nil

	case reflect.Interface:
		marshaller, err := NewInterfaceMarshaller(rType, config, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, nil
	case reflect.Chan, reflect.Complex64, reflect.Invalid, reflect.Uintptr, reflect.UnsafePointer, reflect.Func:
		return nil, fmt.Errorf("unsupported type %v", rType.String())

	default:
		marshaller, err := NewPrimitiveMarshaller(rType, tag)
		if err != nil {
			return nil, err
		}

		return marshaller, nil
	}
}

func (m *Cache) pathCache(path string) *PathCache {
	value, ok := m.pathCaches.Load(path)
	if ok {
		return value.(*PathCache)
	}

	result := &PathCache{
		parent: m,
		path:   path,
		cache:  sync.Map{},
	}
	m.pathCaches.Store(path, result)
	return result
}
