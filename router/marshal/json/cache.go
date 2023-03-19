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

func (m *Cache) ElemMarshallerIfNeeded(rType reflect.Type, config marshal.Default, path string, outputPath string, defaultTag *DefaultTag) (Marshaler, error) {
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	return m.LoadMarshaller(rType, config, path, outputPath, defaultTag)
}

func (c *PathCache) GetMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag) (Marshaler, error) {
	value, ok := c.cache.Load(rType)
	if ok {
		return value.(Marshaler), nil
	}

	marshaler, err := c.getMarshaler(rType, config, path, outputPath, tag)
	if tag == nil {
		tag = &DefaultTag{}
	}

	if err != nil {
		return nil, err
	}

	c.StoreMarshaler(rType, marshaler)
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
		if rType.Elem().Kind() == reflect.Interface {
			return NewSliceInterfaceMarshaller(config, path, outputPath, tag, c.parent), nil
		}

		if rType == rawMessageType {
			return NewRawMessageMarshaller(), nil
		}

		marshaller, err := NewSliceMarshaller(rType, config, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, nil
	case reflect.Map:
		marshaller, err := NewMapMarshaller(rType, config, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, nil

	case reflect.Struct:
		if rType == xreflect.TimeType {
			return NewTimeMarshaller(tag, config), nil
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

	case reflect.Map:
		marshaller, err := NewMapMarshaller(rType, config, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, nil

	case reflect.Int:
		return NewIntMarshaller(tag), nil

	case reflect.Int8:
		return NewInt8Marshaller(tag), nil

	case reflect.Int16:
		return NewInt16Marshaller(tag), nil

	case reflect.Int32:
		return NewInt32Marshaller(tag), nil

	case reflect.Int64:
		return NewInt64Marshaller(tag), nil

	case reflect.Uint:
		return NewUintMarshaller(tag), nil

	case reflect.Uint8:
		return NewUint8Marshaller(tag), nil

	case reflect.Uint16:
		return NewUint16Marshaller(tag), nil

	case reflect.Uint32:
		return NewUint32Marshaller(tag), nil

	case reflect.Uint64:
		return NewUint64Marshaller(tag), nil

	case reflect.Float64:
		return NewFloat64Marshaller(tag), nil

	case reflect.Float32:
		return NewFloat32Marshaller(tag), nil

	case reflect.Bool:
		return NewBoolMarshaller(tag), nil

	case reflect.String:
		return NewStringMarshaller(tag), nil

	default:
		return nil, fmt.Errorf("unsupported type %v", rType.String())
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

func (m *Cache) getPredefinedPtrMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag, cache *Cache) (Marshaler, bool) {
	pathCache := m.pathCache(path)
	marshaller, ok := pathCache.LoadMarshaller(rType)
	if ok {
		return marshaller, true
	}

	marshaler, ok := m.isPredefinedPtrMarshaller(rType, config, tag)
	if ok {
		pathCache.StoreMarshaler(rType, marshaler)
	}

	return marshaler, ok
}

func (c *PathCache) LoadMarshaller(rType reflect.Type) (Marshaler, bool) {
	value, ok := c.cache.Load(rType)
	if ok {
		return value.(Marshaler), true
	}

	return nil, false
}

func (c *PathCache) StoreMarshaler(rType reflect.Type, marshaler Marshaler) {
	c.cache.Store(rType, marshaler)
}

func (m *Cache) isPredefinedPtrMarshaller(rType reflect.Type, config marshal.Default, tag *DefaultTag) (Marshaler, bool) {
	if rType == xreflect.TimeType {
		return NewTimePtrMarshaller(tag, config), true
	}

	switch rType.Kind() {
	case reflect.Int:
		return NewIntPtrMarshaller(tag), true

	case reflect.Int8:
		return NewInt8PtrMarshaller(tag), true

	case reflect.Int16:
		return NewInt16PtrMarshaller(tag), true

	case reflect.Int32:
		return NewInt32PtrMarshaller(tag), true

	case reflect.Int64:
		return NewInt64PtrMarshaller(tag), true

	case reflect.Uint:
		return NewUintPtrMarshaller(tag), true

	case reflect.Uint8:
		return NewUint8PtrMarshaller(tag), true

	case reflect.Uint16:
		return NewUint16PtrMarshaller(tag), true

	case reflect.Uint32:
		return NewUint32PtrMarshaller(tag), true

	case reflect.Uint64:
		return NewUint64PtrMarshaller(tag), true

	case reflect.Float32:
		return NewFloat32PtrMarshaller(tag), true

	case reflect.Float64:
		return NewFloat64PtrMarshaller(tag), true

	case reflect.String:
		return NewStringPtrMarshaller(tag), true

	case reflect.Bool:
		return NewBoolPtrMarshaller(tag), true
	}

	return nil, false
}
