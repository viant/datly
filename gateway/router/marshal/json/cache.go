package json

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/structology/format"
	"github.com/viant/structology/format/text"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
)

var buffersPool *buffers
var types *typesPool
var namesIndex *namesCaseIndex

type (
	namesCaseIndex struct {
		mux      sync.Mutex
		registry map[text.CaseFormat]map[string]string
	}

	marshallersCache struct {
		pathCaches sync.Map // path -> pathCache
	}

	pathCache struct {
		parent *marshallersCache
		path   string
		cache  sync.Map // rType -> Marshaler
	}

	typesPool struct {
		xtypesMap sync.Map
	}
)

func newCache() *marshallersCache {
	return &marshallersCache{pathCaches: sync.Map{}}
}

func (n *namesCaseIndex) formatTo(value string, dstFormat text.CaseFormat) string {
	n.mux.Lock()
	defer n.mux.Unlock()

	registry, ok := n.registry[dstFormat]
	if !ok {
		registry = map[string]string{}
		n.registry[dstFormat] = registry
	}

	formated, ok := registry[value]
	if !ok {
		srcFormat := text.DetectCaseFormat(value)
		if srcFormat.IsDefined() {
			formated = srcFormat.Format(value, dstFormat)
		} else {
			formated = value
		}
		registry[value] = formated
	}

	return formated
}

type buffers struct {
	pool *sync.Pool
}

func (p *buffers) get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

func (p *buffers) put(buffer *bytes.Buffer) {
	buffer.Reset()
	p.pool.Put(buffer)
}

func getXType(rType reflect.Type) *xunsafe.Type {
	load, ok := types.xtypesMap.Load(rType)
	if ok {
		return load.(*xunsafe.Type)
	}

	xType := xunsafe.NewType(rType)
	types.xtypesMap.Store(rType, xType)
	return xType
}

func (m *marshallersCache) loadMarshaller(rType reflect.Type, config *config.IOConfig, path string, outputPath string, formatTag *format.Tag, options ...interface{}) (marshaler, error) {
	aCache := m.pathCache(path)
	marshaller, err := aCache.loadOrGetMarshaller(rType, config, path, outputPath, formatTag, options...)
	if err != nil {
		return nil, err
	}

	return marshaller, nil
}

func (c *pathCache) loadOrGetMarshaller(rType reflect.Type, config *config.IOConfig, path string, outputPath string, tag *format.Tag, options ...interface{}) (marshaler, error) {
	value, ok := c.cache.Load(rType)
	if ok {
		return value.(marshaler), nil
	}

	aMarshaler, err := c.getMarshaller(rType, config, path, outputPath, tag, options...)

	if err != nil {
		return nil, err
	}

	c.storeMarshaler(rType, aMarshaler)
	return aMarshaler, nil
}

func (c *pathCache) getMarshaller(rType reflect.Type, config *config.IOConfig, path string, outputPath string, tag *format.Tag, options ...interface{}) (marshaler, error) {
	if tag == nil {
		tag = &format.Tag{}
	}

	aConfig := c.parseConfig(options)
	if (aConfig == nil || !aConfig.ignoreCustomUnmarshaller) && rType.Implements(unmarshallerIntoType) {
		return newCustomUnmarshaller(rType, config, path, outputPath, tag, c.parent)
	}

	switch rType {
	case xreflect.TimePtrType:
		return newTimePtrMarshaller(tag, config), nil
	case rawMessageType:
		return newRawMessageMarshaller(), nil
	}

	switch rType.Kind() {
	case reflect.Ptr:
		switch rType.Elem().Kind() {
		case reflect.Int:
			return newIntPtrMarshaller(tag), nil

		case reflect.Int8:
			return newInt8PtrMarshaller(tag), nil

		case reflect.Int16:
			return newInt16PtrMarshaller(tag), nil

		case reflect.Int32:
			return newInt32PtrMarshaller(tag), nil

		case reflect.Int64:
			return newInt64PtrMarshaller(tag), nil

		case reflect.Uint:
			return newUintPtrMarshaller(tag), nil

		case reflect.Uint8:
			return newUint8PtrMarshaller(tag), nil

		case reflect.Uint16:
			return newUint16PtrMarshaller(tag), nil

		case reflect.Uint32:
			return newUint32PtrMarshaller(tag), nil

		case reflect.Uint64:
			return newUint64PtrMarshaller(tag), nil

		case reflect.Float32:
			return newFloat32PtrMarshaller(tag), nil

		case reflect.Float64:
			return newFloat64PtrMarshaller(tag), nil

		case reflect.String:
			return newStringPtrMarshaller(tag), nil

		case reflect.Bool:
			return newBoolPtrMarshaller(tag), nil
		}

		marshaller, err := newPtrMarshaller(rType, config, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}
		return marshaller, err

	case reflect.Slice:
		if rType.Elem().Kind() == reflect.Interface {
			return newSliceInterfaceMarshaller(config, path, outputPath, tag, c.parent), nil
		}

		marshaller, err := newSliceMarshaller(rType, config, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, nil

	case reflect.Struct:
		if rType == xreflect.TimeType {
			return newTimeMarshaller(tag, config), nil
		}

		marshaller, err := newStructMarshaller(config, rType, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, nil

	case reflect.Interface:
		marshaller, err := newInterfaceMarshaller(rType, config, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, nil

	case reflect.Map:
		marshaller, err := newMapMarshaller(rType, config, path, outputPath, tag, c.parent)
		if err != nil {
			return nil, err
		}

		return marshaller, nil

	case reflect.Int:
		return newIntMarshaller(tag), nil

	case reflect.Int8:
		return NewInt8Marshaller(tag), nil

	case reflect.Int16:
		return newInt16Marshaller(tag), nil

	case reflect.Int32:
		return newInt32Marshaller(tag), nil

	case reflect.Int64:
		return newInt64Marshaller(tag), nil

	case reflect.Uint:
		return newUintMarshaller(tag), nil

	case reflect.Uint8:
		return newUint8Marshaller(tag), nil

	case reflect.Uint16:
		return newUint16Marshaller(tag), nil

	case reflect.Uint32:
		return newUint32Marshaller(tag), nil

	case reflect.Uint64:
		return newUint64Marshaller(tag), nil

	case reflect.Float64:
		return newFloat64Marshaller(tag), nil

	case reflect.Float32:
		return newFloat32Marshaller(tag), nil

	case reflect.Bool:
		return newBoolMarshaller(tag), nil

	case reflect.String:
		return newStringMarshaller(tag), nil

	default:
		return nil, fmt.Errorf("#unsupported type %v", rType.String())
	}
}

func (m *marshallersCache) pathCache(path string) *pathCache {
	value, ok := m.pathCaches.Load(path)
	if ok {
		return value.(*pathCache)
	}

	result := &pathCache{
		parent: m,
		path:   path,
		cache:  sync.Map{},
	}
	m.pathCaches.Store(path, result)
	return result
}

func (c *pathCache) loadMarshaller(rType reflect.Type) (marshaler, bool) {
	value, ok := c.cache.Load(rType)
	if ok {
		return value.(marshaler), true
	}

	return nil, false
}

func (c *pathCache) storeMarshaler(rType reflect.Type, marshaler marshaler) {
	c.cache.Store(rType, marshaler)
}

func (c *pathCache) parseConfig(options []interface{}) *cacheConfig {
	var aConfig *cacheConfig

	for _, option := range options {
		switch actual := option.(type) {
		case *cacheConfig:
			aConfig = actual
		}
	}

	return aConfig
}
