package json

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

var mapStringIfaceType = reflect.TypeOf(map[string]interface{}{})

type MapMarshaller struct {
	discoveredMarshaller func(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error
	keyMarshaller        Marshaler
	valueMarshaller      Marshaler
	isEmbedded           bool
	cache                *Cache
	config               marshal.Default
	xType                *xunsafe.Type
}

func NewMapMarshaller(rType reflect.Type, config marshal.Default, path string, outputPath string, tag *DefaultTag, cache *Cache) (*MapMarshaller, error) {
	result := &MapMarshaller{
		xType:      GetXType(rType),
		isEmbedded: tag.Embedded,
		cache:      cache,
		config:     config,
	}

	valueMarshaller, err := cache.LoadMarshaller(rType.Elem(), config, path, outputPath, tag)
	if err != nil {
		return nil, err
	}

	result.valueMarshaller = valueMarshaller
	if rType == mapStringIfaceType {
		result.discoveredMarshaller = result.mapStringIfaceMarshaller()
	} else {
		keyMarshaller, err := cache.LoadMarshaller(rType.Key(), config, path, outputPath, tag)
		if err != nil {
			return nil, err
		}
		result.keyMarshaller = keyMarshaller
	}

	return result, nil
}

func (m *MapMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	return fmt.Errorf("unsupported unmarshall to map type, yet")
}

func (m *MapMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	if m.discoveredMarshaller != nil {
		return m.discoveredMarshaller(rType, ptr, sb)
	}

	aMap := reflect.ValueOf(Interface(m.xType, ptr))
	if aMap.IsNil() {
		return nil
	}

	if !m.isEmbedded {
		sb.WriteString("{")
	}

	counter := 0
	iterator := aMap.MapRange()
	if !m.isEmbedded {
		sb.WriteString("{")
	}

	for iterator.Next() {
		if counter > 0 {
			sb.WriteString(",")
		}
		counter++

		aKey := iterator.Key()
		keyIface := aKey.Interface()
		if err := m.keyMarshaller.MarshallObject(aKey.Type(), xunsafe.AsPointer(keyIface), sb); err != nil {
			return err
		}

		sb.WriteString(":")
		value := iterator.Value()
		valueIface := value.Interface()
		if err := m.valueMarshaller.MarshallObject(value.Type(), xunsafe.AsPointer(valueIface), sb); err != nil {
			return err
		}
	}

	if !m.isEmbedded {
		sb.WriteString("}")
	}

	return nil
}

func (m *MapMarshaller) mapStringIfaceMarshaller() func(r reflect.Type, pointer unsafe.Pointer, sb *Session) error {
	return func(r reflect.Type, pointer unsafe.Pointer, sb *Session) error {
		mapPtr := (*map[string]interface{})(pointer)
		if mapPtr == nil {
			return nil
		}

		if !m.isEmbedded {
			sb.WriteString("{")
		}

		aMap := *mapPtr
		counter := 0
		for aKey, aValue := range aMap {
			if counter > 0 {
				sb.WriteString(",")
			}
			counter++
			sb.WriteString(`"`)
			sb.WriteString(namesCaseIndex.FormatTo(aKey, m.config.CaseFormat))
			sb.WriteString(`":`)
			if err := m.valueMarshaller.MarshallObject(reflect.TypeOf(aValue), xunsafe.AsPointer(aValue), sb); err != nil {
				return err
			}
		}

		if !m.isEmbedded {
			sb.WriteString("}")
		}

		return nil
	}
}