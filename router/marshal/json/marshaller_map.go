package json

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal/common"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type mapMarshaller struct {
	discoveredMarshaller func(ptr unsafe.Pointer, sb *MarshallSession) error
	keyMarshaller        marshaler
	valueMarshaller      marshaler
	isEmbedded           bool
	cache                *marshallersCache
	config               common.IOConfig
	xType                *xunsafe.Type
	valueType            reflect.Type
	keyType              reflect.Type
}

func newMapMarshaller(rType reflect.Type, config common.IOConfig, path string, outputPath string, tag *DefaultTag, cache *marshallersCache) (*mapMarshaller, error) {
	result := &mapMarshaller{
		xType:      getXType(rType),
		isEmbedded: tag.Embedded,
		cache:      cache,
		config:     config,
		valueType:  rType.Elem(),
		keyType:    rType.Key(),
	}

	valueMarshaller, err := cache.loadMarshaller(rType.Elem(), config, path, outputPath, tag)
	if err != nil {
		return nil, err
	}

	result.valueMarshaller = valueMarshaller
	if rType == mapStringIfaceType {
		result.discoveredMarshaller = result.mapStringIfaceMarshaller()
	} else {
		keyMarshaller, err := cache.loadMarshaller(rType.Key(), config, path, outputPath, tag)
		if err != nil {
			return nil, err
		}
		result.keyMarshaller = keyMarshaller
	}

	return result, nil
}

func (m *mapMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	aMap := reflect.New(m.xType.Type()).Elem().Interface()

	var unMarshaller gojay.UnmarshalerJSONObject

	switch aMap.(type) {
	case map[string]int:
		*(*map[string]int)(pointer) = make(map[string]int)
		actual := *(*map[string]int)(pointer)
		unMarshaller = &mapStringIntUnmarshaler{actual}
	case map[string]float64:
		*(*map[string]float64)(pointer) = make(map[string]float64)
		actual := *(*map[string]float64)(pointer)

		unMarshaller = &mapStringFloatUnmarshaler{actual}
	case map[string]string:
		*(*map[string]string)(pointer) = make(map[string]string)
		actual := *(*map[string]string)(pointer)
		unMarshaller = &mapStringStringUnmarshaler{actual}
	}

	if unMarshaller != nil {
		return decoder.AddObject(unMarshaller)
	}
	return fmt.Errorf("unsupported unmarshall to map type, yet")
}

type mapStringIntUnmarshaler struct {
	aMap map[string]int
}

func (m *mapStringIntUnmarshaler) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	value := 0
	err := dec.Int(&value)
	if err != nil {
		return err
	}
	m.aMap[key] = value
	return nil
}

func (m *mapStringIntUnmarshaler) NKeys() int {
	return len(m.aMap)
}

type mapStringFloatUnmarshaler struct {
	aMap map[string]float64
}

func (m *mapStringFloatUnmarshaler) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	value := 0.0
	err := dec.Float64(&value)
	if err != nil {
		return err
	}
	m.aMap[key] = value
	return nil
}

func (m *mapStringFloatUnmarshaler) NKeys() int {
	return len(m.aMap)
}

type mapStringStringUnmarshaler struct {
	aMap map[string]string
}

func (m *mapStringStringUnmarshaler) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	value := ""
	err := dec.String(&value)
	if err != nil {
		return err
	}
	m.aMap[key] = value
	return nil
}

func (m mapStringStringUnmarshaler) NKeys() int {
	return len(m.aMap)
}

func (m *mapMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	if m.discoveredMarshaller != nil {
		return m.discoveredMarshaller(ptr, sb)
	}

	aMap := reflect.ValueOf(asInterface(m.xType, ptr))
	if aMap.IsNil() {
		return nil
	}

	if !m.isEmbedded {
		sb.WriteString("{")
	}

	counter := 0
	iterator := aMap.MapRange()

	for iterator.Next() {
		if counter > 0 {
			sb.WriteString(",")
		}
		counter++

		aKey := iterator.Key()
		keyIface := aKey.Interface()
		if err := m.keyMarshaller.MarshallObject(AsPtr(keyIface, m.keyType), sb); err != nil {
			return err
		}

		sb.WriteString(":")
		value := iterator.Value()
		valueIface := value.Interface()
		if err := m.valueMarshaller.MarshallObject(AsPtr(valueIface, m.valueType), sb); err != nil {
			return err
		}
	}

	if !m.isEmbedded {
		sb.WriteString("}")
	}

	return nil
}

func (m *mapMarshaller) mapStringIfaceMarshaller() func(pointer unsafe.Pointer, sb *MarshallSession) error {
	return func(pointer unsafe.Pointer, sb *MarshallSession) error {
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
			sb.WriteString(namesIndex.formatTo(aKey, m.config.CaseFormat))
			sb.WriteString(`":`)

			if err := m.valueMarshaller.MarshallObject(AsPtr(aValue, m.valueType), sb); err != nil {
				return err
			}
		}

		if !m.isEmbedded {
			sb.WriteString("}")
		}

		return nil
	}
}
