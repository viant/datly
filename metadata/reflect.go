package metadata

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/viant/datly/metadata/tag"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

//DiscoverColumns returns columns from the struct
func DiscoverColumns(aType reflect.Type, viewCase format.Case) (Columns, error) {
	var result Columns
	if aType.Kind() == reflect.Ptr {
		aType = aType.Elem()
	}
	if aType.Kind() == reflect.Slice {
		aType = aType.Elem()
	}
	if aType.Kind() == reflect.Ptr {
		aType = aType.Elem()
	}
	if aType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, but had: %v", aType.Name())
	}
	for i := 0; i < aType.NumField(); i++ {
		field := aType.Field(i)
		if isExported := field.PkgPath == ""; !isExported {
			continue
		}
		fieldType := field.Type
		if !shared.IsBaseType(fieldType) {
			continue
		}
		fieldName := format.CaseUpperCamel.Format(field.Name, viewCase)
		aTag := tag.Parse(field.Tag.Get(tag.SQLxTag))
		if aTag.Transient {
			continue
		}
		columnName := fieldName
		if names := aTag.Column; names != "" {
			columns := strings.Split(names, "|")
			columnName = columns[0]
		}
		result = append(result, Column{
			Name:       columnName,
			DataType:   field.Type.Name(),
			FieldIndex: i,
		})
	}
	return result, nil
}

func KeyFunc(fields []*xunsafe.Field) func(o interface{}) interface{} {
	if len(fields) == 1 {
		return func(o interface{}) interface{} {
			ptr := xunsafe.Addr(o)
			return fields[0].Value(ptr)
		}
	}
	return func(instance interface{}) interface{} {
		key := new(bytes.Buffer)
		addr := unsafe.Pointer(reflect.ValueOf(instance).Elem().UnsafeAddr())
		for _, field := range fields {
			val := field.Value(addr)
			switch actual := val.(type) {
			case string:
				key.WriteString(actual)
			default:
				if err := binary.Write(key, binary.LittleEndian, val); err != nil {
					key.WriteString(fmt.Sprintf("%v", actual))
				}
			}
			key.WriteString("/")
		}
		return key.String()
	}
}
