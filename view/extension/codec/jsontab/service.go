package jsontab

import (
	"fmt"
	"github.com/viant/tagly/format"
	"github.com/viant/xdatly/handler/response/tabular/tjson"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

type Service struct{}

func (t *Service) Transfer(aSlice interface{}) (*tjson.Tabular, error) {
	sliceType := reflect.TypeOf(aSlice)
	if sliceType.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected slice but had: %T", aSlice)
	}
	xSlice := xunsafe.NewSlice(sliceType)
	componentType := xSlice.Type.Elem()
	if componentType.Kind() == reflect.Ptr {
		componentType = componentType.Elem()
	}
	ptr := xunsafe.AsPointer(aSlice)
	sliceLen := xSlice.Len(ptr)
	xStruct := xunsafe.NewStruct(componentType)
	var result = &tjson.Tabular{}
	t.transferColumns(xStruct, result)
	err := t.transferRecords(sliceLen, xSlice, ptr, xStruct, result)
	return result, err
}

func (t *Service) transferRecords(sliceLen int, xSlice *xunsafe.Slice, ptr unsafe.Pointer, xStruct *xunsafe.Struct, result *tjson.Tabular) error {
	for i := 0; i < sliceLen; i++ {
		source := xSlice.ValuePointerAt(ptr, i)
		sourcePtr := xunsafe.AsPointer(source)
		record, err := t.transferRecord(xStruct, sourcePtr)
		if err != nil {
			return err
		}
		result.Records = append(result.Records, record)
	}
	return nil
}

func (t *Service) transferRecord(xStruct *xunsafe.Struct, sourcePtr unsafe.Pointer) (tjson.Record, error) {
	var record tjson.Record
	var timeLayout = time.RFC3339

	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]
		tag, err := format.Parse(field.Tag, "json")
		if err != nil {
			return nil, err
		}
		if tag.Ignore {
			continue
		}

		if tag.TimeLayout != "" {
			timeLayout = tag.TimeLayout
		}

		value := ""
		switch field.Type.Kind() {
		case reflect.String:
			value = field.String(sourcePtr)
		case reflect.Int:
			value = strconv.Itoa(field.Int(sourcePtr))
		case reflect.Float64:
			value = strconv.FormatFloat(field.Float64(sourcePtr), 'f', 10, 64)
		case reflect.Float32:
			value = strconv.FormatFloat(float64(field.Float32(sourcePtr)), 'f', 10, 64)
		case reflect.Ptr:
			switch field.Type.Elem().Kind() {
			case reflect.String:
				if v := field.StringPtr(sourcePtr); v != nil {
					value = *v
				}
			case reflect.Int:
				if v := field.IntPtr(sourcePtr); v != nil {
					value = strconv.Itoa(*v)
				}
			case reflect.Float64:
				if v := field.Float64Ptr(sourcePtr); v != nil {
					value = strconv.FormatFloat(*v, 'f', -1, 64)
				}
			case reflect.Float32:
				if v := field.Float32Ptr(sourcePtr); v != nil {
					value = strconv.FormatFloat(float64(*v), 'f', -1, 32)
				}
			default:

				v := field.Value(sourcePtr)
				switch field.Type {
				case xreflect.TimePtrType:
					if ts, ok := v.(*time.Time); ok && ts != nil {
						value = ts.Format(timeLayout)
					}
				case xreflect.TimeType:
					if ts, ok := v.(time.Time); ok {
						value = ts.Format(timeLayout)
					}
				default:
					if field.Type.Elem().Kind() == reflect.Struct {
						//TODO: handle nested struct
						continue
					}
					return nil, fmt.Errorf("jsontab: usnupported type: %T", v)
				}
			}
		case reflect.Slice:
			switch field.Type.Elem().Kind() {
			case reflect.Ptr:
				if field.Type.Elem().Elem().Kind() == reflect.Struct {
					continue //TODO: handle nested struct
				}
			}
		default:

			v := field.Value(sourcePtr)
			switch field.Type {
			case xreflect.TimePtrType:
				if ts, ok := v.(*time.Time); ok && ts != nil {
					value = ts.Format(timeLayout)
				}
			case xreflect.TimeType:
				if ts, ok := v.(time.Time); ok {
					value = ts.Format(timeLayout)
				}
			default:
				return nil, fmt.Errorf("jsontab: usnupported type: %T", v)
			}
		}
		record = append(record, tjson.Value(value))
	}
	return record, nil
}

func (t *Service) transferColumns(xStruct *xunsafe.Struct, result *tjson.Tabular) {
	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]

		tag, err := format.Parse(field.Tag, "json")
		if err != nil || tag.Ignore {
			continue
		}

		column := &tjson.Column{}
		if tag.Name != "" {
			column.Name = tag.Name
		} else {
			column.Name = field.Name
		}

		fieldKind := field.Kind()
		if fieldKind == reflect.Ptr {
			fieldKind = field.Type.Elem().Kind()
		}
		switch fieldKind {
		case reflect.String:
			column.Type = "string"
		case reflect.Int:
			column.Type = "long"
		case reflect.Float64, reflect.Float32:
			column.Type = "double"
		case reflect.Slice:
			continue //TODO: handle slice
		default:
			switch field.Type {
			case xreflect.TimeType, xreflect.TimePtrType:
				column.Type = "timestamp"

			}
			if field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Ptr {
				continue //TODO: handle nested struct
			}

		}
		result.Columns = append(result.Columns, column)
	}
}

func New() *Service {
	return &Service{}
}
