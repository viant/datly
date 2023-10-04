package jsontab

import (
	"fmt"
	"github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

type (
	Column struct {
		Name string `json:",omitempty"`
		Type string `json:",omitempty"`
	}

	Value string

	Record []Value

	Records []Record

	Columns []*Column

	Result struct {
		Columns Columns `json:",omitempty"`
		Records Records `json:",omitempty"`
	}
)

type Service struct{}

func (t *Service) Transfer(aSlice interface{}) (*Result, error) {
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
	var result = &Result{}
	t.transferColumns(xStruct, result)
	err := t.transferRecords(sliceLen, xSlice, ptr, xStruct, result)
	return result, err
}

func (t *Service) transferRecords(sliceLen int, xSlice *xunsafe.Slice, ptr unsafe.Pointer, xStruct *xunsafe.Struct, result *Result) error {
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

func (t *Service) transferRecord(xStruct *xunsafe.Struct, sourcePtr unsafe.Pointer) (Record, error) {
	var record Record
	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]
		tag := json.Parse(field.Tag.Get("json"))
		if tag.Transient {
			continue
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
			}
		default:
			v := field.Value(sourcePtr)
			switch field.Type {
			case xreflect.TimePtrType:
				if ts, ok := v.(*time.Time); ok && ts != nil {
					value = ts.Format(time.RFC3339)
				}
			case xreflect.TimeType:
				if ts, ok := v.(time.Time); ok {
					value = ts.Format(time.RFC3339)
				}
			default:
				return nil, fmt.Errorf("jsontab: usnupported type: %T", v)
			}
		}
		record = append(record, Value(value))
	}
	return record, nil
}

func (t *Service) transferColumns(xStruct *xunsafe.Struct, result *Result) {
	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]

		tag := json.Parse(field.Tag.Get("json"))
		if tag.Transient {
			continue
		}

		column := &Column{}
		if tag.FieldName != "" {
			column.Name = tag.FieldName
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
		default:
			switch field.Type {
			case xreflect.TimeType, xreflect.TimePtrType:
				column.Type = "date"
			}
		}
		result.Columns = append(result.Columns, column)
	}
}

func New() *Service {
	return &Service{}
}
