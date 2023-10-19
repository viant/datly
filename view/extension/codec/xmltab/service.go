package xmltab

import (
	"fmt"
	"github.com/viant/structology/format"
	"github.com/viant/xmlify"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

const RFC3339NanoCustomized = "2006-01-02T15:04:05.000Z07:00"
const XmlTabNullValue = "true"

type (
	ColumnHeader struct {
		ID   string `json:",omitempty" xmlify:"path=@id"`
		Type string `json:",omitempty" xmlify:"path=@type"`
	}

	ColumnsWrapper struct {
		Columns []*ColumnHeader `xmlify:"name=column"`
	}

	ColumnValue struct {
		LongType   *string `json:",omitempty" xmlify:"omitempty,path=@lg"`
		IntType    *string `json:",omitempty" xmlify:"omitempty,path=@long"`
		DoubleType *string `json:",omitempty" xmlify:"omitempty,path=@db"`
		DateType   *string `json:",omitempty" xmlify:"omitempty,path=@ts"`
		Value      *string `json:",omitempty" xmlify:"omitempty,omittagname"`
		ValueAttr  *string `json:",omitempty" xmlify:"omitempty,path=@nil"`
	}

	Row struct {
		ColumnValues []*ColumnValue `xmlify:"name=c"`
	}

	RowsWrapper struct {
		Rows []*Row `xmlify:"name=r"`
	}

	Result struct {
		ColumnsWrapper ColumnsWrapper `xmlify:"name=columns"`
		RowsWrapper    RowsWrapper    `xmlify:"name=rows"`
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
		result.RowsWrapper.Rows = append(result.RowsWrapper.Rows, record)
	}
	return nil
}

func (t *Service) transferRecord(xStruct *xunsafe.Struct, sourcePtr unsafe.Pointer) (*Row, error) {
	var row Row
	var nullValue = XmlTabNullValue

	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]
		tag, err := xmlify.ParseTag(field.Tag)
		if err != nil || tag.Ignore {
			continue
		}

		value := &ColumnValue{}
		switch field.Type.Kind() {
		case reflect.String:
			s := field.String(sourcePtr)
			value.Value = &s
		case reflect.Int:
			s := strconv.Itoa(field.Int(sourcePtr))
			value.LongType = &s
		case reflect.Float64:
			s := strconv.FormatFloat(field.Float64(sourcePtr), 'f', 10, 64)
			value.DoubleType = &s
		case reflect.Float32:
			s := strconv.FormatFloat(float64(field.Float32(sourcePtr)), 'f', 10, 64)
			value.DoubleType = &s
		case reflect.Ptr:
			switch field.Type.Elem().Kind() {
			case reflect.String:
				if v := field.StringPtr(sourcePtr); v != nil {
					value.Value = v
				} else {
					value.ValueAttr = &nullValue
				}
			case reflect.Int:
				if v := field.IntPtr(sourcePtr); v != nil {
					s := strconv.Itoa(*v)
					value.LongType = &s
				} else {
					value.ValueAttr = &nullValue
				}
			case reflect.Float64:
				if v := field.Float64Ptr(sourcePtr); v != nil {
					s := strconv.FormatFloat(*v, 'f', -1, 64)
					value.DoubleType = &s
				} else {
					value.ValueAttr = &nullValue
				}
			case reflect.Float32:
				if v := field.Float32Ptr(sourcePtr); v != nil {
					s := strconv.FormatFloat(float64(*v), 'f', -1, 32)
					value.DoubleType = &s
				} else {
					value.ValueAttr = &nullValue
				}
			default:
				v := field.Value(sourcePtr)
				switch field.Type {
				case xreflect.TimePtrType:
					if ts, ok := v.(*time.Time); ok && ts != nil {
						s := ts.Format(RFC3339NanoCustomized)
						value.DateType = &s
					} else {
						value.ValueAttr = &nullValue
					}
				case xreflect.TimeType:
					if ts, ok := v.(time.Time); ok {
						s := ts.Format(RFC3339NanoCustomized)
						value.DateType = &s
					} else {
						value.ValueAttr = &nullValue
					}
				default:
					return nil, fmt.Errorf("xmltab: usnupported type: %T", v)
				}
			}
		default:
			return nil, fmt.Errorf("xmltab: usnupported kind: %v", field.Type.Kind())
		}
		row.ColumnValues = append(row.ColumnValues, value)
	}
	return &row, nil
}

func (t *Service) transferColumns(xStruct *xunsafe.Struct, result *Result) {
	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]

		tag, err := format.Parse(field.Tag)
		if err != nil || tag.Ignore {
			continue
		}

		column := &ColumnHeader{}
		if tag.Name != "" {
			column.ID = tag.Name
		} else {
			column.ID = field.Name
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
				column.Type = "timestamp"
			}
		}

		result.ColumnsWrapper.Columns = append(result.ColumnsWrapper.Columns, column)
	}
}

func New() *Service {
	return &Service{}
}
