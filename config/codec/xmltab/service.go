package xmltab

import (
	"fmt"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

type ( // 03

	ColumnHeader struct {
		ID   string `json:",omitempty" xmlify:"path=@id"`
		Type string `json:",omitempty" xmlify:"path=@type"`
	}

	ColumnsWrapper struct {
		Columns []*ColumnHeader `xmlify:"name=column"`
	}

	// TODO add ptr *
	ColumnValue struct {
		LongType   string  `json:",omitempty" xmlify:"omitempty,path=@lg"`
		IntType    int     `json:",omitempty" xmlify:"omitempty,path=@long"`
		DoubleType string  `json:",omitempty" xmlify:"omitempty,path=@double"`
		DateType   string  `json:",omitempty" xmlify:"omitempty"`
		Value      *string `json:",omitempty" xmlify:"omitempty,omittagname"` //TODO change to *string
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
	if sliceLen == 0 {
		return nil, nil
	}
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
	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]
		value := &ColumnValue{}
		switch field.Type.Kind() {
		case reflect.String:
			s := field.String(sourcePtr) //TODO check
			value.Value = &s
		case reflect.Int:
			value.LongType = strconv.Itoa(field.Int(sourcePtr))
		case reflect.Float64:
			value.DoubleType = strconv.FormatFloat(field.Float64(sourcePtr), 'f', 10, 64)
		case reflect.Float32:
			value.DoubleType = strconv.FormatFloat(float64(field.Float32(sourcePtr)), 'f', 10, 64)
		case reflect.Ptr:
			switch field.Type.Elem().Kind() {
			case reflect.String:
				if v := field.StringPtr(sourcePtr); v != nil {
					value.Value = v
				}
			case reflect.Int:
				if v := field.IntPtr(sourcePtr); v != nil {
					value.LongType = strconv.Itoa(*v)
				}
			case reflect.Float64:
				if v := field.Float64Ptr(sourcePtr); v != nil {
					value.DoubleType = strconv.FormatFloat(*v, 'f', -1, 64)
				}
			case reflect.Float32:
				if v := field.Float32Ptr(sourcePtr); v != nil {
					value.DoubleType = strconv.FormatFloat(float64(*v), 'f', -1, 32)
				}
			}
		default:
			v := field.Value(sourcePtr)
			switch field.Type {
			case xreflect.TimePtrType:
				if ts, ok := v.(*time.Time); ok && ts != nil {
					value.DateType = ts.Format(time.RFC3339)
				}
			case xreflect.TimeType:
				if ts, ok := v.(time.Time); ok {
					value.DateType = ts.Format(time.RFC3339)
				}
			default:
				return nil, fmt.Errorf("xmltab: usnupported type: %T", v)
			}
		}
		row.ColumnValues = append(row.ColumnValues, value)
	}
	return &row, nil
}

func (t *Service) transferColumns(xStruct *xunsafe.Struct, result *Result) {
	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]
		column := &ColumnHeader{ID: field.Name}
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

		// TODO columns Wrapper?
		result.ColumnsWrapper.Columns = append(result.ColumnsWrapper.Columns, column)
	}
}

func New() *Service {
	return &Service{}
}

/*
<result>
        <columns>
            <column id="Id" type="long"/>
            <column id="Name" type="string"/>
        </columns>
        <rows>
            <r>
                <c lg="1"/>
                <c>name 1</c>
            </r>
            <r>
                <c lg="2"/>
                <c>name 2</c>
            </r>
        </rows>
    </result>
*/
