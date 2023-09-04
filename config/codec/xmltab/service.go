package xmltab

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"unsafe"
)

type (
	Column struct {
		ID   string
		Type string
	}

	Value struct {
		LongType   string
		DoubleType string
		DateType   string
		Value      string
	}

	Record []*Value

	Records []Record

	Columns []*Column

	Result struct {
		Columns Columns
		Records Records
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
	t.transferRecords(sliceLen, xSlice, ptr, xStruct, result)
	return result, nil
}

func (t *Service) transferRecords(sliceLen int, xSlice *xunsafe.Slice, ptr unsafe.Pointer, xStruct *xunsafe.Struct, result *Result) {
	for i := 0; i < sliceLen; i++ {
		source := xSlice.ValuePointerAt(ptr, i)
		sourcePtr := xunsafe.AsPointer(source)
		record := t.transferRecord(xStruct, sourcePtr)
		result.Records = append(result.Records, record)
	}
}

func (t *Service) transferRecord(xStruct *xunsafe.Struct, sourcePtr unsafe.Pointer) Record {
	var record Record
	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]
		value := &Value{}
		switch field.Kind() {
		case reflect.String:
			value.Value = field.String(sourcePtr)
		case reflect.Int:
			value.LongType = strconv.Itoa(field.Int(sourcePtr))
		case reflect.Float64:
			value.DoubleType = strconv.FormatFloat(field.Float64(sourcePtr), 'f', 10, 64)
		case reflect.Float32:
			value.DoubleType = strconv.FormatFloat(float64(field.Float32(sourcePtr)), 'f', 10, 64)
		default:
			//	fieldValue := field.Value(sourcePtr)
			//		if field.Type == //check time and *time.Time
		}
		record = append(record, value)
	}
	return record
}

func (t *Service) transferColumns(xStruct *xunsafe.Struct, result *Result) {
	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]
		column := &Column{ID: field.Name}
		switch field.Kind() {
		case reflect.String:
			column.Type = "string"
		case reflect.Int:
			column.Type = "long"
		case reflect.Float64, reflect.Float32:
			column.Type = "double"
		default:
			//		if field.Type == //check time and *time.Time
		}
		result.Columns = append(result.Columns, column)
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
