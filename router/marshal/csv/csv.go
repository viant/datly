package csv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/xunsafe"
	"io"
	"reflect"
)

type (
	Marshaller struct {
		elemType        reflect.Type
		xSlice          *xunsafe.Slice
		fieldsPositions map[string]int
		fields          []*Field
		maxDepth        int
	}

	Field struct {
		parentType reflect.Type
		path       string
		xField     *xunsafe.Field
		depth      int
	}
)

func NewMarshaller(rType reflect.Type) (*Marshaller, error) {
	marshaller := &Marshaller{
		elemType:        rType,
		fieldsPositions: map[string]int{},
	}

	if err := marshaller.init(); err != nil {
		return nil, err
	}

	return marshaller, nil
}

func (m *Marshaller) init() error {
	m.xSlice = xunsafe.NewSlice(reflect.SliceOf(m.elemType))
	m.indexByPath(m.elemType, "", 0)

	return nil
}

func (m *Marshaller) indexByPath(parentType reflect.Type, path string, depth int) {
	numField := parentType.NumField()
	for i := 0; i < numField; i++ {
		field := parentType.Field(i)
		m.fields = append(m.fields, m.newField(path, field, depth, parentType))
		m.fieldsPositions[field.Name] = i
	}
}

func (m *Marshaller) Unmarshal(b []byte, dest interface{}) error {
	reader := csv.NewReader(bytes.NewReader(b))
	headers, err := reader.Read()
	if err != nil {
		return m.asReadError(err)
	}

	fields, err := m.headerFields(headers)
	if err != nil {
		return err
	}

	//anIndex :=

	for {
		record, err := reader.Read()
		if err != nil {
			return m.asReadError(err)
		}

		if len(record) != len(fields) {
			return fmt.Errorf("record header and the record are differ in length. Fields len: %v, Record len: %v", len(fields), len(record))
		}

	}
}

func (m *Marshaller) headerFields(headers []string) ([]*Field, error) {
	fieldIndexes := make([]*Field, 0, len(headers))
	for _, header := range headers {
		index, ok := m.fieldsPositions[header]
		if !ok {
			return nil, fmt.Errorf("not found field %v", header)
		}

		fieldIndexes = append(fieldIndexes, m.fields[index])
	}

	return fieldIndexes, nil
}

func (m *Marshaller) newField(path string, field reflect.StructField, depth int, parentType reflect.Type) *Field {
	return &Field{
		path:       path,
		xField:     xunsafe.NewField(field),
		depth:      depth,
		parentType: parentType,
	}
}

func (m *Marshaller) asReadError(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}
