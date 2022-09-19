package csv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/shared"
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
		uniquesFields   map[string]bool
		references      map[string][]string
		pathAccessors   map[string]*xunsafe.Field
	}

	Field struct {
		parentType reflect.Type
		path       string
		xField     *xunsafe.Field
		depth      int
		unique     bool
		name       string
	}

	Config struct {
		UniqueFields    []string
		References      []*Reference // parent -> children. Foo.ID -> Boo.FooId
		FieldSeparator  string
		ObjectSeparator string
	}

	Reference struct {
		ParentField string
		ChildField  string
	}
)

func NewMarshaller(rType reflect.Type, config *Config) (*Marshaller, error) {
	if config == nil {
		config = &Config{}
	}

	marshaller := &Marshaller{
		elemType:        rType,
		fieldsPositions: map[string]int{},
		uniquesFields:   map[string]bool{},
		references:      map[string][]string{},
		pathAccessors:   map[string]*xunsafe.Field{},
	}

	if err := marshaller.init(config); err != nil {
		return nil, err
	}

	return marshaller, nil
}

func (m *Marshaller) init(config *Config) error {
	m.initConfig(config)

	m.xSlice = xunsafe.NewSlice(reflect.SliceOf(m.elemType))
	m.indexByPath(m.elemType, "", 0, nil)

	return nil
}

func (m *Marshaller) indexByPath(parentType reflect.Type, path string, depth int, parentAccessor *xunsafe.Field) {
	numField := parentType.NumField()
	m.pathAccessors[path] = parentAccessor
	for i := 0; i < numField; i++ {
		field := parentType.Field(i)
		fieldPath := m.fieldPositionKey(path, field)

		elemType := shared.Elem(field.Type)
		if elemType.Kind() == reflect.Struct {
			m.indexByPath(elemType, fieldPath, depth+1, xunsafe.NewField(field))
			continue
		}

		m.fieldsPositions[fieldPath] = len(m.fields)
		m.fields = append(m.fields, m.newField(path, field, depth, parentType))
	}
}

func (m *Marshaller) fieldPositionKey(path string, field reflect.StructField) string {
	name := field.Tag.Get(TagName)
	if name != "" {
		return name
	}

	return m.combine(path, field.Name)
}

func (m *Marshaller) combine(path, name string) string {
	if path == "" {
		return name
	}

	return path + "." + name
}

func (m *Marshaller) Unmarshal(b []byte, dest interface{}) error {
	reader := csv.NewReader(bytes.NewReader(b))
	headers, err := reader.Read()
	if err != nil {
		return m.asReadError(err)
	}

	session, fields, err := m.session(headers, dest)
	if err != nil {
		return err
	}

	for {
		record, err := reader.Read()
		if err != nil {
			return m.asReadError(err)
		}

		if len(record) != len(fields) {
			return fmt.Errorf("record header and the record are differ in length. Fields len: %v, Record len: %v", len(fields), len(record))
		}

		if err = session.addRecord(record); err != nil {
			return err
		}
	}
}

func (m *Marshaller) newField(path string, field reflect.StructField, depth int, parentType reflect.Type) *Field {
	return &Field{
		path:       path,
		xField:     xunsafe.NewField(field),
		depth:      depth,
		parentType: parentType,
		name:       field.Name,
	}
}

func (m *Marshaller) asReadError(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

func (m *Marshaller) initConfig(config *Config) {
	for i := range config.UniqueFields {
		m.uniquesFields[config.UniqueFields[i]] = true
	}

	for _, reference := range config.References {
		m.references[reference.ParentField] = append(m.references[reference.ParentField], reference.ChildField)
	}
}

func (m *Marshaller) session(headers []string, dest interface{}) (*Session, []*Field, error) {
	fields, err := m.fieldsByName(headers)
	if err != nil {
		return nil, nil, err
	}

	s := &Session{
		pathIndex: map[string]int{},
		dest:      dest,
	}

	return s, fields, s.init(fields, m.references, m.pathAccessors)
}

func (m *Marshaller) fieldsByName(names []string) ([]*Field, error) {
	fields := make([]*Field, 0, len(names))
	for _, header := range names {
		index, ok := m.fieldsPositions[header]
		if !ok {
			return nil, fmt.Errorf("not found field %v", header)
		}

		fields = append(fields, m.fields[index])
	}
	return fields, nil
}

func (m *Marshaller) ReadHeaders(b []byte) ([]string, error) {
	reader := csv.NewReader(bytes.NewReader(b))
	headers, err := reader.Read()
	if err != nil {
		return nil, m.asReadError(err)
	}

	fields, err := m.fieldsByName(headers)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(fields))
	for _, field := range fields {
		result = append(result, m.combine(field.path, field.name))
	}

	return result, nil
}
