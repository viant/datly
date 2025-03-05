package tabjson

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/pkg/errors"
	io2 "github.com/viant/sqlx/io"

	//io2 "github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
	"io"
	"reflect"
	"time"
)

var timeType = reflect.TypeOf(time.Now())
var timeTypePtr = reflect.PtrTo(timeType)

type (
	Marshaller struct {
		xType           *xunsafe.Type
		elemType        reflect.Type
		xSlice          *xunsafe.Slice
		fieldsPositions map[string]int
		fields          []*Field
		maxDepth        int
		uniquesFields   map[string]bool
		references      map[string][]string
		pathAccessors   map[string]*xunsafe.Field
		stringifiers    map[reflect.Type]*io2.ObjectStringifier
		config          *Config
		uniqueTypes     map[reflect.Type]bool
	}

	Field struct {
		parentType reflect.Type
		path       string
		name       string
		header     string
		holder     string

		xField      *xunsafe.Field
		unique      bool
		stringifier io2.FieldStringifierFn
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

	if config.EncloseBy == "" {
		config.EncloseBy = `"`
	}

	if config.EscapeBy == "" {
		config.EscapeBy = `\`
	}

	if config.FieldSeparator == "" {
		config.FieldSeparator = `,`
	}

	if config.ObjectSeparator == "" {
		config.ObjectSeparator = "\n"
	}

	if config.NullValue == "" {
		config.NullValue = "null"
	}

	if config.StringifierConfig.StringifierFloat32Config.Precision == "" {
		config.StringifierConfig.StringifierFloat32Config.Precision = "-1"
	}

	if config.StringifierConfig.StringifierFloat64Config.Precision == "" {
		config.StringifierConfig.StringifierFloat64Config.Precision = "-1"
	}

	excluded := map[string]bool{}
	for _, path := range config.ExcludedPaths {
		excluded[path] = true
	}

	rType = ensureSlice(rType)
	elemType := elem(rType)

	//fmt.Printf("kind %v, name %v \n", elemType.Kind(), elemType.Name())

	marshaller := &Marshaller{
		config:          config,
		elemType:        elemType, // TODO MFI: destination of rType
		fieldsPositions: map[string]int{},
		uniquesFields:   map[string]bool{},
		references:      map[string][]string{},
		pathAccessors:   map[string]*xunsafe.Field{},
		xType:           xunsafe.NewType(elemType), // TODO MFI  reflect.Type => xunsafe.Type
		uniqueTypes:     map[reflect.Type]bool{},
	}

	if err := marshaller.init(config, excluded); err != nil {
		return nil, err
	}

	return marshaller, nil
}

func ensureSlice(rType reflect.Type) reflect.Type {
	destType := rType
	if destType.Kind() == reflect.Ptr {
		destType = destType.Elem()
	}
	switch destType.Kind() {
	case reflect.Struct:
		for i := 0; i < destType.NumField(); i++ {
			field := destType.Field(i)
			fieldType := field.Type
			if fieldType.Kind() == reflect.Ptr {
				fieldType = fieldType.Elem()
			}
			if fieldType.Kind() == reflect.Slice {
				candidate := fieldType.Elem()
				if candidate.Kind() == reflect.Struct || (candidate.Kind() == reflect.Ptr && candidate.Elem().Kind() == reflect.Struct) {
					return candidate
				}
			}
		}
	}
	return rType
}

func (m *Marshaller) init(config *Config, excluded map[string]bool) error {
	m.initConfig(config)

	m.xSlice = xunsafe.NewSlice(reflect.SliceOf(m.elemType))
	m.indexByPath(m.elemType, "", excluded, "", nil)

	return nil
}

func (m *Marshaller) indexByPath(parentType reflect.Type, path string, excluded map[string]bool, holder string, parentAccessor *xunsafe.Field) {
	elemParentType := elem(parentType)
	if m.uniqueTypes[parentType] { //avoid cyclic reference
		return
	}
	m.uniqueTypes[parentType] = true

	numField := elemParentType.NumField()
	m.pathAccessors[path] = parentAccessor
	for i := 0; i < numField; i++ {
		field := elemParentType.Field(i)
		if field.PkgPath != "" {
			continue
		}

		fieldPath, fieldName := m.asKeys(path, field)
		if excluded[fieldPath] {
			continue
		}

		elemType := elem(field.Type)
		if elemType.Kind() == reflect.Struct && elemType != timeType {
			m.indexByPath(elemType, fieldPath, excluded, fieldName, xunsafe.NewField(field))
			continue
		}

		m.fieldsPositions[fieldName] = len(m.fields)
		m.fields = append(m.fields, m.newField(path, holder, field, parentType, fieldPath))
	}
}

func (m *Marshaller) asKeys(path string, field reflect.StructField) (pathKey string, positionsKey string) {
	name := field.Tag.Get(TagName)
	if name != "" {
		return m.combine(path, name), name
	}

	asFullPath := m.combine(path, field.Name)
	return asFullPath, asFullPath
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

	fields, err := m.fieldsByName(headers)
	if err != nil {
		return err
	}

	session, err := m.session(fields, dest)
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

func (m *Marshaller) newField(path string, holder string, field reflect.StructField, parentType reflect.Type, fieldPath string) *Field {
	xField := xunsafe.NewField(field)

	var stringifierConfig *io2.StringifierConfig
	if m.config != nil {
		stringifierConfig = &m.config.StringifierConfig
	}

	return &Field{
		path:        path,
		xField:      xField,
		parentType:  parentType,
		name:        field.Name,
		header:      fieldPath,
		holder:      holder,
		stringifier: io2.Stringifier(xField, false, m.config.NullValue, stringifierConfig),
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

func (m *Marshaller) session(fields []*Field, dest interface{}) (*UnmarshalSession, error) {
	s := &UnmarshalSession{
		pathIndex: map[string]int{},
		dest:      dest,
	}

	return s, s.init(fields, m.references, m.pathAccessors, m.stringifiers)
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

func (m *Marshaller) Marshal(val interface{}, options ...interface{}) ([]byte, error) {
	valueType := reflect.TypeOf(val)
	if elem(valueType) != m.elemType {
		return nil, fmt.Errorf("can't marshal %T with %v marshaller", val, m.elemType.String())
	}

	fnValueAt, size, err := io2.Values(val)
	if err != nil {
		return nil, err
	}

	options = append(options, io2.Parallel(true)) // TODO MFI ???

	session, err := m.session(m.fields, nil)
	if err != nil {
		return nil, err
	}

	configs := m.marshalOptions(options)

	buffer, err := m.marshalData(fnValueAt, size, session.parentNode, configs)

	if err != nil {
		return nil, err
	}

	return io.ReadAll(buffer)
}

func (m *Marshaller) marshalOptions(options []interface{}) []*Config {
	var depthConfigs []*Config
	for _, option := range options {
		switch actual := option.(type) {
		case []*Config:
			depthConfigs = actual
		}
	}
	return depthConfigs
}

func (m *Marshaller) marshalData(fnValueAt io2.ValueAccessor, size int, object *Object, configs []*Config) (*Buffer, error) {
	buffer := NewBuffer(1024)

	accessor, err := object.Accessor(0, m.config, 0, configs)
	if err != nil {
		return nil, err
	}

	writer := newWriter(accessor, m.config, buffer, m.xType, fnValueAt, size, "")
	writer.writeAllObjects(accessor, true)
	return buffer, nil
}

func elem(rType reflect.Type) reflect.Type {
	for {
		switch rType.Kind() {
		case reflect.Ptr, reflect.Slice:
			rType = rType.Elem()

		default:
			return rType
		}
	}
}
