package state

import (
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/structology/format/text"
	"github.com/viant/xreflect"
	xunsafe "github.com/viant/xunsafe"
	"reflect"
	"strings"
)

// Schema represents View as Go type.
type (
	Schema struct {
		Package     string      `json:",omitempty" yaml:"Package,omitempty"`
		Name        string      `json:",omitempty" yaml:"Name,omitempty"`
		DataType    string      `json:",omitempty" yaml:"DataType,omitempty"`
		Cardinality Cardinality `json:",omitempty" yaml:"Cardinality,omitempty"`
		Methods     []reflect.Method
		rType       reflect.Type
		sliceType   reflect.Type
		slice       *xunsafe.Slice
		autoGen     bool
		autoGenFn   func() (reflect.Type, error)
		initialized bool
		isStruct    bool
		generator   func()
	}

	SchemaOption func(s *Schema)
)

func (s *Schema) LoadTypeIfNeeded(lookupType xreflect.LookupType) error {
	if s == nil {
		return nil
	}
	if s.rType != nil && s.rType != xreflect.InterfaceType {
		return nil
	}
	if s.TypeName() == "" {
		return nil
	}
	rType, err := lookupType(s.TypeName())
	if err != nil {
		return err
	}
	s.SetType(rType)
	return nil
}

func (s *Schema) EnsurePointer() {
	hasDataType := s.DataType != ""
	if hasDataType {
		if s.DataType == "" || s.DataType[0] == '*' {
			return
		}
		s.DataType = "*" + s.DataType
	}
	if s.Name == "" || s.Name[0] == '*' {
		return
	}
	s.Name = "*" + s.Name
}

func (s *Schema) SimpleTypeName() string {
	ret := s.TypeName()
	if index := strings.LastIndex(ret, "."); index != -1 {
		return ret[index+1:]
	}
	return ret
}

func (s *Schema) TypeName() string {
	name := shared.FirstNotEmpty(s.Name, s.DataType)
	if s.Package == "" {
		return name
	}
	return s.Package + "." + name
}

// Type returns struct type
func (s *Schema) Type() reflect.Type {
	if s == nil {
		return nil
	}
	return s.rType
}

// IsNamed returns true if compiled named type is used
func (s *Schema) IsNamed() bool {
	if s.rType == nil {
		return false
	}
	return s.rType.Name() != ""
}

// CompType returns component type
func (s *Schema) CompType() reflect.Type {
	if s.sliceType == nil {
		return nil
	}
	return s.sliceType.Elem()
}

func (s *Schema) IsStruct() bool {
	return s.isStruct
}

// SetType sets Types
func (s *Schema) SetType(rType reflect.Type) {
	if rType.Kind() == reflect.Slice { //i.e []int
		s.Cardinality = Many
	}

	if s.Cardinality == "" {
		s.Cardinality = One
	}

	if rType.Kind() == reflect.Struct && (strings.HasPrefix(s.DataType, "*") || strings.HasPrefix(s.Name, "*")) {
		rType = reflect.PtrTo(rType)
	}
	if s.Cardinality == Many {
		if rType.Kind() != reflect.Slice {
			rType = reflect.SliceOf(rType)
		}
	} else if rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	s.rType = rType
	s.slice = xunsafe.NewSlice(rType)
	s.sliceType = s.slice.Type

	if rType.Kind() == reflect.Struct {
		s.isStruct = true
	}
	if rType.Kind() == reflect.Ptr && rType.Elem().Kind() == reflect.Struct {
		s.isStruct = true
	}

}

// Init build struct type
func (s *Schema) Init(resource Resource) error {
	if s.initialized {
		return nil
	}

	s.initialized = true
	if strings.Contains(s.DataType, "[]") {
		s.Cardinality = Many
	}
	if s.Cardinality != Many {
		s.Cardinality = One
	}

	if err := s.LoadTypeIfNeeded(resource.LookupType()); err != nil || s.rType != nil {
		return err
	}
	if s.autoGenFn != nil {
		rType, err := s.autoGenFn()
		if err != nil {
			return err
		}
		s.SetType(rType)
	}
	s.autoGen = true
	return nil
}

const pkgPath = "github.com/viant/datly/View"

func NewField(aTag string, structFieldName string, rType reflect.Type) reflect.StructField {
	var fieldPkgPath string
	if structFieldName[0] < 'A' || structFieldName[0] > 'Z' {
		fieldPkgPath = pkgPath
	}

	aField := reflect.StructField{
		Name:    structFieldName,
		Type:    rType,
		Tag:     reflect.StructTag(aTag),
		PkgPath: fieldPkgPath,
	}
	return aField
}

func StructFieldName(sourceCaseFormat text.CaseFormat, columnName string) string {
	var structFieldName string
	if sourceCaseFormat == text.CaseFormatUpperCamel {
		structFieldName = columnName
	} else {
		structFieldName = sourceCaseFormat.Format(columnName, text.CaseFormatUpperCamel)
	}
	return structFieldName
}

// AutoGen indicates whether Schema was generated using ColumnTypes fetched from DB or was passed programmatically.
func (s *Schema) AutoGen() bool {
	return s.autoGen
}

// Slice returns slice as xunsafe.Slice
func (s *Schema) Slice() *xunsafe.Slice {
	return s.slice
}

// SliceType returns reflect.SliceOf() Schema type
func (s *Schema) SliceType() reflect.Type {
	return s.sliceType
}

func (s *Schema) InheritType(rType reflect.Type) {
	s.SetType(rType)
	s.autoGen = false
}

func (s *Schema) Clone() *Schema {
	schema := *s
	return &schema
}

func (s *Schema) InitType(lookupType xreflect.LookupType, ptr bool) error {
	if s.rType != nil {
		return nil
	}
	name := s.Name
	var options []xreflect.Option
	if name == "" {
		name = s.DataType
	}
	if name != s.DataType && strings.Contains(s.DataType, " ") { //TODO replace with xreflect check if definition
		options = append(options, xreflect.WithTypeDefinition(s.DataType))
	}
	if s.Package != "" {
		options = append(options, xreflect.WithPackage(s.Package))
	}

	if s.rType != nil {
		return nil
	}

	if name == "" {
		return fmt.Errorf("schema was empty")
	}
	rType, err := types.LookupType(lookupType, name, options...)
	if err != nil {
		return err
	}
	if ptr && rType.Kind() != reflect.Ptr {
		rType = reflect.PtrTo(rType)
	}
	s.SetType(rType)
	return nil
}

// WithAutoGenFlag creates with autogen schema option
func WithAutoGenFlag(flag bool) SchemaOption {
	return func(s *Schema) {
		s.autoGen = flag
	}
}

func WithAutoGenFunc(fn func() (reflect.Type, error)) SchemaOption {
	return func(s *Schema) {
		s.autoGenFn = fn
	}
}

func WithMany() SchemaOption {
	return func(s *Schema) {
		s.Cardinality = Many
	}
}

func NewSchema(compType reflect.Type, opts ...SchemaOption) *Schema {
	result := &Schema{
		Name:    "",
		autoGen: false,
	}

	for _, opt := range opts {
		opt(result)
	}
	if compType != nil {
		result.SetType(compType)
		if result.Name == "" {
			compTypeName := compType.Name()
			if pkg := compType.PkgPath(); pkg != "" {
				if index := strings.LastIndex(pkg, "/"); index != -1 {
					pkg = pkg[index+1:]
				}
				compTypeName = pkg + "." + compTypeName
			}
			result.Name = compTypeName
		}
		result.initialized = true
	}
	return result
}

var emptyStruct = reflect.TypeOf(struct{}{})

// EmptySchema returns empty struct schema
func EmptySchema() *Schema {
	return NewSchema(emptyStruct)
}
