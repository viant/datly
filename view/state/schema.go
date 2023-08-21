package state

import (
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	xunsafe "github.com/viant/xunsafe"
	"reflect"
	"strings"
)

// Schema represents View as Go type.
type (
	Schema struct {
		Package     string `json:",omitempty" yaml:"package,omitempty"`
		Name        string `json:",omitempty" yaml:"name,omitempty"`
		DataType    string `json:",omitempty" yaml:"dataType,omitempty"`
		Cardinality Cardinality
		Methods     []reflect.Method
		compType    reflect.Type
		sliceType   reflect.Type
		slice       *xunsafe.Slice
		autoGen     bool
		autoGenFn   func() (reflect.Type, error)
		initialized bool
		generator   func()
	}

	SchemaOption func(s *Schema)
)

func (s *Schema) TypeName() string {
	name := shared.FirstNotEmpty(s.Name, s.DataType)
	if s.Package == "" {
		return name
	}
	return s.Package + "." + name
}

// Type returns struct type
func (c *Schema) Type() reflect.Type {
	return c.compType
}

func (c *Schema) SetType(rType reflect.Type) {
	if rType.Kind() == reflect.Slice { //i.e []int
		c.Cardinality = Many
	}
	if c.Cardinality == "" {
		c.Cardinality = One
	}
	if c.Cardinality == Many {
		if rType.Kind() != reflect.Slice {
			rType = reflect.SliceOf(rType)
		}
	} else if rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	c.compType = rType
	c.slice = xunsafe.NewSlice(rType)
	c.sliceType = c.slice.Type
}

// Init build struct type
func (c *Schema) Init(resource Resourcelet) error {
	if c.initialized {
		return nil
	}

	c.initialized = true
	if strings.Contains(c.DataType, "[]") {
		c.Cardinality = Many
	}
	if c.Cardinality != Many {
		c.Cardinality = One
	}

	if c.DataType != "" {
		rType, err := types.LookupType(resource.LookupType(), c.TypeName())
		if err != nil {
			return err
		}

		c.SetType(rType)
		return nil
	}

	if c.autoGenFn != nil {
		rType, err := c.autoGenFn()
		if err != nil {
			return err
		}
		c.SetType(rType)
	}
	c.autoGen = true
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

func StructFieldName(sourceCaseFormat format.Case, columnName string) string {
	var structFieldName string
	if sourceCaseFormat == format.CaseUpperCamel {
		structFieldName = columnName
	} else {
		structFieldName = sourceCaseFormat.Format(columnName, format.CaseUpperCamel)
	}
	return structFieldName
}

// AutoGen indicates whether Schema was generated using ColumnTypes fetched from DB or was passed programmatically.
func (c *Schema) AutoGen() bool {
	return c.autoGen
}

// Slice returns slice as xunsafe.Slice
func (c *Schema) Slice() *xunsafe.Slice {
	return c.slice
}

// SliceType returns reflect.SliceOf() Schema type
func (c *Schema) SliceType() reflect.Type {
	return c.sliceType
}

func (c *Schema) InheritType(rType reflect.Type) {
	c.SetType(rType)
	c.autoGen = false
}

func (c *Schema) copy() *Schema {
	schema := *c
	return &schema
}

func (c *Schema) InitType(lookupType xreflect.LookupType, ptr bool) error {
	name := c.Name
	var options []xreflect.Option
	if name == "" {
		name = c.DataType
	}
	if name != c.DataType && strings.Contains(c.DataType, " ") { //TODO replace with xreflect check if definition
		options = append(options, xreflect.WithTypeDefinition(c.DataType))
	}
	if c.Package != "" {
		options = append(options, xreflect.WithPackage(c.Package))
	}
	rType, err := types.LookupType(lookupType, name, options...)
	if err != nil {
		return err
	}
	if ptr && rType.Kind() != reflect.Ptr {
		rType = reflect.PtrTo(rType)
	}
	c.SetType(rType)
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
			result.Name = compType.Name()
		}
		result.initialized = true
	}
	return result
}
