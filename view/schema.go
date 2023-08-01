package view

import (
	"fmt"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/utils/types"
	"github.com/viant/sqlx/io/read/cache/ast"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	xunsafe "github.com/viant/xunsafe"
	"reflect"
	"strings"
)

// Schema represents View as Go type.
type Schema struct {
	Package     string `json:",omitempty" yaml:"package,omitempty"`
	Name        string `json:",omitempty" yaml:"name,omitempty"`
	DataType    string `json:",omitempty" yaml:"dataType,omitempty"`
	Cardinality Cardinality
	Methods     []reflect.Method
	compType    reflect.Type
	sliceType   reflect.Type
	slice       *xunsafe.Slice
	xType       *xunsafe.Type
	autoGen     bool
	initialized bool
}

func (s *Schema) TypeName() string {
	name := FirstNotEmpty(s.Name, s.DataType)
	if s.Package == "" {
		return name
	}
	return s.Package + "." + name
}
func NewSchema(compType reflect.Type) *Schema {
	result := &Schema{
		Name:        "",
		autoGen:     false,
		initialized: true,
	}

	result.SetType(compType)
	return result
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
	if c.Cardinality == Many && rType.Kind() != reflect.Slice {
		rType = reflect.SliceOf(rType)
	}
	c.compType = rType
	c.updateSliceType()
}

func (c *Schema) updateSliceType() {
	c.slice = xunsafe.NewSlice(c.compType)
	c.sliceType = c.slice.Type
}

// Init build struct type
func (c *Schema) Init(resource Resourcelet, viewCaseFormat format.Case, options ...interface{}) error {
	var columns []*Column
	var relations []*Relation
	var selfRef *SelfReference
	var async *Async

	for _, option := range options {
		if option == nil {
			continue
		}

		switch actual := option.(type) {
		case []*Column:
			columns = actual
		case []*Relation:
			relations = actual
		case *SelfReference:
			selfRef = actual
		case *Async:
			async = actual
		}
	}

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

	if c.compType != nil {
		c.updateSliceType()
		return nil
	}

	if c.DataType != "" {
		rType, err := types.LookupType(resource.LookupType(), c.TypeName())
		if err != nil {
			return err
		}

		c.SetType(rType)
		return nil
	}

	c.initByColumns(columns, relations, selfRef, viewCaseFormat, async)
	c.autoGen = true

	return nil
}

func (c *Schema) initByColumns(columns []*Column, relations []*Relation, selfRef *SelfReference, viewCaseFormat format.Case, async *Async) {
	excluded := make(map[string]bool)
	for _, rel := range relations {
		if !rel.IncludeColumn && rel.Cardinality == One {
			excluded[rel.Column] = true
		}
	}

	fieldsLen := len(columns)
	structFields := make([]reflect.StructField, 0)
	for i := 0; i < fieldsLen; i++ {
		columnName := columns[i].Name
		if _, ok := excluded[columnName]; ok {
			continue
		}

		rType := columns[i].rType
		if columns[i].Nullable && rType.Kind() != reflect.Ptr {
			rType = reflect.PtrTo(rType)
		}

		if columns[i].Codec != nil {
			rType = columns[i].Codec.Schema.Type()
		}

		defaultTag := createDefaultTagIfNeeded(columns[i])

		sqlxTag := `sqlx:"name=` + columnName + `"`

		var aTag string
		if defaultTag == "" {
			aTag = sqlxTag
		} else {
			aTag = sqlxTag + " " + defaultTag
		}

		if columns[i].Tag != "" {
			if aTag != "" {
				aTag += " "
			}
			aTag += columns[i].Tag
		}

		if !strings.Contains(aTag, "velty") {
			names := columnName
			if aFieldName := StructFieldName(viewCaseFormat, columnName); aFieldName != names {
				names = names + "|" + aFieldName
			}
			aTag += fmt.Sprintf(` velty:"names=%v"`, names)
		}

		aField := newCasedField(aTag, columnName, viewCaseFormat, rType)
		structFields = append(structFields, aField)
	}

	holders := make(map[string]bool)
	for _, rel := range relations {
		if _, ok := holders[rel.Holder]; ok {
			continue
		}

		rType := rel.Of.DataType()
		if rType.Kind() == reflect.Struct {
			rType = reflect.PtrTo(rType)
			rel.Of.Schema.SetType(rType)
		}

		if rel.Cardinality == Many {
			rType = reflect.SliceOf(rType)
		}

		var fieldTag string
		if async != nil {
			if async.MarshalRelations {
				fieldTag = AsyncTagName + `:"enc=JSON" jsonx:"rawJSON"`
			} else {
				fieldTag = AsyncTagName + `:"table=` + async.Table + `"`
			}
		}

		holders[rel.Holder] = true
		structFields = append(structFields, reflect.StructField{
			Name: rel.Holder,
			Type: rType,
			Tag:  reflect.StructTag(fieldTag),
		})

		if meta := rel.Of.View.Template.Meta; meta != nil {
			metaType := meta.Schema.Type()
			if metaType.Kind() != reflect.Ptr {
				metaType = reflect.PtrTo(metaType)
			}

			tag := `json:",omitempty" yaml:",omitempty" sqlx:"-"`
			structFields = append(structFields, newCasedField(tag, meta.Name, format.CaseUpperCamel, metaType))
		}
	}

	if selfRef != nil {
		structFields = append(structFields, newCasedField("", selfRef.Holder, format.CaseUpperCamel, reflect.SliceOf(ast.InterfaceType)))
	}
	structType := reflect.PtrTo(reflect.StructOf(structFields))
	c.SetType(structType)
}

func newCasedField(aTag string, columnName string, sourceCaseFormat format.Case, rType reflect.Type) reflect.StructField {
	structFieldName := StructFieldName(sourceCaseFormat, columnName)

	return newField(aTag, structFieldName, rType)
}

func newField(aTag string, structFieldName string, rType reflect.Type) reflect.StructField {
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

func createDefaultTagIfNeeded(column *Column) string {
	if column == nil {
		return ""
	}

	attributes := make([]string, 0)
	if column.Format != "" {
		attributes = append(attributes, json.FormatAttribute+"="+column.Format)
	}
	if column.IgnoreCaseFormatter {
		attributes = append(attributes, json.IgnoreCaseFormatter+"=true,name="+column.Name)
	}
	if column.Default != "" {
		attributes = append(attributes, json.ValueAttribute+"="+column.Default)
	}

	if len(attributes) == 0 {
		return ""
	}

	return json.DefaultTagName + `:"` + strings.Join(attributes, ",") + `"`
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

func (c *Schema) inheritType(rType reflect.Type) {
	c.SetType(rType)
	c.autoGen = false
}

// XType returns structType as *xunsafe.Type
func (c *Schema) XType() *xunsafe.Type {
	return c.xType
}

func (c *Schema) copy() *Schema {
	//newSchema := &Schema{
	//	Name:     c.Name,
	//	autoGen:  c.autoGen,
	//	Type: c.Type,
	//}

	schema := *c
	return &schema
}

func (c *Schema) setType(lookupType xreflect.LookupType, ptr bool) error {
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
