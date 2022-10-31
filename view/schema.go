package view

import (
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/sqlx/io/read/cache/ast"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

//Schema represents View as Go type.
type Schema struct {
	Name string `json:",omitempty" yaml:"name,omitempty"`

	compType  reflect.Type
	sliceType reflect.Type

	slice *xunsafe.Slice
	xType *xunsafe.Type

	autoGen     bool
	DataType    string `json:",omitempty" yaml:"dataType,omitempty"`
	Cardinality Cardinality
	initialized bool
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

//Type returns struct type
func (c *Schema) Type() reflect.Type {
	if c == nil {
		panic(c)
	}
	return c.compType
}

func (c *Schema) SetType(rType reflect.Type) {
	if c.Cardinality == "" {
		c.Cardinality = One
	}

	if c.Cardinality == Many {
		rType = reflect.SliceOf(rType)
	}

	c.compType = rType
	c.updateSliceType()
}

func (c *Schema) updateSliceType() {
	c.slice = xunsafe.NewSlice(c.compType)
	c.sliceType = c.slice.Type
}

//Init build struct type
func (c *Schema) Init(columns []*Column, relations []*Relation, viewCaseFormat format.Case, types Types, selfRef *SelfReference) error {
	if c.initialized {
		return nil
	}

	c.initialized = true
	if c.Cardinality != Many {
		c.Cardinality = One
	}

	if c.compType != nil {
		c.updateSliceType()
		return nil
	}

	if c.DataType != "" {
		rType, err := GetOrParseType(types, c.DataType)
		if err != nil {
			return err
		}

		c.SetType(rType)
		return nil
	}

	c.initByColumns(columns, relations, selfRef, viewCaseFormat)
	c.autoGen = true

	return nil
}

func (c *Schema) initByColumns(columns []*Column, relations []*Relation, selfRef *SelfReference, viewCaseFormat format.Case) {
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
		sqlxTag := `sqlx:"name=` + columnName + `" velty:"name=` + columnName + `"`

		var aTag string
		if defaultTag == "" {
			aTag = sqlxTag
		} else {
			aTag = sqlxTag + " " + defaultTag
		}

		aField := c.newField(aTag, columnName, viewCaseFormat, rType)
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

		holders[rel.Holder] = true
		structFields = append(structFields, reflect.StructField{
			Name: rel.Holder,
			Type: rType,
		})

		if meta := rel.Of.View.Template.Meta; meta != nil {
			metaType := meta.Schema.Type()
			if metaType.Kind() != reflect.Ptr {
				metaType = reflect.PtrTo(metaType)
			}

			structFields = append(structFields, c.newField(`json:",omitempty" yaml:",omitempty"`, meta.Name, format.CaseUpperCamel, metaType))
		}
	}

	if selfRef != nil {
		structFields = append(structFields, c.newField("", selfRef.Holder, format.CaseUpperCamel, reflect.SliceOf(ast.InterfaceType)))
	}

	structType := reflect.PtrTo(reflect.StructOf(structFields))
	c.SetType(structType)
}

func (c *Schema) newField(aTag string, columnName string, sourceCaseFormat format.Case, rType reflect.Type) reflect.StructField {
	var structFieldName string
	if sourceCaseFormat == format.CaseUpperCamel {
		structFieldName = columnName
	} else {
		structFieldName = sourceCaseFormat.Format(columnName, format.CaseUpperCamel)
	}

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

func createDefaultTagIfNeeded(column *Column) string {
	if column == nil {
		return ""
	}

	attributes := make([]string, 0)
	if column.Format != "" {
		attributes = append(attributes, json.FormatAttribute+"="+column.Format)
	}

	if column.Default != "" {
		attributes = append(attributes, json.ValueAttribute+"="+column.Default)
	}

	if len(attributes) == 0 {
		return ""
	}

	return json.DefaultTagName + `:"` + strings.Join(attributes, ",") + `"`
}

//AutoGen indicates whether Schema was generated using ColumnTypes fetched from DB or was passed programmatically.
func (c *Schema) AutoGen() bool {
	return c.autoGen
}

//Slice returns slice as xunsafe.Slice
func (c *Schema) Slice() *xunsafe.Slice {
	return c.slice
}

//SliceType returns reflect.SliceOf() Schema type
func (c *Schema) SliceType() reflect.Type {
	return c.sliceType
}

func (c *Schema) inheritType(rType reflect.Type) {
	c.SetType(rType)
	c.autoGen = false
}

//XType returns structType as *xunsafe.Type
func (c *Schema) XType() *xunsafe.Type {
	return c.xType
}

func (c *Schema) copy() *Schema {
	newSchema := &Schema{
		Name:     c.Name,
		autoGen:  c.autoGen,
		DataType: c.DataType,
	}

	newSchema.SetType(c.compType)
	return c
}

func (c *Schema) parseType(types Types) error {
	parseType, err := GetOrParseType(types, NotEmptyOf(c.DataType, c.Name))
	if err != nil {
		return err
	}

	c.SetType(parseType)
	return nil
}
