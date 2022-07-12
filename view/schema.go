package view

import (
	"github.com/viant/datly/router/marshal/json"
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
}

func NewSchema(compType reflect.Type) *Schema {
	result := &Schema{
		Name:    "",
		autoGen: false,
	}

	result.setType(compType)
	return result
}

//Type returns struct type
func (c *Schema) Type() reflect.Type {
	return c.compType
}

func (c *Schema) setType(rType reflect.Type) {
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
func (c *Schema) Init(columns []*Column, relations []*Relation, viewCaseFormat format.Case, types Types) error {
	if c.Cardinality != Many {
		c.Cardinality = One
	}

	if c.compType != nil {
		c.updateSliceType()
		return nil
	}

	if c.DataType != "" {
		rType, err := ParseType(c.DataType)
		if err != nil {
			rType, err = types.Lookup(c.DataType)
			if err != nil {
				return err
			}
		}

		c.setType(rType)
		return nil
	}

	c.initByColumns(columns, relations, viewCaseFormat)
	c.autoGen = true

	return nil
}

func (c *Schema) initByColumns(columns []*Column, relations []*Relation, viewCaseFormat format.Case) {
	excluded := make(map[string]bool)
	for _, rel := range relations {
		if !rel.IncludeColumn && rel.Cardinality == One {
			excluded[rel.Column] = true
		}
	}

	fieldsLen := len(columns)
	structFields := make([]reflect.StructField, 0)
	for i := 0; i < fieldsLen; i++ {
		if _, ok := excluded[columns[i].Name]; ok {
			continue
		}

		defaultTag := createDefaultTagIfNeeded(columns[i])
		sqlxTag := `sqlx:"name=` + columns[i].Name + `"`

		var aTag string
		if defaultTag == "" {
			aTag = sqlxTag
		} else {
			aTag = sqlxTag + " " + defaultTag
		}

		structFieldName := viewCaseFormat.Format(columns[i].Name, format.CaseUpperCamel)
		rType := columns[i].rType
		if columns[i].Nullable && rType.Kind() != reflect.Ptr {
			rType = reflect.PtrTo(rType)
		}

		if columns[i].Codec != nil {
			rType = columns[i].Codec.Schema.Type()
		}

		structFields = append(structFields, reflect.StructField{
			Name:  structFieldName,
			Type:  rType,
			Index: []int{i},
			Tag:   reflect.StructTag(aTag),
		})
	}

	holders := make(map[string]bool)
	for _, rel := range relations {
		if _, ok := holders[rel.Holder]; ok {
			continue
		}

		rType := rel.Of.DataType()
		if rType.Kind() == reflect.Struct {
			rType = reflect.PtrTo(rType)
			rel.Of.Schema.setType(rType)
		}

		if rel.Cardinality == Many {
			rType = reflect.SliceOf(rType)
		}

		holders[rel.Holder] = true

		structFields = append(structFields, reflect.StructField{
			Name: rel.Holder,
			Type: rType,
		})
	}

	structType := reflect.StructOf(structFields)
	c.setType(structType)
}

func createDefaultTagIfNeeded(column *Column) string {
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
	c.setType(rType)
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

	newSchema.setType(c.compType)
	return c
}
