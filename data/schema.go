package data

import (
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
)

//Schema represents View as Go type.
type Schema struct {
	Name string

	compType  reflect.Type
	sliceType reflect.Type

	slice *xunsafe.Slice
	xType *xunsafe.Type

	autoGen   bool
	OmitEmpty bool
	DataType  string
}

//Type returns struct type
func (c *Schema) Type() reflect.Type {
	return c.compType
}

func (c *Schema) setType(rType reflect.Type) {
	c.compType = rType
	c.slice = xunsafe.NewSlice(c.compType)
	c.sliceType = c.slice.Type
}

//Init build struct type
func (c *Schema) Init(columns []*Column, relations []*Relation, viewCaseFormat format.Case) {
	if c.compType != nil {
		c.setType(c.compType)
		return
	}

	excluded := make(map[string]bool)
	for _, rel := range relations {
		if !rel.IncludeColumn && rel.Cardinality == "One" {
			excluded[rel.Column] = true
		}
	}

	omitEmptyTag := ""
	if c.OmitEmpty {
		omitEmptyTag = `json:",omitempty" `
	}

	fieldsLen := len(columns)
	structFields := make([]reflect.StructField, 0)
	for i := 0; i < fieldsLen; i++ {
		if _, ok := excluded[columns[i].Name]; ok {
			continue
		}

		structFieldName := viewCaseFormat.Format(columns[i].Name, format.CaseUpperCamel)
		structFields = append(structFields, reflect.StructField{
			Name:  structFieldName,
			Type:  columns[i].rType,
			Index: []int{i},
			Tag:   reflect.StructTag(omitEmptyTag + `sqlx:"name="` + columns[i].Name + "`"),
		})
	}

	for _, rel := range relations {
		rType := rel.Of.DataType()
		if rType.Kind() == reflect.Struct {
			rType = reflect.PtrTo(rType)
			rel.Of.Schema.setType(rType)
		}

		if rel.Cardinality == "Many" {
			rType = reflect.SliceOf(rType)
		}

		structFields = append(structFields, reflect.StructField{
			Name: rel.Holder,
			Type: rType,
			Tag:  reflect.StructTag(omitEmptyTag),
		})
	}

	c.setType(reflect.StructOf(structFields))
	c.autoGen = true

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
