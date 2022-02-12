package data

import (
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
)

type Component struct {
	Name      string
	compType  reflect.Type
	slice     *xunsafe.Slice
	sliceType reflect.Type
	autoGen   bool
	OmitEmpty bool
}

//Type returns struct type
func (c *Component) Type() reflect.Type {
	return c.compType
}

func (c *Component) setType(rType reflect.Type) {
	switch rType.Kind() {
	case reflect.Struct:
		rType = reflect.PtrTo(rType)
	}
	c.compType = rType
	c.slice = xunsafe.NewSlice(c.compType)
	c.sliceType = c.slice.Type
}

//Init build struct type from Fields
func (c *Component) Init(columns []*Column, relations []*Relation, viewCaseFormat format.Case) {
	if c.compType != nil {
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

func (c *Component) AutoGen() bool {
	return c.autoGen
}

func (c *Component) Slice() *xunsafe.Slice {
	return c.slice
}

func (c *Component) SliceType() reflect.Type {
	return c.sliceType
}
