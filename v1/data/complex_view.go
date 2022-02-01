package data

import (
	"fmt"
	"reflect"
	"strings"
)

type ComplexView struct {
	*View
	Relations       []*Relation
	Component       *Component
	replacedColumns map[string]*Relation
}

type Relation struct {
	Name      string
	Child     *View
	ChildName string

	Ref   *Reference
	RefId string
}

func (c *ComplexView) DataType() reflect.Type {
	return c.Component.ComponentType()
}

func AssembleView(parent *View, relations ...*Relation) (*ComplexView, error) {
	for i := range relations {
		if len(relations[i].Child.Columns) == 0 {
			return nil, fmt.Errorf("all relations have to have specified View columns")
		}
	}

	replacedColumns := make(map[string]*Relation)
	for i := range relations {
		if relations[i].Ref.Cardinality != "One" {
			continue
		}
		title := strings.Title(relations[i].Ref.On.Column)
		replacedColumns[title] = relations[i]
	}

	dataType := parent.DataType()
	elem := dataType.Elem()
	field := elem.NumField()
	newStructFields := make([]reflect.StructField, field)
	var relation *Relation
	var ok bool
	var columnName string
	i := 0
	for ; i < field; i++ {
		columnName = strings.Title(elem.Field(i).Name)
		if relation, ok = replacedColumns[columnName]; ok {
			newStructFields[i] = reflect.StructField{
				Name:  strings.Title(relation.Ref.On.RefHolder),
				Type:  relation.Child.Component.ComponentType(),
				Index: []int{i},
			}
		} else {
			newStructFields[i] = elem.Field(i)
		}
	}

	for relationIndex := range relations {
		if relations[relationIndex].Ref.Cardinality != "Many" {
			continue
		}

		newStructFields = append(newStructFields, reflect.StructField{
			Name:  strings.Title(relations[relationIndex].Ref.On.RefHolder),
			Type:  reflect.SliceOf(relations[relationIndex].Child.DataType()),
			Index: []int{i},
		})

		i++
	}

	return &ComplexView{
		View:            parent,
		Relations:       relations,
		Component:       NewComponent(reflect.StructOf(newStructFields)),
		replacedColumns: replacedColumns,
	}, nil
}
