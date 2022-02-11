package data

import (
	"fmt"
	"reflect"
	"strings"
)

func AssembleView(parent *View, references ...*Reference) (*View, error) {
	for i := range references {
		if len(references[i].Child.Columns) == 0 {
			return nil, fmt.Errorf("all references have to have specified View columns")
		}
	}

	replacedColumns := make(map[string]*Reference)
	for i := range references {
		if references[i].Cardinality != "One" {
			continue
		}
		title := strings.Title(references[i].Column)
		replacedColumns[title] = references[i]
	}

	dataType := parent.DataType()
	elem := dataType.Elem()
	field := elem.NumField()
	newStructFields := make([]reflect.StructField, field)
	var relation *Reference
	var ok bool
	var columnName string
	i := 0
	for ; i < field; i++ {
		columnName = strings.Title(elem.Field(i).Name)
		if relation, ok = replacedColumns[columnName]; ok {
			newStructFields[i] = reflect.StructField{
				Name:  strings.Title(relation.RefHolder),
				Type:  relation.Child.Component.ComponentType(),
				Index: []int{i},
			}
		} else {
			newStructFields[i] = elem.Field(i)
		}
	}

	for relationIndex := range references {
		if references[relationIndex].Cardinality != "Many" {
			continue
		}

		newStructFields = append(newStructFields, reflect.StructField{
			Name:  strings.Title(references[relationIndex].RefHolder),
			Type:  reflect.SliceOf(references[relationIndex].Child.DataType()),
			Index: []int{i},
		})

		i++
	}

	return &View{
		Connector:  parent.Connector,
		Name:       parent.Name,
		Alias:      parent.Alias,
		Table:      parent.Table,
		From:       parent.From,
		Columns:    parent.Columns,
		Criteria:   parent.Criteria,
		Default:    &Config{},
		PrimaryKey: parent.PrimaryKey,
		Mutable:    parent.Mutable,
		References: references,
		Component:  NewComponent(reflect.StructOf(newStructFields)),
		columns:    nil,
	}, nil
}
