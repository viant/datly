package data

import (
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox/format"
	"reflect"
	"strings"
)

//ColumnSlice wrap slice of Column
type ColumnSlice []*Column

//Columns represents *Column registry.
type Columns map[string]*Column

//Index indexes columns by Column.Name
func (c ColumnSlice) Index(caser format.Case) Columns {
	result := Columns{}
	for i, _ := range c {
		result.Register(caser, c[i])
	}
	return result
}

//Register registers *Column
func (c Columns) Register(caser format.Case, column *Column) {
	keys := shared.KeysOf(column.Name, true)
	for _, key := range keys {
		c[key] = column
	}
	c[caser.Format(column.Name, format.CaseUpperCamel)] = column

	if column.field != nil {
		c[column.field.Name] = column
	}
}

//RegisterHolder looks for the Column by Relation.Column name.
//If it finds registers that Column with Relation.Holder key.
func (c Columns) RegisterHolder(relation *Relation) error {
	column, err := c.Lookup(relation.Column)
	if err != nil {
		//TODO: evaluate later
		return nil
	}

	c[relation.Holder] = column
	keys := shared.KeysOf(relation.Holder, false)
	for _, key := range keys {
		c[key] = column
	}

	return nil
}

//Lookup returns Column with given name.
func (c Columns) Lookup(name string) (*Column, error) {
	if dotPos := strings.Index(name, ".");dotPos !=-1 {
		name = name[dotPos:]
	}
	name = strings.ToLower(name)

	column, ok := c[name]
	if !ok {
		keys := []string{}
		for k := range c {
			keys = append(keys, k)
		}
		err := fmt.Errorf("undefied columnname %v, avails: %+v", name, strings.Join(keys, ","))
		return column, err
	}

	return column, nil
}

func (c Columns) RegisterWithName(name string, column *Column) {
	keys := shared.KeysOf(name, true)
	for _, key := range keys {
		c[key] = column
	}
}

//Init initializes each Column in the slice.
func (c ColumnSlice) Init() error {
	for i := range c {
		if err := c[i].Init(); err != nil {
			return err
		}
	}
	return nil
}

func (c ColumnSlice) updateTypes(columns []*Column, caser format.Case) {
	index := ColumnSlice(columns).Index(caser)

	for _, column := range c {
		if column.rType == nil || shared.Elem(column.rType).Kind() == reflect.Interface {
			newCol, err := index.Lookup(column.Name)
			if err != nil {
				continue
			}

			column.rType = newCol.rType
		}
	}
}
