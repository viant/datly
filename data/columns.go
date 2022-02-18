package data

import (
	"fmt"
	"github.com/viant/toolbox/format"
)

//ColumnSlice wrap slice of Column
type ColumnSlice []*Column

type Columns map[string]*Column

//Index indexes columns by Column Name using various strategies.
func (c ColumnSlice) Index(caser format.Case) Columns {
	result := Columns{}
	for i, _ := range c {
		result.Register(caser, c[i])
	}
	return result
}

func (c Columns) Register(caser format.Case, column *Column) {
	keys := KeysOf(column.Name, true)
	for _, key := range keys {
		c[key] = column
	}
	c[caser.Format(column.Name, format.CaseUpperCamel)] = column
}

func (c Columns) RegisterHolder(relation *Relation) error {
	column, err := c.Lookup(relation.Column)
	if err != nil {
		return err
	}

	c[relation.Holder] = column
	keys := KeysOf(relation.Holder, false)
	for _, key := range keys {
		c[key] = column
	}

	return nil
}

func (c Columns) Lookup(name string) (*Column, error) {
	column, ok := c[name]
	if !ok {
		return column, fmt.Errorf("not found column with name %v", name)
	}

	return column, nil
}

//Init initializes every Column in the slice.
func (c ColumnSlice) Init() error {
	for i := range c {
		if err := c[i].Init(); err != nil {
			return nil
		}
	}

	return nil
}
