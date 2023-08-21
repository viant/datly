package view

import (
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"github.com/viant/toolbox/format"
	"github.com/viant/xdatly/codec"
	"reflect"
	"strings"
)

// Columns wrap slice of Column
type Columns []*Column

func (c Columns) Index(caser format.Case) NamedColumns {
	result := NamedColumns{}
	for i, _ := range c {
		result.Register(caser, c[i])
	}
	return result
}

// NamedColumns represents *Column registry.
type NamedColumns map[string]*Column

func (c NamedColumns) ColumnName(key string) (string, error) {
	lookup, err := c.Lookup(key)
	if err != nil {
		return "", err
	}

	return lookup.Name, nil
}

func (c NamedColumns) Column(name string) (codec.Column, bool) {
	lookup, err := c.Lookup(name)
	if err != nil {
		return nil, false
	}

	return lookup, true
}

// Index indexes columns by Column.Name

// Register registers *Column
func (c NamedColumns) Register(caser format.Case, column *Column) {
	keys := shared.KeysOf(column.Name, true)
	for _, key := range keys {
		c[key] = column
	}
	c[caser.Format(column.Name, format.CaseUpperCamel)] = column

	if field := column.Field(); field != nil {
		c[field.Name] = column
	}
}

// RegisterHolder looks for the Column by Relation.Column name.
// If it finds registers that Column with Relation.Holder key.
func (c NamedColumns) RegisterHolder(columnName, holder string) error {
	column, err := c.Lookup(columnName)
	if err != nil {
		//TODO: evaluate later
		return nil
	}

	c[holder] = column
	keys := shared.KeysOf(holder, false)
	for _, key := range keys {
		c[key] = column
	}

	return nil
}

// Lookup returns Column with given name.
func (c NamedColumns) Lookup(name string) (*Column, error) {
	column, ok := c[name]
	if ok {
		return column, nil
	}

	column, ok = c[strings.ToUpper(name)]
	if ok {
		return column, nil
	}

	column, ok = c[strings.ToLower(name)]
	if ok {
		return column, nil
	}

	keys := make([]string, len(c))
	counter := 0
	for k := range c {
		keys[counter] = k
		counter++
	}

	return nil, fmt.Errorf("undefined column name %v, avails: %+v", name, strings.Join(keys, ","))
}

func (c NamedColumns) RegisterWithName(name string, column *Column) {
	keys := shared.KeysOf(name, true)
	for _, key := range keys {
		c[key] = column
	}
}

// Init initializes each Column in the slice.
func (c Columns) Init(resourcelet state.Resourcelet, config map[string]*ColumnConfig, caser format.Case, allowNulls bool) error {
	for i := range c {
		columnConfig := config[c[i].Name]

		if err := c[i].Init(resourcelet, caser, allowNulls, columnConfig); err != nil {
			return err
		}
	}

	return nil
}

func (c Columns) updateTypes(columns []*Column, caser format.Case) {
	index := Columns(columns).Index(caser)

	for _, column := range c {
		if column.ColumnType() == nil || shared.Elem(column.ColumnType()).Kind() == reflect.Interface {
			newCol, err := index.Lookup(column.Name)
			if err != nil {
				continue
			}
			column.SetColumnType(newCol.ColumnType())
		}
	}
}

func NewColumns(columns sqlparser.Columns) Columns {
	var result = make(Columns, 0, len(columns))
	for _, item := range columns {
		name := item.Identity()
		column := NewColumn(name, item.Type, item.RawType, item.IsNullable, WithTag(item.Tag))

		if item.Default != nil {
			column.Default = *item.Default
		}
		result = append(result, column)
	}
	return result
}
