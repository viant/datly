package view

import (
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

// Columns wrap slice of Column
type Columns []*Column

func (c Columns) Index(formatCase text.CaseFormat) NamedColumns {
	result := NamedColumns{}
	for i, _ := range c {
		if aTag := c[i].Tag; aTag != "" {
			if src := reflect.StructTag(aTag).Get("source"); src != "" {
				result[strings.ToLower(src)] = c[i]
			}
		}
		result.Register(formatCase, c[i])
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

// Views indexes columns by Column.Name

// Register registers *Column
func (c NamedColumns) Register(caseFormat text.CaseFormat, column *Column) {
	keys := shared.KeysOf(column.Name, true)
	for _, key := range keys {
		c[key] = column
	}
	c[caseFormat.Format(column.Name, text.CaseFormatUpperCamel)] = column

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

// ApplyConfig applies column config
func (c Columns) ApplyConfig(configs map[string]*ColumnConfig, lookupType xreflect.LookupType) error {
	if len(configs) == 0 {
		return nil
	}
	for _, column := range c {
		cfg, ok := configs[column.Name]
		if !ok {
			continue
		}
		if cfg.Required != nil && *cfg.Required {
			column.Nullable = false
		}
		columnType := column.DataType
		column.ApplyConfig(cfg)

		if column.DataType != columnType {
			rType, err := types.LookupType(lookupType, column.DataType)
			if err != nil {
				return fmt.Errorf("failed to update column: %v %w", column.Name, err)
			}
			column.SetColumnType(rType)
		}

	}
	return nil
}

// Init initializes each Column in the slice.
func (c Columns) Init(resourcelet state.Resource, caseFormat text.CaseFormat, allowNulls bool) error {
	for i := range c {
		if err := c[i].Init(resourcelet, caseFormat, allowNulls); err != nil {
			return err
		}
	}
	return nil
}

func (c Columns) updateTypes(columns []*Column, caseFormat text.CaseFormat) {
	index := Columns(columns).Index(caseFormat)

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
		column := NewColumn(name, item.Type, item.RawType, item.IsNullable, WithColumnTag(item.Tag))
		if item.Name != item.Alias && item.Alias != "" && item.Name != "" {
			column.Tag = fmt.Sprintf(`source:"%v"`, item.Name)
		}
		if item.Default != nil {
			column.Default = *item.Default
		}
		if column.rType == nil {
			column.rType, _ = types.LookupType(extension.Config.Types.Lookup, column.DataType)
		}
		result = append(result, column)
	}
	return result
}
