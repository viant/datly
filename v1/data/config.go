package data

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox"
	"reflect"
	"strings"
)

//Config represent a data selector for projection and selection
type Config struct {
	Columns    []string       `json:",omitempty"`
	Prefix     string         `json:",omitempty"`
	OrderBy    string         `json:",omitempty"`
	Offset     int            `json:",omitempty"`
	CaseFormat string         `json:",omitempty"`
	Limit      int            `json:",omitempty"`
	OmitEmpty  bool           `json:",omitempty"`
	Criteria   *data.Criteria `json:",omitempty"`

	selected       map[string]bool
	rType          reflect.Type
	columns        []*Column
	defaultColumns map[string]bool
}

func (c *Config) GetType() reflect.Type {
	return c.rType
}

func (c *Config) GetColumns() []*Column {
	return c.columns
}

func (c *Config) GetOrderBy() string {
	return c.OrderBy
}

func (c *Config) GetOffset() int {
	return c.Offset
}

func (c *Config) GetLimit() int {
	return c.Limit
}

//Apply applies selector values
func (c *Config) Apply(bindings map[string]interface{}) {
	if value, ok := bindings[c.Prefix+shared.FieldsKey]; ok {
		if fields := toolbox.AsString(value); value != "" {
			c.Columns = asStringSlice(fields)
			c.selected = make(map[string]bool)
			for _, column := range c.Columns {
				c.selected[column] = true
			}
		}
	}
	if value, ok := bindings[c.Prefix+shared.OrderByKey]; ok {
		c.OrderBy = toolbox.AsString(value)
	}

	if value, ok := bindings[c.Prefix+shared.CriteriaKey]; ok {
		if c.Criteria == nil {
			c.Criteria = &data.Criteria{}
		}
		c.Criteria.Expression = toolbox.AsString(value)
		if value, ok := bindings[c.Prefix+shared.ParamsKey]; ok {
			if fields := toolbox.AsString(value); value != "" {
				c.Criteria.Params = asStringSlice(fields)
			}
		}
	}
	if value, ok := bindings[c.Prefix+shared.LimitKey]; ok {
		c.Limit = toolbox.AsInt(value)
	}
	if value, ok := bindings[c.Prefix+shared.OffsetKey]; ok {
		c.Offset = toolbox.AsInt(value)
	}
}

//IsSelected returns true if supplied column matched selector.columns or selector has no specified columns.
func (c *Config) IsSelected(columns []string) bool {
	if len(c.selected) == 0 { //no filter all selected
		return true
	}
	for _, column := range columns {
		if !c.selected[column] {
			return false
		}
	}
	return true
}

func (c *Config) ensureColumns(columns []*Column) error {
	if len(c.Columns) == 0 {
		c.setColumns(columns)
	} else {
		c.ensureDefaultColumns()
		filteredColumns := make([]*Column, 0)
		for i := range columns {
			if _, ok := c.defaultColumns[strings.Title(columns[i].Name)]; !ok {
				continue
			}
			filteredColumns = append(filteredColumns, columns[i])
		}
		c.setColumns(filteredColumns)
	}

	return c.buildType()
}

func (c *Config) setColumns(columns []*Column) {
	columnsLen := len(columns)
	c.Columns = make([]string, columnsLen)
	c.columns = columns
	for i := 0; i < columnsLen; i++ {
		c.Columns[i] = columns[i].Name
	}
}

func (c *Config) buildType() error {
	structFields := make([]reflect.StructField, len(c.columns))
	for i := range c.columns {
		colType, err := c.columns[i].Type()
		if err != nil {
			return err
		}

		structFields[i] = reflect.StructField{
			Name: strings.Title(c.columns[i].Name),
			Type: colType,
		}
	}

	c.rType = reflect.StructOf(structFields)
	return nil
}

func (c *Config) ensureDefaultColumns() map[string]bool {
	if c.defaultColumns != nil {
		return c.defaultColumns
	}

	result := make(map[string]bool)
	for i := range c.Columns {
		result[c.Columns[i]] = true
		result[strings.ToLower(c.Columns[i])] = true
		result[strings.ToUpper(c.Columns[i])] = true
		result[strings.Title(c.Columns[i])] = true
	}

	c.defaultColumns = result
	return result
}

func asStringSlice(text string) []string {
	var result = make([]string, 0)
	items := strings.Split(text, ",")
	for _, item := range items {
		result = append(result, strings.TrimSpace(item))
	}
	return result
}
