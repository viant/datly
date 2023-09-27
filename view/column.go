package view

import (
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

// Column represents View column
type (
	Column struct {
		Name                string       `json:",omitempty"`
		DataType            string       `json:",omitempty"`
		Tag                 string       `json:",omitempty"`
		IgnoreCaseFormatter bool         `json:",omitempty"`
		Expression          string       `json:",omitempty"`
		Filterable          bool         `json:",omitempty"`
		Nullable            bool         `json:",omitempty"`
		Default             string       `json:",omitempty"`
		Format              string       `json:",omitempty"`
		Codec               *state.Codec `json:",omitempty"`
		DatabaseColumn      string       `json:",omitempty"`
		IndexedBy           string       `json:",omitempty"`

		rType         reflect.Type
		sqlExpression string
		field         *reflect.StructField
		_initialized  bool
		_fieldName    string
	}
	ColumnOption func(c *Column)
)

// SqlExpression builds column sql expression if any expression specified in format: Expression AS Name
func (c *Column) SqlExpression() string {
	return c.sqlExpression
}

// ColumnName returns Column Name
func (c *Column) ColumnName() string {
	return c.Name
}

// Init initializes Column
func (c *Column) Init(resourcelet state.Resource, caser format.Case, allowNulls bool, config *ColumnConfig) error {
	if c._initialized {
		return nil
	}
	c._initialized = true
	if config != nil {
		c.ApplyConfig(config)
		if config.Default != nil {
			c.Default = *config.Default
		}
	}

	if c.DatabaseColumn == "" {
		c.DatabaseColumn = c.Name
	}
	if c.Name == "" {
		return fmt.Errorf("column name was empty")
	}

	if c.rType == nil || c.rType == xreflect.InterfaceType {
		nonPtrType := c.rType
		for nonPtrType != nil && nonPtrType.Kind() == reflect.Ptr {
			nonPtrType = nonPtrType.Elem()
		}

		if nonPtrType == nil || c.DataType != "" {
			rType, err := types.LookupType(resourcelet.LookupType(), c.DataType)
			if err != nil && c.rType == nil {
				return err
			}

			if rType != nil {
				c.rType = rType
			}
		}
	}
	if err := c.buildSQLExpression(allowNulls); err != nil {
		return err
	}

	c._fieldName = caser.Format(c.Name, format.CaseUpperCamel)

	if c.Codec != nil {
		if err := c.Codec.Init(resourcelet, c.rType); err != nil {
			return err
		}
	}

	return nil
}

func (c *Column) buildSQLExpression(allowNulls bool) error {
	defaultValue := c.defaultValue(c.rType)
	c.sqlExpression = c.DatabaseColumn
	if c.Expression != "" {
		c.sqlExpression = c.Expression
	}

	if defaultValue != "" && !allowNulls {
		c.sqlExpression = "COALESCE(" + c.sqlExpression + "," + defaultValue + ") AS " + c.Name
	} else if c.Expression != "" {
		c.sqlExpression = c.sqlExpression + " AS " + c.Name
	}

	return nil
}

func (c *Column) SetField(field reflect.StructField) {
	c.field = &field
}

func (c *Column) Field() *reflect.StructField {
	return c.field
}

func (c *Column) defaultValue(rType reflect.Type) string {
	if !c.Nullable {
		return ""
	}
	switch rType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8,
		reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return `0`
	case reflect.Float64, reflect.Float32:
		return `0`
	case reflect.String:
		return `""`
	case reflect.Bool:
		return `false`
	default:
		return c.Default
	}
}

func (c *Column) FieldName() string {
	return c._fieldName
}

func (c *Column) ColumnType() reflect.Type {
	return c.rType
}

func (c *Column) SetColumnType(rType reflect.Type) {
	c.rType = rType
}

func (c *Column) ApplyConfig(config *ColumnConfig) {
	if config.Codec != nil {
		c.Codec = config.Codec
	}

	if config.DataType != nil {
		c.DataType = *config.DataType
	}

	if config.Tag != nil {
		c.Tag = *config.Tag
	}

	if config.Format != nil {
		c.Format = *config.Format
	}
	if config.IgnoreCaseFormatter != nil {
		c.IgnoreCaseFormatter = *config.IgnoreCaseFormatter
	}
}

func WithColumnTag(tag string) ColumnOption {
	return func(c *Column) {
		c.Tag = tag
	}
}

func NewColumn(name, dataTypeName string, rType reflect.Type, nullable bool, opts ...ColumnOption) *Column {
	ret := &Column{
		DatabaseColumn: name,
		Name:           strings.Trim(name, "'"),
		DataType:       dataTypeName,
		rType:          rType,
		Nullable:       nullable,
	}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

type ColumnConfig struct {
	Name                string       `json:",omitempty"`
	IgnoreCaseFormatter *bool        `json:",omitempty"`
	Expression          *string      `json:",omitempty"`
	Codec               *state.Codec `json:",omitempty"`
	DataType            *string      `json:",omitempty"`
	Format              *string      `json:",omitempty"`
	Tag                 *string      `json:",omitempty"`
	Default             *string      `json:",omitempty"`
}
