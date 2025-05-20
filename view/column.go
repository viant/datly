package view

import (
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlx/io"
	"github.com/viant/tagly/format"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

// Column represents View column
type (
	Column struct {
		Name     string `json:",omitempty"`
		DataType string `json:",omitempty"`
		Tag      string `json:",omitempty"`

		Expression     string       `json:",omitempty"`
		Filterable     bool         `json:",omitempty"`
		Nullable       bool         `json:",omitempty"`
		Default        string       `json:",omitempty"`
		FormatTag      *format.Tag  `json:",omitempty"`
		Codec          *state.Codec `json:",omitempty"`
		DatabaseColumn string       `json:",omitempty"`
		IndexedBy      string       `json:",omitempty"`

		rType         reflect.Type
		sqlExpression string
		field         *reflect.StructField
		_initialized  bool
		_fieldName    string
	}
	ColumnOption func(c *Column)
)

func (c *Column) TimeLayout() string {
	if c.FormatTag == nil {
		return ""
	}
	return c.FormatTag.TimeLayout
}

func (c *Column) CaseFormat() text.CaseFormat {
	if c.FormatTag == nil {
		return text.CaseFormatUndefined
	}
	return text.NewCaseFormat(c.FormatTag.CaseFormat)
}
func (c *Column) EnsureFormatTag() {
	if c.FormatTag == nil {
		c.FormatTag = &format.Tag{}
	}
}

// SqlExpression builds column sql expression if any expression specified in format: Expression AS Name
func (c *Column) SqlExpression() string {
	return c.sqlExpression
}

// ColumnName returns Column Name
func (c *Column) ColumnName() string {
	return c.Name
}

// Init initializes Column
func (c *Column) Init(resource state.Resource, caseFormat text.CaseFormat, allowNulls bool) error {
	if c._initialized {
		return nil
	}
	c._initialized = true
	if c.DatabaseColumn == "" {
		c.DatabaseColumn = c.Name
	}
	if c.Name == "" {
		return fmt.Errorf("column name was empty")
	}
	err := c.EnsureType(resource.LookupType())
	if err != nil {
		return err
	}
	if err := c.buildSQLExpression(allowNulls); err != nil {
		return err
	}

	if caseFormat.IsDefined() {
		c._fieldName = text.CaseFormatUpperCamel.Format(c.Name, caseFormat)
	} else {
		c._fieldName = c.Name //OR TO detect input case format and still convert to upper camel
	}
	if c.Codec != nil {
		if err := c.Codec.Init(resource, c.rType); err != nil {
			return err
		}
	}
	return nil
}

func (c *Column) EnsureType(lookupType xreflect.LookupType) error {
	if c.rType != nil && c.rType != xreflect.InterfaceType {
		return nil
	}
	if c.DataType == "" {
		if c.DataType == "" && c.rType == nil {
			return fmt.Errorf("invalid column %s, data type: %s", c.Name, c.DataType)
		}
		return nil
	}
	rType, err := types.LookupType(lookupType, c.DataType)
	if err != nil && c.rType == nil {
		return err
	}
	if rType != nil {
		c.rType = rType
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
		if c.Codec == nil {
			c.Codec = config.Codec

		} else if c.Codec.Name != config.Codec.Name {
			c.Codec = config.Codec
		}
	}

	if config.DataType != nil && *config.DataType != "" {
		setter.SetStringIfEmpty(&c.DataType, *config.DataType)
	}

	if config.Tag != nil && c.Tag != *config.Tag {
		c.Tag = " " + strings.Trim(*config.Tag, ` '`)
		if formatTag, _ := format.Parse(reflect.StructTag(*config.Tag)); formatTag != nil {
			c.FormatTag = formatTag
		}
	}
	if config.Default != nil {
		c.Default = *config.Default
	}
	c._initialized = false
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
	if sqlTag := io.ParseTag(reflect.StructTag(ret.Tag)); sqlTag != nil {
		if columnName := sqlTag.Name(); columnName != ret.DatabaseColumn && sqlTag.Ns == "" {
			ret.DatabaseColumn = columnName
		}
	}

	return ret
}

type (
	ColumnConfig struct {
		Name                string       `json:",omitempty"`
		Alias               string       `json:",omitempty"`
		IgnoreCaseFormatter *bool        `json:",omitempty"`
		Expression          *string      `json:",omitempty"`
		Codec               *state.Codec `json:",omitempty"`
		DataType            *string      `json:",omitempty"`
		Required            *bool        `json:",omitempty"`
		Format              *string      `json:",omitempty"`
		Tag                 *string      `json:",omitempty"`
		Default             *string      `json:",omitempty"`
	}

	ColumnConfigs []*ColumnConfig

	NamedColumnConfig map[string]*ColumnConfig
)

func (c *ColumnConfig) IgnoreColumn() bool {
	return c.Tag != nil && strings.Contains(*c.Tag, `sqlx:"-"`)
}

func (c ColumnConfigs) Index() NamedColumnConfig {
	var result = make(map[string]*ColumnConfig)
	for _, item := range c {
		result[item.Name] = item
	}
	return result
}
