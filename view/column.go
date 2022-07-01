package view

import (
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox/format"
	"reflect"
	"strings"
	"time"
)

type foo struct {
	Interface interface{}
}

//Column represents view View column
type Column struct {
	Name       string `json:",omitempty"`
	DataType   string `json:",omitempty"`
	Expression string `json:",omitempty"`
	Filterable bool   `json:",omitempty"`
	Nullable   bool   `json:",omitempty"`
	Default    string `json:",omitempty"`
	Format     string `json:",omitempty"`

	rType         reflect.Type
	tag           *io.Tag
	sqlExpression string
	field         *reflect.StructField
	initialized   bool
	_fieldName    string
}

//SqlExpression builds column sql expression if any expression specified in format: Expression AS Name
func (c *Column) SqlExpression() string {
	return c.sqlExpression
}

func ParseType(dataType string) (reflect.Type, error) {
	precisionIndex := strings.Index(dataType, "(")
	if precisionIndex != -1 {
		dataType = dataType[:precisionIndex]
	}

	switch strings.ToLower(dataType) {
	case "int", "integer", "bigint", "smallint", "tinyint":
		return reflect.TypeOf(0), nil
	case "float", "float64", "numeric", "decimal":
		return reflect.TypeOf(0.0), nil
	case "bool", "boolean":
		return reflect.TypeOf(false), nil
	case "string", "varchar", "char", "text":
		return reflect.TypeOf(""), nil
	case "date", "time", "timestamp", "datetime":
		return reflect.TypeOf(time.Time{}), nil
	case "interface":
		t := reflect.ValueOf(interface{}(foo{})).FieldByName("Interface").Type()
		return t, nil
	}

	return nil, fmt.Errorf("unsupported type: %v", dataType)
}

//ColumnName returns Column Name
func (c *Column) ColumnName() string {
	return c.Name
}

//Init initializes Column
func (c *Column) Init(caser format.Case, allowNulls bool) error {
	if c.initialized {
		return nil
	}
	c.initialized = true

	if c.Name == "" {
		return fmt.Errorf("column name was empty")
	}

	if c.rType == nil {
		rType, err := ParseType(c.DataType)
		if err != nil {
			return err
		}
		c.rType = rType
	}

	if err := c.buildSQLExpression(allowNulls); err != nil {
		return err
	}

	c._fieldName = caser.Format(c.Name, format.CaseUpperCamel)

	return nil
}

func (c *Column) buildSQLExpression(allowNulls bool) error {
	defaultValue := c.defaultValue(c.rType)
	c.sqlExpression = c.Name
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

func (c *Column) setField(field reflect.StructField) {
	c.field = &field
	c.tag = io.ParseTag(field.Tag.Get(option.TagSqlx))
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
		return ""
	}
}

func (c *Column) FieldName() string {
	return c._fieldName
}

func (c *Column) ColumnType() reflect.Type {
	return c.rType
}
