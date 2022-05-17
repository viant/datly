package data

import (
	"fmt"
	"github.com/viant/datly/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox/format"
	"reflect"
	"strings"
	"time"
)

//Column represents data View column
type Column struct {
	Name       string `json:",omitempty"`
	DataType   string `json:",omitempty"`
	Expression string `json:",omitempty"`
	Filterable bool   `json:",omitempty"`
	Nullable   bool

	rType         reflect.Type
	tag           *io.Tag
	sqlExpression string
	criteriaKind  sql.Kind
	field         *reflect.StructField
	initialized   bool
	_fieldName    string
}

//SqlExpression builds column sql expression if any expression specified in format: Expression AS Name
func (c *Column) SqlExpression() string {
	return c.sqlExpression
}

func parseType(dataType string) (reflect.Type, error) {
	switch strings.Title(dataType) {
	case "Int":
		return reflect.TypeOf(0), nil
	case "Float", "Float64":
		return reflect.TypeOf(0.0), nil
	case "Bool":
		return reflect.TypeOf(false), nil
	case "String":
		return reflect.TypeOf(""), nil
	case "Date", "Time":
		return reflect.TypeOf(time.Time{}), nil
	}
	return nil, fmt.Errorf("unsupported column type: %v", dataType)
}

//ColumnName returns Column Name
func (c *Column) ColumnName() string {
	return c.Name
}

//Init initializes Column
func (c *Column) Init(caser format.Case) error {
	if c.initialized {
		return nil
	}
	c.initialized = true

	if c.Name == "" {
		return fmt.Errorf("column name was empty")
	}

	if c.rType == nil {
		rType, err := parseType(c.DataType)
		if err != nil {
			return err
		}
		c.rType = rType
	}

	switch c.rType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8,
		reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		c.criteriaKind = sql.Int
	case reflect.Float64, reflect.Float32:
		c.criteriaKind = sql.Float
	case reflect.String:
		c.criteriaKind = sql.String
	case reflect.Bool:
		c.criteriaKind = sql.Bool
	}

	if err := c.buildSQLExpression(); err != nil {
		return err
	}

	c._fieldName = caser.Format(c.Name, format.CaseUpperCamel)

	return nil
}

func (c *Column) buildSQLExpression() error {
	defaultValue := c.defaultValue(c.rType)
	c.sqlExpression = c.Name
	if c.Expression != "" {
		c.sqlExpression = c.Expression
	}

	if defaultValue != "" {
		c.sqlExpression = "COALESCE(" + c.sqlExpression + "," + defaultValue + ") AS " + c.Name
	} else if c.Expression != "" {
		c.sqlExpression = c.sqlExpression + " AS " + c.Name

	}

	return nil
}

//Kind returns  Column sql.Kind
func (c *Column) Kind() sql.Kind {
	return c.criteriaKind
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
