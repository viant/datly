package data

import (
	"fmt"
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

	rType         reflect.Type
	sqlExpression string
}

//Type parses and returns column DataType.
func (c *Column) Type() (reflect.Type, error) {
	if c.rType != nil {
		return c.rType, nil
	}
	var err error
	c.rType, err = parseType(c.DataType)

	if err != nil {
		return nil, err
	}
	return c.rType, nil
}

//SqlExpression builds column sql expression if any expression specified in format: Expression AS Name
func (c *Column) SqlExpression() string {
	if c.sqlExpression != "" {
		return c.sqlExpression
	}

	c.sqlExpression = c.ColumnName()
	if c.Expression != "" {
		c.sqlExpression = c.Expression + " AS " + c.ColumnName()
	}

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
func (c *Column) Init() error {
	if c.Name == "" {
		return fmt.Errorf("column name was empty")
	}

	rType, err := parseType(c.DataType)
	if err != nil {
		return err
	}

	c.rType = rType
	return nil
}
