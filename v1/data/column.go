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

	oldName       string
	reference     *Reference
	rType         reflect.Type
	structField   *reflect.StructField
	sqlExpression string
}

func (c *Column) setReference(reference *Reference) {
	rType := reference.Child.DataType()
	if reference.Cardinality == "Many" {
		rType = reflect.SliceOf(rType)
	}

	c.rType = rType

	c.structField = &reflect.StructField{
		Name: strings.Title(reference.RefHolder),
		Type: rType,
		Tag:  reflect.StructTag(`sqlx:"name="` + c.Name),
	}
	c.oldName = c.Name
	c.Name = strings.Title(reference.RefHolder)
	c.reference = reference
}

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

func (c *Column) SqlExpression() string {
	if c.sqlExpression != "" {
		return c.sqlExpression
	}

	c.sqlExpression = c.SqlColumnName()
	if c.Expression != "" {
		c.sqlExpression = c.Expression + " AS " + c.SqlColumnName()
	}

	return c.sqlExpression
}

func parseType(dataType string) (reflect.Type, error) {
	switch strings.Title(dataType) {
	case "Int":
		return reflect.TypeOf(0), nil
	case "Float":
		return reflect.TypeOf(0.0), nil
	case "Float64":
		return reflect.TypeOf(0.0), nil
	case "Bool":
		return reflect.TypeOf(false), nil
	case "String":
		return reflect.TypeOf(""), nil
	case "Date":
		return reflect.TypeOf(time.Time{}), nil
	case "Time":
		return reflect.TypeOf(time.Time{}), nil
	}
	return nil, fmt.Errorf("unsupported column type: %v", dataType)
}

func (c *Column) SqlColumnName() string {
	if c.oldName != "" {
		return c.oldName
	}

	return c.Name
}

func (c *Column) StructField() (*reflect.StructField, error) {
	if c.structField != nil {
		return c.structField, nil
	}

	rType, err := c.Type()
	if err != nil {
		return nil, err
	}

	c.structField = &reflect.StructField{
		Name: strings.Title(c.Name),
		Type: rType,
	}

	return c.structField, nil
}
