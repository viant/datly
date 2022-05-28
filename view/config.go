package view

import (
	"context"
	"fmt"
	"reflect"
)

var intType = reflect.TypeOf(0)
var stringType = reflect.TypeOf("")

//Config represent a view config selector
type (
	Config struct {
		//TODO: Should order by be a slice?
		OrderBy     string `json:",omitempty"`
		Limit       int    `json:",omitempty"`
		Constraints *Constraints

		LimitParam    *Parameter
		OffsetParam   *Parameter
		FieldsParam   *Parameter
		OrderByParam  *Parameter
		CriteriaParam *Parameter
	}
)

func (c *Config) Init(ctx context.Context, resource *Resource) error {
	c.ensureConstraints()
	if err := c.initCustomParams(ctx, resource); err != nil {
		return err
	}

	return nil
}

func (c *Config) ensureConstraints() {
	if c.Constraints == nil {
		c.Constraints = &Constraints{}
	}
}

func (c *Config) initCustomParams(ctx context.Context, resource *Resource) error {
	if err := c.initParamIfNeeded(ctx, c.CriteriaParam, resource, stringType); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.LimitParam, resource, intType); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.OrderByParam, resource, stringType); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.OffsetParam, resource, intType); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.FieldsParam, resource, stringType); err != nil {
		return err
	}

	return nil
}

func (c *Config) initParamIfNeeded(ctx context.Context, param *Parameter, resource *Resource, requiredType reflect.Type) error {
	if param == nil {
		return nil
	}

	if err := param.Init(ctx, resource, nil); err != nil {
		return err
	}

	if param.Schema.Type() != requiredType {
		return fmt.Errorf("parameter %v type missmatch, required parameter to be type of %v but was %v", param.Name, requiredType.String(), param.Schema.Type().String())
	}

	return nil
}
