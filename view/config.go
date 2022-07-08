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
		OrderBy     string       `json:",omitempty"`
		Limit       int          `json:",omitempty"`
		Constraints *Constraints `json:",omitempty"`

		LimitParam    *Parameter `json:",omitempty"`
		OffsetParam   *Parameter `json:",omitempty"`
		FieldsParam   *Parameter `json:",omitempty"`
		OrderByParam  *Parameter `json:",omitempty"`
		CriteriaParam *Parameter `json:",omitempty"`
	}
)

func (c *Config) Init(ctx context.Context, resource *Resource, parent *View) error {
	if err := c.ensureConstraints(resource); err != nil {
		return err
	}

	if err := c.initCustomParams(ctx, resource, parent); err != nil {
		return err
	}

	return nil
}

func (c *Config) ensureConstraints(resource *Resource) error {
	if c.Constraints == nil {
		c.Constraints = &Constraints{}
	}

	return c.Constraints.init(resource)
}

func (c *Config) initCustomParams(ctx context.Context, resource *Resource, parent *View) error {
	if err := c.initParamIfNeeded(ctx, c.CriteriaParam, resource, stringType, parent); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.LimitParam, resource, intType, parent); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.OrderByParam, resource, stringType, parent); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.OffsetParam, resource, intType, parent); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.FieldsParam, resource, stringType, parent); err != nil {
		return err
	}

	return nil
}

func (c *Config) initParamIfNeeded(ctx context.Context, param *Parameter, resource *Resource, requiredType reflect.Type, view *View) error {
	if param == nil {
		return nil
	}

	if err := param.Init(ctx, view, resource, nil); err != nil {
		return err
	}

	if param.Schema.Type() != requiredType {
		return fmt.Errorf("parameter %v type missmatch, required parameter to be type of %v but was %v", param.Name, requiredType.String(), param.Schema.Type().String())
	}

	return nil
}
