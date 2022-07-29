package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/selector"
	"reflect"
	"strings"
)

var intType = reflect.TypeOf(0)
var stringType = reflect.TypeOf("")

//Config represent a view config selector
type (
	Config struct {
		//TODO: Should order by be a slice?
		OrderBy       string             `json:",omitempty"`
		Limit         int                `json:",omitempty"`
		Constraints   *Constraints       `json:",omitempty"`
		Parameters    *SelectorParameter `json:",omitempty"`
		LimitParam    *Parameter         `json:",omitempty"`
		OffsetParam   *Parameter         `json:",omitempty"`
		PageParam     *Parameter         `json:",omitempty"`
		FieldsParam   *Parameter         `json:",omitempty"`
		OrderByParam  *Parameter         `json:",omitempty"`
		CriteriaParam *Parameter         `json:",omitempty"`
	}

	SelectorParameter struct {
		Limit    string `json:",omitempty"`
		Offset   string `json:",omitempty"`
		Page     string `json:",omitempty"`
		Fields   string `json:",omitempty"`
		OrderBy  string `json:",omitempty"`
		Criteria string `json:",omitempty"`
	}
)

func (c *Config) ParameterName(ns, paramName string) string {
	if c.Parameters == nil {
		return ns + paramName
	}
	var result = ""
	if ns != "" && strings.HasPrefix(paramName, ns) {
		paramName = paramName[len(ns):]
	}
	switch strings.ToLower(paramName) {
	case selector.Fields:
		result = c.Parameters.Fields
	case selector.Offset:
		result = c.Parameters.Offset
	case selector.OrderBy:
		result = c.Parameters.OrderBy
	case selector.Limit:
		result = c.Parameters.Limit
	case selector.Criteria:
		result = c.Parameters.Criteria
	case selector.Page:
		result = c.Parameters.Page
	}
	if result == "" {
		return ns + paramName
	}
	return result
}

func (c *Config) Init(ctx context.Context, resource *Resource, parent *View) error {
	if err := c.ensureConstraints(resource); err != nil {
		return err
	}

	if params := c.Parameters; params != nil {

		if name := params.Limit; name != "" {
			c.LimitParam = &Parameter{Name: name, In: NewQueryLocation(name), Schema: NewSchema(selector.ParamType(selector.Limit)), Description: selector.Description(selector.Limit, parent.Name)}
		}
		if name := params.Offset; name != "" {
			c.OffsetParam = &Parameter{Name: name, In: NewQueryLocation(name), Schema: NewSchema(selector.ParamType(selector.Offset)), Description: selector.Description(selector.Offset, parent.Name)}
		}
		if name := params.Page; name != "" {
			c.PageParam = &Parameter{Name: name, In: NewQueryLocation(name), Schema: NewSchema(selector.ParamType(selector.Page)), Description: selector.Description(selector.Page, parent.Name)}
		}
		if name := params.Fields; name != "" {
			c.FieldsParam = &Parameter{Name: name, In: NewQueryLocation(name), Schema: NewSchema(selector.ParamType(selector.Fields)), Description: selector.Description(selector.Fields, parent.Name)}
		}
		if name := params.Criteria; name != "" {
			c.CriteriaParam = &Parameter{Name: name, In: NewQueryLocation(name), Schema: NewSchema(selector.ParamType(selector.Criteria)), Description: selector.Description(selector.Criteria, parent.Name)}
		}
		if name := params.OrderBy; name != "" {
			c.OrderByParam = &Parameter{Name: name, In: NewQueryLocation(name), Schema: NewSchema(selector.ParamType(selector.OrderBy)), Description: selector.Description(selector.OrderBy, parent.Name)}
		}
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
