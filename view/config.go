package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

const (
	FieldsQuery   = "_fields"
	OffsetQuery   = "_offset"
	LimitQuery    = "_limit"
	CriteriaQuery = "_criteria"
	OrderByQuery  = "_orderby"
	PageQuery     = "_page"
	ContentFormat = "_format"
	SyncFlag      = "viewSyncFlag"
)

var stringsType = reflect.TypeOf([]string{})

var QueryStateParameters = &Config{
	LimitParameter:         &state.Parameter{Name: "Limit", In: state.NewQueryLocation(LimitQuery), Schema: state.NewSchema(xreflect.IntType)},
	OffsetParameter:        &state.Parameter{Name: "Offset", In: state.NewQueryLocation(OffsetQuery), Schema: state.NewSchema(xreflect.IntType)},
	PageParameter:          &state.Parameter{Name: "Page", In: state.NewQueryLocation(PageQuery), Schema: state.NewSchema(xreflect.IntType)},
	FieldsParameter:        &state.Parameter{Name: "Fields", In: state.NewQueryLocation(FieldsQuery), Schema: state.NewSchema(stringsType)},
	OrderByParameter:       &state.Parameter{Name: "OrderBy", In: state.NewQueryLocation(OrderByQuery), Schema: state.NewSchema(stringsType)},
	CriteriaParameter:      &state.Parameter{Name: "Criteria", In: state.NewQueryLocation(OrderByQuery), Schema: state.NewSchema(xreflect.StringType)},
	SyncFlagParameter:      &state.Parameter{Name: "SyncFlag", In: state.NewParameterLocation(SyncFlag), Schema: state.NewSchema(boolType)},
	ContentFormatParameter: &state.Parameter{Name: "ContentFormat", In: state.NewQueryLocation(ContentFormat), Schema: state.NewSchema(xreflect.StringType)},
}

// Config represent a View config selector
type (
	Config struct {
		//TODO: Should order by be a slice?
		Namespace         string             `json:",omitempty"`
		OrderBy           string             `json:",omitempty"`
		Limit             int                `json:",omitempty"`
		Constraints       *Constraints       `json:",omitempty"`
		Parameters        *SelectorParameter `json:",omitempty"`
		LimitParameter    *state.Parameter   `json:",omitempty"`
		OffsetParameter   *state.Parameter   `json:",omitempty"`
		PageParameter     *state.Parameter   `json:",omitempty"`
		FieldsParameter   *state.Parameter   `json:",omitempty"`
		OrderByParameter  *state.Parameter   `json:",omitempty"`
		CriteriaParameter *state.Parameter   `json:",omitempty"`

		//Settings parameters
		SyncFlagParameter      *state.Parameter `json:",omitempty"`
		ContentFormatParameter *state.Parameter `json:",omitempty"`

		limitDefault    *bool
		offsetDefault   *bool
		pageDefault     *bool
		fieldsDefault   *bool
		criteriaDefault *bool
		orderByDefault  *bool
	}

	SelectorParameter struct {
		Limit    string `json:",omitempty"`
		Offset   string `json:",omitempty"`
		Page     string `json:",omitempty"`
		Fields   string `json:",omitempty"`
		OrderBy  string `json:",omitempty"`
		Criteria string `json:",omitempty"`
		SyncFlag string `json:",omitempty"`
	}
)

func (c *Config) GetSyncFlagParameter() *state.Parameter {
	if c.SyncFlagParameter != nil {
		return c.SyncFlagParameter
	}
	return QueryStateParameters.SyncFlagParameter
}

func (c *Config) GetContentFormatParameter() *state.Parameter {
	if c.ContentFormatParameter != nil {
		return c.ContentFormatParameter
	}
	return QueryStateParameters.ContentFormatParameter
}

func (c *Config) Init(ctx context.Context, resource *Resource, parent *View) error {
	if err := c.ensureConstraints(resource); err != nil {
		return err
	}

	parameters := c.Parameters
	if parameters == nil {
		parameters = &SelectorParameter{}
	}

	if name := parameters.Limit; (name != "" || c.Constraints.Limit) && derefBool(c.limitDefault, c.LimitParameter == nil) {
		c.limitDefault = boolPtr(name == "")
		c.LimitParameter = c.newSelectorParam(name, LimitQuery, parent)
	}

	if name := parameters.Offset; (name != "" || c.Constraints.Offset) && derefBool(c.offsetDefault, c.OffsetParameter == nil) {
		c.offsetDefault = boolPtr(name == "")
		c.OffsetParameter = c.newSelectorParam(name, OffsetQuery, parent)
	}

	if name := parameters.Page; (name != "" || c.Constraints.IsPageEnabled()) && derefBool(c.pageDefault, c.PageParameter == nil) {
		c.pageDefault = boolPtr(name == "")
		c.PageParameter = c.newSelectorParam(name, PageQuery, parent)
	}

	if name := parameters.Fields; (name != "" || c.Constraints.Projection) && derefBool(c.fieldsDefault, c.FieldsParameter == nil) {
		c.fieldsDefault = boolPtr(name == "")
		c.FieldsParameter = c.newSelectorParam(name, FieldsQuery, parent)
	}

	if name := parameters.Criteria; (name != "" || c.Constraints.Criteria) && derefBool(c.criteriaDefault, c.CriteriaParameter == nil) {
		c.criteriaDefault = boolPtr(name == "")
		c.CriteriaParameter = c.newSelectorParam(name, CriteriaQuery, parent)
	}

	if name := parameters.OrderBy; (name != "" || c.Constraints.OrderBy) && derefBool(c.orderByDefault, c.OrderByParameter == nil) {
		c.orderByDefault = boolPtr(name == "")
		c.OrderByParameter = c.newSelectorParam(name, OrderByQuery, parent)
	}

	if name := parameters.SyncFlag; (name != "") && derefBool(c.fieldsDefault, c.SyncFlagParameter == nil) {
		c.fieldsDefault = boolPtr(name == "")
		c.SyncFlagParameter = c.newSelectorParam(name, SyncFlag, parent)
	}

	if err := c.initCustomParams(ctx, resource, parent); err != nil {
		return err
	}

	return nil
}

func (c *Config) newSelectorParam(name, paramKind string, parent *View) *state.Parameter {
	return &state.Parameter{
		Name:        shared.FirstNotEmpty(name, paramKind),
		In:          state.NewQueryLocation(shared.FirstNotEmpty(name, c.Namespace+paramKind)),
		Schema:      state.NewSchema(ParamType(paramKind)),
		Description: Description(paramKind, parent.Name),
	}
}

func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}

	result := *c
	return &result
}

func (c *Config) ensureConstraints(resource *Resource) error {
	if c.Constraints == nil {
		c.Constraints = &Constraints{}
	}

	return c.Constraints.init(resource)
}

func (c *Config) initCustomParams(ctx context.Context, resource *Resource, parent *View) error {
	if err := c.initParamIfNeeded(ctx, c.CriteriaParameter, resource, parent, xreflect.StringType, reflect.TypeOf(&codec.Criteria{}), reflect.TypeOf(codec.Criteria{})); err != nil {
		return err
	}
	if err := c.initParamIfNeeded(ctx, c.LimitParameter, resource, parent, xreflect.IntType); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.OrderByParameter, resource, parent, stringsType); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.OffsetParameter, resource, parent, xreflect.IntType); err != nil {
		return err
	}

	if err := c.initParamIfNeeded(ctx, c.FieldsParameter, resource, parent, stringsType); err != nil {
		return err
	}
	if err := c.initParamIfNeeded(ctx, c.PageParameter, resource, parent, xreflect.IntType); err != nil {
		return err
	}

	return nil
}

func (c *Config) initParamIfNeeded(ctx context.Context, param *state.Parameter, resource *Resource, view *View, requiredTypes ...reflect.Type) error {
	if param == nil {
		return nil
	}

	if param.Name == "" {
		param.Name = param.Ref
	}
	aResource := &Resourcelet{View: view, Resource: resource}
	if err := param.Init(ctx, aResource); err != nil {
		return err
	}

	if err := c.paramTypeMatches(param, requiredTypes); err != nil {
		return err
	}

	return nil
}

func (c *Config) paramTypeMatches(param *state.Parameter, requiredTypes []reflect.Type) error {
	paramType := param.OutputType()
	for _, requiredType := range requiredTypes {
		if paramType == requiredType {
			return nil
		}
	}

	supportedTypes := []string{}
	for _, requiredType := range requiredTypes {
		supportedTypes = append(supportedTypes, requiredType.String())
	}
	return fmt.Errorf("parameter %v type missmatch, required parameter to be type of %v but was %v", param.Name, strings.Join(supportedTypes, ", "), param.Schema.Type().String())
}

func (c *Config) CloneWithNs(ctx context.Context, resource *Resource, owner *View, ns string) (*Config, error) {
	shallowCopy := *c
	shallowCopy.Namespace = ns
	copyRef := &shallowCopy
	return copyRef, copyRef.Init(ctx, resource, owner)
}

func ParamType(name string) reflect.Type {
	switch name {
	case LimitQuery, OffsetQuery, PageQuery:
		return xreflect.IntType
	case OrderByQuery, FieldsQuery:
		return stringsType
	default:
		return xreflect.StringType
	}
}

func Description(paramName, viewName string) string {
	switch paramName {
	case LimitQuery:
		return fmt.Sprintf("allows to limit %v View data returned from db", viewName)
	case OffsetQuery:
		return fmt.Sprintf("allows to skip first n  View %v records, it has to be used alongside the limit", viewName)
	case CriteriaQuery:
		return fmt.Sprintf("allows to filter View %v data that matches given criteria", viewName)
	case FieldsQuery:
		return fmt.Sprintf("allows to control View %v fields present in response", viewName)
	case OrderByQuery:
		return fmt.Sprintf("allows to sort View %v results", viewName)
	case PageQuery:
		return fmt.Sprintf("allows to skip first page * limit values, starting from 1 page. Has precedence over offset")
	}

	return ""
}

func boolPtr(value bool) *bool {
	return &value
}

func derefBool(value *bool, onNil bool) bool {
	if value == nil {
		return onNil
	}

	return *value
}
