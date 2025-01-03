package contract

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository/codegen"
	"github.com/viant/datly/service"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"net/http"
	"reflect"
)

const pkgPath = "github.com/viant/datly/gateway/router"

type (
	//Style defines style
	//TODO deprecate with function on input parameters to determine style
	Style string

	Contract struct {
		Name       string `json:",omitempty" yaml:",omitempty"`
		Input      Input
		Output     Output
		ModulePath string
		Service    service.Type `json:",omitempty"`
	}
)

// Types returns all types
func (c *Contract) Types() []*state.Type {
	var types []*state.Type
	if c.Input.Type.Type().IsDefined() {
		types = append(types, &c.Input.Type)
	}
	if c.Output.Type.Type().IsDefined() {
		types = append(types, &c.Output.Type)
	}
	return types
}
func (c *Contract) Init(ctx context.Context, path *Path, aView *view.View, resource *view.Resource) (err error) {
	if err = c.initServiceType(path); err != nil {
		return err
	}
	if err = c.initCardinality(); err != nil {
		return err
	}
	if err = c.Input.Init(ctx, aView); err != nil {
		return err
	}
	if err = c.Output.Init(ctx, aView, &c.Input.Body, c.Service == service.TypeReader); err != nil {
		return err
	}
	if !codegen.IsGeneratorContext(ctx) {
		if err := c.adjustInputType(ctx, aView, resource); err != nil {
			return err
		}
	}
	return nil
}

func (c *Contract) adjustInputType(ctx context.Context, aView *view.View, resource *view.Resource) error {
	if c.Input.Type.Schema.IsNamed() {
		return nil
	}
	localInput := c.Output.Type.Parameters.LocationInput()
	if len(localInput) == 0 {
		return nil
	}
	aResource := aView.Resource()
	namedParameters := resource.NamedParameters()
	for _, param := range localInput {
		if _, ok := namedParameters[param.Name]; !ok {
			resource.Parameters = append(resource.Parameters, param)
			namedParameters[param.Name] = param
		}
		_ = param.Init(ctx, aResource)
	}
	rType, err := c.Input.Type.Parameters.ReflectType(c.Input.Type.Package, aResource.LookupType(), state.WithLocationInput(localInput))
	if err != nil {
		return fmt.Errorf("invalid local input: %w", err)
	}
	c.Input.Type.SetType(rType)

	return nil
}

func (c *Contract) initServiceType(path *Path) error {
	switch c.Service {
	case "", service.TypeReader:
		c.Service = service.TypeReader
		return nil
	case service.TypeExecutor:
		return nil
	}

	switch path.Method {
	case http.MethodGet:
		c.Service = service.TypeReader
		return nil
	default:
		return fmt.Errorf("http method %v unsupported, no default service specified for given method", path.Method)
	}
}

func (c *Contract) initCardinality() error {
	switch c.Output.Cardinality {
	case state.One, state.Many:
		return nil
	case "":
		c.Output.Cardinality = state.Many
		return nil
	default:
		return fmt.Errorf("unsupported cardinality type %v\n", c.Output.Cardinality)
	}
}

func (c *Contract) BodyType() reflect.Type {
	if c.Input.Body.Schema == nil {
		return nil
	}
	rType := c.Input.Body.Schema.Type()
	if rType != nil && rType.Kind() == reflect.Map {
		return reflect.TypeOf(struct {
		}{})
	}
	return c.Input.Body.Schema.Type()
}

func (c *Contract) OutputType() reflect.Type {
	if c.Output.Type.Schema == nil {
		return nil
	}
	if parameter := c.Output.Type.AnonymousParameters(); parameter != nil {
		return parameter.OutputType()
	}
	return c.Output.Type.Schema.Type()
}
