package contract

import (
	"context"
	"fmt"
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

	// BodySelector deprecated,  use output parameter instead
	//deprecated
	BodySelector struct {
		StateValue string
	}
)

const (
	BasicStyle         Style = "Basic"
	ComprehensiveStyle Style = "Comprehensive"
)

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
	if err = c.Output.Init(ctx, aView, c.Input.Body.Parameters, c.Service == service.TypeReader); err != nil {
		return err
	}
	if err := c.adjustInputType(ctx, aView, resource); err != nil {
		return err
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

func (r *Contract) initServiceType(path *Path) error {
	switch r.Service {
	case "", service.TypeReader:
		r.Service = service.TypeReader
		return nil
	case service.TypeExecutor:
		return nil
	}

	switch path.Method {
	case http.MethodGet:
		r.Service = service.TypeReader
		return nil
	default:
		return fmt.Errorf("http method %v unsupported, no default service specified for given method", path.Method)
	}
}

func (r *Contract) initCardinality() error {
	switch r.Output.Cardinality {
	case state.One, state.Many:
		return nil
	case "":
		r.Output.Cardinality = state.Many
		return nil
	default:
		return fmt.Errorf("unsupported cardinality type %v\n", r.Output.Cardinality)
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
