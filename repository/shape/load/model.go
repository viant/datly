package load

import (
	"reflect"

	"github.com/viant/datly/repository/shape"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/xreflect"
)

// Component is a shape-loaded runtime-neutral component artifact.
// It intentionally avoids repository package coupling to keep shape/load reusable.
type Component struct {
	Name             string
	URI              string
	Method           string
	RootView         string
	Views            []string
	Relations        []*plan.Relation
	ViewRelations    []*view.Relation
	Declarations     map[string]*plan.ViewDeclaration
	QuerySelectors   map[string][]string
	Predicates       map[string][]*plan.ViewPredicate
	TypeContext      *typectx.Context
	Directives       *dqlshape.Directives
	ColumnsDiscovery bool

	Input  []*plan.State
	Output []*plan.State
	Meta   []*plan.State
	Async  []*plan.State
	Other  []*plan.State
}

// ShapeSpecKind implements shape.ComponentSpec.
func (c *Component) ShapeSpecKind() string { return "component" }

// InputParameters returns input states as state.Parameters for type generation.
func (c *Component) InputParameters() state.Parameters {
	if c == nil {
		return nil
	}
	var result state.Parameters
	for _, s := range c.Input {
		if s != nil {
			p := s.Parameter
			result = append(result, &p)
		}
	}
	return result
}

// OutputParameters returns output states as state.Parameters for type generation.
func (c *Component) OutputParameters() state.Parameters {
	if c == nil {
		return nil
	}
	var result state.Parameters
	for _, s := range c.Output {
		if s != nil {
			p := s.Parameter
			result = append(result, &p)
		}
	}
	return result
}

// InputReflectType builds the Input struct reflect.Type using state.Parameters.ReflectType.
// This produces the same struct shape as the legacy codegen (with parameter tags, Has markers, etc.).
func (c *Component) InputReflectType(pkgPath string, lookupType xreflect.LookupType, opts ...state.ReflectOption) (reflect.Type, error) {
	params := c.InputParameters()
	if len(params) == 0 {
		return nil, nil
	}
	return params.ReflectType(pkgPath, lookupType, opts...)
}

// OutputReflectType builds the Output struct reflect.Type using state.Parameters.ReflectType.
func (c *Component) OutputReflectType(pkgPath string, lookupType xreflect.LookupType, opts ...state.ReflectOption) (reflect.Type, error) {
	params := c.OutputParameters()
	if len(params) == 0 {
		return nil, nil
	}
	return params.ReflectType(pkgPath, lookupType, opts...)
}

// ComponentFrom extracts the typed component from a ComponentArtifact.
// Returns (nil, false) when a is nil or contains an unexpected concrete type.
func ComponentFrom(a *shape.ComponentArtifact) (*Component, bool) {
	if a == nil {
		return nil, false
	}
	c, ok := a.Component.(*Component)
	return c, ok && c != nil
}
