package load

import (
	"github.com/viant/datly/repository/shape"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
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

// ComponentFrom extracts the typed component from a ComponentArtifact.
// Returns (nil, false) when a is nil or contains an unexpected concrete type.
func ComponentFrom(a *shape.ComponentArtifact) (*Component, bool) {
	if a == nil {
		return nil, false
	}
	c, ok := a.Component.(*Component)
	return c, ok && c != nil
}
