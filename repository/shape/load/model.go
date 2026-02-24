package load

import "github.com/viant/datly/repository/shape/plan"
import dqlshape "github.com/viant/datly/repository/shape/dql/shape"
import "github.com/viant/datly/repository/shape/typectx"
import "github.com/viant/datly/view"

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
