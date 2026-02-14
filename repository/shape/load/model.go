package load

import "github.com/viant/datly/repository/shape/plan"
import "github.com/viant/datly/repository/shape/typectx"

// Component is a shape-loaded runtime-neutral component artifact.
// It intentionally avoids repository package coupling to keep shape/load reusable.
type Component struct {
	Name        string
	URI         string
	Method      string
	RootView    string
	Views       []string
	TypeContext *typectx.Context

	Input  []*plan.State
	Output []*plan.State
	Meta   []*plan.State
	Async  []*plan.State
	Other  []*plan.State
}
