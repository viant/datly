package plan

import "github.com/viant/datly/repository/shape"

// ShapeSpecKind implements shape.PlanSpec.
func (r *Result) ShapeSpecKind() string { return "plan" }

// ResultFrom extracts the typed plan result from a PlanResult.
// Returns (nil, false) when a is nil or contains an unexpected concrete type.
func ResultFrom(a *shape.PlanResult) (*Result, bool) {
	if a == nil {
		return nil, false
	}
	r, ok := a.Plan.(*Result)
	return r, ok && r != nil
}
