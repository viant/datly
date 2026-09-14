package reader

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

// SelectorBindingPlan maps an already-bound input field to a view selector property.
// Bindly owns value resolution; this plan only describes reader interpretation.
type SelectorBindingPlan struct {
	View       *data.View
	Property   spec.SelectorProperty
	FieldIndex []int
}
