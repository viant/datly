package reader

import (
	"reflect"

	"github.com/viant/datly/spec"
)

const ViewDependencyKind = "view"

// ViewDependency is one immutable, typed independent-view read compiled from a
// component input binding.
type ViewDependency struct {
	Name       string
	TargetType reflect.Type
	Component  *spec.Component
	InputType  reflect.Type
	Plan       *Plan
}
