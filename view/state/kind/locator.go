package kind

import (
	"context"
	"github.com/viant/datly/view/state"
)

// Locator defines state locator
type Locator interface {

	//Output returns parameter value
	Value(ctx context.Context, name string) (interface{}, bool, error)

	//Names returns names of supported parameters
	Names() []string
}

// KindLocator defines state value kind locators
type KindLocator interface {
	Lookup(kind state.Kind) (Locator, error)
}
