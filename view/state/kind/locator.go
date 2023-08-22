package kind

import "github.com/viant/datly/view/state"

// Locator defines state locator
type Locator interface {

	//Value returns parameter value
	Value(name string) (interface{}, bool, error)

	//Names returns names of supported parameters
	Names() []string
}

// Locators defines state value kind locators
type Locators interface {
	Lookup(kind state.Kind) Locator
}
