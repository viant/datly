package locator

import "github.com/viant/datly/view/state"

// NewLocator new locators
type NewLocator func(options ...Option) (state.Locator, error)
