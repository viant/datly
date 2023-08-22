package locator

import (
	"github.com/viant/datly/view/state/kind"
)

// NewLocator new locators
type NewLocator func(options ...Option) (kind.Locator, error)
