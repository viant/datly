package session

import "github.com/viant/datly/view/state/kind/locator"

type Option func(s *State)

// Options merges multi options
func Options(opts ...[]Option) []Option {
	var result []Option
	for _, item := range opts {
		if len(item) == 0 {
			continue
		}
		result = append(result, item...)
	}
	return result
}

func WithLocatorOptions(opts ...locator.Option) Option {
	return func(s *State) {
		s.locatorOptions = opts
	}
}

func WithLocators(locators *locator.Locators) Option {
	return func(s *State) {
		s.Locators = locators
	}
}
