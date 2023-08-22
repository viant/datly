package session

import "github.com/viant/datly/view/state/kind/locator"

type Option func(s *Session)

func WithLocatorOptions(opts ...locator.Option) Option {
	return func(s *Session) {
		s.locatorOptions = opts
	}
}
