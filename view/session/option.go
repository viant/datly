package session

import (
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/codec"
)

type (
	Options struct {
		selectors      *view.Selectors
		namespacedView *view.NamespacedView
		kindLocator    *locator.KindLocator
		parameters     state.NamedParameters
		locatorOptions []locator.Option //resousrce, route level options
		codecOptions   []codec.Option
	}
	Option func(o *Options)
)

func (o *Options) AddLocator(option locator.Option) {
	o.locatorOptions = append(o.locatorOptions, option)
}
func (o *Options) AddCodec(option codec.Option) {
	o.codecOptions = append(o.codecOptions, option)
}

func (o *Options) Clone() *Options {
	ret := *o
	return &ret
}

func NewOptions(options ...Option) *Options {
	ret := &Options{}
	ret.apply(options)
	return ret
}

// AsOptions merges multi options
func AsOptions(opts ...[]Option) []Option {
	var result []Option
	for _, item := range opts {
		if len(item) == 0 {
			continue
		}
		result = append(result, item...)
	}
	return result
}

func WithLocators(locators *locator.KindLocator) Option {
	return func(s *Options) {
		s.kindLocator = locators
	}
}

func WithLocatorOptions(options ...locator.Option) Option {
	return func(s *Options) {
		s.locatorOptions = options
	}
}

func WithCodeOptions(options ...codec.Option) Option {
	return func(s *Options) {
		s.codecOptions = options
	}
}
