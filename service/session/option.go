package session

import (
	"context"
	"embed"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/service/auth"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/handler/logger"
)

type (
	Options struct {
		state               *view.State
		resource            state.Resource
		namespacedView      view.NamespacedView
		kindLocator         *locator.KindLocator
		namedParameters     state.NamedParameters
		locatorOptions      []locator.Option //resousrce, route level options
		locatorOpt          *locator.Options
		codecOptions        []codec.Option
		types               []*state.Type
		registry            *repository.Registry
		component           *repository.Component
		logger              logger.Logger
		operate             func(ctx context.Context, aSession *Session, aComponent *repository.Component) (interface{}, error)
		indirectState       bool
		reportNotAssignable *bool
		scope               string
		embeddedFS          *embed.FS
		auth                *auth.Service
		preseedCache        bool
	}

	Option func(o *Options)
)

func (o *Options) Logger() logger.Logger {
	return o.logger
}

func (o *Options) Registry() *repository.Registry {
	return o.registry
}

func (o *Options) HasInputParameters() bool {
	if o.locatorOpt == nil {
		return false
	}
	return len(o.locatorOpt.InputParameters) > 0
}
func (o *Options) shallReportNotAssignable() bool {
	if o.reportNotAssignable == nil {
		return true
	}
	return *o.reportNotAssignable
}
func (o *Options) Indirect(flag bool, options ...locator.Option) *Options {
	ret := *o
	ret.indirectState = flag
	ret.locatorOptions = append(ret.locatorOptions, locator.WithLogger(o.logger))
	if len(options) > 0 {
		ret.locatorOptions = append(ret.locatorOptions, options...)
		ret.kindLocator = locator.NewKindsLocator(ret.kindLocator, ret.locatorOptions...)
	}
	ret.locatorOpt = locator.NewOptions(ret.locatorOptions)
	return &ret
}

func (o *Options) State() *view.State {
	return o.state
}

func (o *Options) Operate() func(ctx context.Context, aSession *Session, aComponent *repository.Component) (interface{}, error) {
	return o.operate
}

func (o *Options) AddLocator(option locator.Option) {
	o.locatorOptions = append(o.locatorOptions, option)
}

func (o *Options) AddLocators(options ...locator.Option) {
	o.locatorOptions = append(o.locatorOptions, options...)
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
		s.locatorOpt = locator.NewOptions(options)
	}
}

func WithStateResource(resource state.Resource) Option {
	return func(s *Options) {
		s.resource = resource
	}
}

func WithCodeOptions(options ...codec.Option) Option {
	return func(s *Options) {
		s.codecOptions = options
	}
}

func WithReportNotAssignable(flag bool) Option {
	return func(s *Options) {
		s.reportNotAssignable = &flag
	}
}

func WithTypes(types ...*state.Type) Option {
	return func(s *Options) {
		s.types = append(s.types, types...)
	}
}

func WithAuth(auth *auth.Service) Option {
	return func(s *Options) {
		s.auth = auth
	}
}

// WithPreseedCache controls whether NewSession should pre-seed child cache from parent (default false)
func WithPreseedCache(flag bool) Option {
	return func(s *Options) {
		s.preseedCache = flag
	}
}

func WithComponent(component *repository.Component) Option {
	return func(s *Options) {
		s.component = component
	}
}

func WithEmbeddedFS(fs *embed.FS) Option {
	return func(s *Options) {
		s.embeddedFS = fs
	}
}

func WithScope(scope string) Option {
	return func(s *Options) {
		s.scope = scope
	}
}

func WithOperate(operate func(ctx context.Context, aSession *Session, aComponent *repository.Component) (interface{}, error)) Option {
	return func(s *Options) {
		s.operate = operate
	}
}

func WithRegistry(registry *repository.Registry) Option {
	return func(s *Options) {
		s.registry = registry
	}
}

func WithLogger(logger logger.Logger) Option {
	return func(s *Options) {
		s.logger = logger
	}
}
