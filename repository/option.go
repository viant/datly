package repository

import (
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

type Options struct {
	fs         afs.Service
	useColumns bool
	registry   *extension.Registry
	metrics    *view.Metrics
	resources  version.Resources
	transform  marshal.TransformIndex
}

func (o *Options) ensureRegistry() {
	if o.registry == nil {
		o.registry = &extension.Registry{}
	}
}

func (o *Options) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
	o.init()
}

func (o *Options) init() {
	if o.fs == nil {
		o.fs = afs.New()
	}
	o.ensureRegistry()
	if o.registry.Types == nil {
		o.registry.Types = extension.Config.Types
	}
	if o.registry.Codecs == nil {
		o.registry.Codecs = extension.Config.Codecs
	}
	if o.registry.Predicates == nil {
		o.registry.Predicates = extension.Config.Predicates
	}
}

func NewOptions(opts ...Option) *Options {
	ret := &Options{}
	ret.Apply(opts...)

	return ret
}

type Option func(o *Options)

func WithMetrics(metrics *view.Metrics) Option {
	return func(o *Options) {
		o.metrics = metrics
	}
}

func WithTypes(types *xreflect.Types) Option {
	return func(o *Options) {
		o.ensureRegistry()
		o.registry.Types = types
	}
}

func WithCodecs(codecs *codec.Registry) Option {
	return func(o *Options) {
		o.ensureRegistry()
		o.registry.Codecs = codecs
	}
}

func WithPredicates(predicates *extension.PredicateRegistry) Option {
	return func(o *Options) {
		o.ensureRegistry()
		o.registry.Predicates = predicates
	}
}

func WithNamedResources(resources version.Resources) Option {
	return func(o *Options) {
		o.resources = resources
	}
}
