package repository

import (
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/resource"
	"github.com/viant/datly/view/extension"
	"github.com/viant/gmetric"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"strings"
	"time"
)

type Options struct {
	fs                   afs.Service
	componentURL         string
	resourceURL          string
	pluginURL            string
	extensions           *extension.Registry
	resources            *resource.Service
	refreshFrequency     time.Duration
	apiPrefix            string
	useColumns           *bool
	metrics              *gmetric.Service
	transforms           marshal.TransformIndex
	dispatcher           func(registry *Registry) contract.Dispatcher
	cacheConnectorPrefix string
	path                 *path.Path
}

func (o *Options) UseColumn() bool {
	if o.useColumns == nil {
		return false
	}
	return *o.useColumns
}

func (o *Options) ensureRegistry() {
	if o.extensions == nil {
		o.extensions = &extension.Registry{}
	}
}

func (o *Options) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
	o.init()
}

var trueValue = true

func (o *Options) init() {
	if o.fs == nil {
		o.fs = afs.New()
	}
	o.ensureRegistry()
	if o.extensions.Types == nil {
		o.extensions.Types = extension.Config.Types
	}
	if o.extensions.Codecs == nil {
		o.extensions.Codecs = extension.Config.Codecs
	}
	if o.extensions.Predicates == nil {
		o.extensions.Predicates = extension.Config.Predicates
	}
	if o.refreshFrequency == 0 {
		o.refreshFrequency = 5 * time.Second
	}
	o.refreshFrequency = ensureFrequency(o.refreshFrequency)

	if o.apiPrefix == "" {
		o.apiPrefix = "/v1/api/"
	}
	if o.useColumns == nil {
		o.useColumns = &trueValue
	}
	o.ensureResourceURL()
	o.ensurePluginURL()
}

func (o *Options) ensurePluginURL() {
	if index := strings.LastIndex(o.resourceURL, "/dependencies"); index != -1 && o.pluginURL == "" {
		o.pluginURL = o.resourceURL[:index] + "/plugins"
	}
}

func (o *Options) ensureResourceURL() {
	if index := strings.LastIndex(o.componentURL, "/routes"); index != -1 && o.resourceURL == "" {
		o.resourceURL = o.componentURL[:index] + "/dependencies"
	}
}

func ensureFrequency(checkFrequency time.Duration) time.Duration {
	if checkFrequency <= time.Millisecond {
		checkFrequency = time.Second
	}
	return checkFrequency
}

func NewOptions(componentsURL string, opts ...Option) *Options {
	ret := &Options{componentURL: componentsURL}
	ret.Apply(opts...)

	return ret
}

type Option func(o *Options)

func WithMetrics(metrics *gmetric.Service) Option {
	return func(o *Options) {
		o.metrics = metrics
	}
}

func WithTypes(types *xreflect.Types) Option {
	return func(o *Options) {
		o.ensureRegistry()
		o.extensions.Types = types
	}
}

func WithCodecs(codecs *codec.Registry) Option {
	return func(o *Options) {
		o.ensureRegistry()
		o.extensions.Codecs = codecs
	}
}

func WithPredicates(predicates *extension.PredicateRegistry) Option {
	return func(o *Options) {
		o.ensureRegistry()
		o.extensions.Predicates = predicates
	}
}

func WithExtensions(registry *extension.Registry) Option {
	return func(o *Options) {
		o.extensions = registry
	}
}

func WithResources(resources *resource.Service) Option {
	return func(o *Options) {
		o.resources = resources
	}
}

func WithRefreshFrequency(refreshFrequency time.Duration) Option {
	return func(o *Options) {
		o.refreshFrequency = refreshFrequency
	}
}

func WithResourceURL(URL string) Option {
	return func(o *Options) {
		o.resourceURL = URL
	}

}

func WithCacheConnectorPrefix(prefix string) Option {
	return func(o *Options) {
		o.cacheConnectorPrefix = prefix
	}
}

func WithPluginURL(URL string) Option {
	return func(o *Options) {
		o.pluginURL = URL
	}
}

func WithApiPrefix(prefix string) Option {
	return func(o *Options) {
		o.apiPrefix = prefix
	}
}

func WithDispatcher(fn func(registry *Registry) contract.Dispatcher) Option {
	return func(o *Options) {
		o.dispatcher = fn
	}
}

func WithPath(aPath *path.Path) Option {
	return func(o *Options) {
		o.path = aPath
	}
}
