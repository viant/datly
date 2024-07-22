package repository

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/gmetric"
	"github.com/viant/scy/auth/custom"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
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
	ignorePlugin         bool
	extensions           *extension.Registry
	resources            Resources
	namedResources       []string
	refreshFrequency     time.Duration
	apiPrefix            string
	useColumns           *bool
	metrics              *gmetric.Service
	transforms           marshal.TransformIndex
	dispatcher           func(registry *Registry) contract.Dispatcher
	cacheConnectorPrefix string
	path                 *path.Path
	jWTVerifier          *verifier.Service
	customAuth           *custom.Service
	jwtSigner            *signer.Service
	types                []*view.PackagedType
	resource             state.Resource
	constants            map[string]string
	substitutes          map[string]view.Substitutes
}

func (o *Options) UseColumn() bool {
	if o.useColumns == nil {
		return false
	}
	return *o.useColumns
}

func (o *Options) ensureRegistry() {
	if o.extensions == nil {
		o.extensions = extension.NewRegistry()
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

	if len(o.types) > 0 {
		for _, aType := range o.types {
			_ = o.extensions.Types.Register(aType.TypeName(), xreflect.WithReflectType(aType.Type))
		}
	}
}

func (o *Options) ensurePluginURL() {
	if o.ignorePlugin {
		return
	}
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

func NewOptions(opts []Option) *Options {
	ret := &Options{}
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

// WithExtensions returns extension option
func WithExtensions(registry *extension.Registry) Option {
	return func(o *Options) {
		o.extensions = registry
	}
}

// WithNoPlugin returns with no plugin option
func WithNoPlugin() Option {
	return func(o *Options) {
		o.ignorePlugin = true
	}
}

func WithResources(resources Resources) Option {
	return func(o *Options) {
		o.resources = resources
	}
}

func WithNamedResources(names ...string) ComponentOption {
	return func(c *Component) error {
		c.with = names
		return nil
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

func WithComponentURL(componentURL string) Option {
	return func(o *Options) {
		o.componentURL = componentURL
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

func WithJWTSigner(aSigner *signer.Config) Option {
	return func(o *Options) {
		o.jwtSigner = signer.New(aSigner)
		_ = o.jwtSigner.Init(context.Background())
	}
}

func WithPackageTypes(types ...*view.PackagedType) Option {
	return func(o *Options) {
		o.types = types
	}
}

func WithCustomAuth(auth *custom.Service) Option {
	return func(o *Options) {
		o.customAuth = auth
	}
}

func WithJWTVerifier(aVerifier *verifier.Config) Option {
	return func(o *Options) {
		jwtVerifier := verifier.New(aVerifier)
		o.jWTVerifier = jwtVerifier
		_ = jwtVerifier.Init(context.Background())
	}
}

func WithResource(resource *view.Resource) ComponentOption {
	return func(c *Component) error {
		if c.View.GetResource() == nil {
			c.View.SetResource(&view.Resource{})
		}
		res := c.View.GetResource()
		res.MergeFrom(resource, c.types)
		return nil
	}
}

func WithConstants(key string, value string) Option {
	return func(o *Options) {
		if o.constants == nil {
			o.constants = make(map[string]string)
		}
		o.constants[key] = value
	}
}

func WithSubstitutes(name string, substitutes map[string]string) Option {
	return func(o *Options) {
		if len(o.substitutes) == 0 {
			o.substitutes = map[string]view.Substitutes{}
		}
		o.substitutes[name] = substitutes
	}
}

func WithIgnorePlugin(flag bool) Option {
	return func(o *Options) {
		o.ignorePlugin = flag
	}
}
