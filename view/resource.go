package view

import (
	"context"
	"embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/async/mbus"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/docs"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xreflect"
	"gopkg.in/yaml.v3"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	//ResourceConnectors default connector resource name
	ResourceConnectors = "connectors"
	ResourceConstants  = "constants"
	ResourceMBus       = "mbus"
)

// Resource represents grouped View needed to build the View
// can be loaded from i.e. yaml file
type (
	Resource struct {
		Metrics   *Metrics
		SourceURL string `json:",omitempty"`

		CacheProviders []*Cache
		_cacheIndex    map[string]int

		Connectors  []*Connector
		_connectors Connectors

		MessageBuses  []*mbus.Resource
		_messageBuses MessageBuses

		Views  Views `json:",omitempty"`
		_views NamedViews

		Parameters  state.Parameters `json:",omitempty"`
		_parameters state.NamedParameters

		Types  []*TypeDefinition
		_types *xreflect.Types

		Imports Imports

		Loggers  logger.Adapters `json:",omitempty"`
		_loggers logger.AdapterIndex

		codecs *codec.Registry

		Predicates  []*predicate.Template
		_predicates *extension.PredicateRegistry

		viewColumns map[string]Columns

		Substitutes Substitutes
		Docs        *Documentation
		FSEmbedder  *state.FSEmbedder

		modTime time.Time
		_doc    docs.Service
		fs      afs.Service
		_docs   *docs.Registry
		_mux    sync.RWMutex
	}

	NamedResources map[string]*Resource

	ResourceOptions struct {
		Codecs          *codec.Registry
		Columns         map[string]Columns
		Types           *xreflect.Types
		TransformsIndex marshal.TransformIndex
		Predicates      *extension.PredicateRegistry
		Docs            *docs.Registry
		Metrics         *Metrics
	}

	Import struct {
		Alias      string `json:",omitempty"`
		ModulePath string `json:",omitempty"`
	}

	Imports map[string]*Import
)

func (i Imports) Add(modulePath string) {
	_, alias := path.Split(modulePath)
	i[alias] = &Import{alias, modulePath}
}

func (i Imports) AddWithAlias(alias, modulePath string) {
	i[alias] = &Import{alias, modulePath}
}

func (r *Resource) RepositoryURL() string {
	if index := strings.Index(r.SourceURL, "/routes/"); index != -1 {
		return r.SourceURL[:index]
	}
	return ""
}

func (r *Resource) Lock() {
	r._mux.Lock()
}

func (r *Resource) Unlock() {
	r._mux.Unlock()
}

func (r *Resource) NamedParameters() state.NamedParameters {
	return r._parameters
}

func (r *Resource) LookupParameter(name string) (*state.Parameter, error) {
	return r._parameters.Lookup(name)
}

func (r *Resource) TypeRegistry() *xreflect.Types {
	return r._types
}

func (r *Resource) LookupType() xreflect.LookupType {
	return r._types.Lookup
}

func (r *Resource) ExpandSubstitutes(text string) string {
	r._mux.RLock()
	defer r._mux.RUnlock()
	return r.Substitutes.Replace(text)
}
func (r *Resource) ReverseSubstitutes(text string) string {
	r._mux.RLock()
	defer r._mux.RUnlock()
	return r.Substitutes.ReverseReplace(text)
}

func (r *Resource) SetFs(fs afs.Service) {
	r.fs = fs
}

func (r *Resource) LoadText(ctx context.Context, URL string) (string, error) {
	data, err := r.loadText(ctx, URL, true)
	if err != nil {
		return "", err
	}
	data = r.ExpandSubstitutes(data)
	return data, nil
}

func (r *Resource) loadText(ctx context.Context, URL string, expand bool) (string, error) {
	if url.Scheme(URL, "") == "" && r.SourceURL != "" {
		parent, _ := url.Split(r.SourceURL, file.Scheme)
		URL = url.Join(parent, URL)
	}
	fs := r.fs
	if fs == nil {
		fs = afs.New()
	}
	var embedFs *embed.FS
	if r.FSEmbedder != nil {
		embedFs = r.FSEmbedder.EmbedFS()
	}
	data, err := fs.DownloadWithURL(ctx, URL, embedFs)

	if err = r.UpdateTime(ctx, URL); err != nil {
		return "", err
	}

	return string(data), err
}

func (r *Resource) ModTime() time.Time {
	return r.modTime
}

func (r *Resource) UpdateTime(ctx context.Context, URL string) error {
	if !strings.HasSuffix(URL, ".sql") || URL == "" {
		return nil
	}
	object, err := r.LoadObject(ctx, URL)
	if err != nil {
		return err
	}
	if object.ModTime().After(r.modTime) {
		r.modTime = object.ModTime()
	}
	return nil
}

func (r *Resource) LoadObject(ctx context.Context, URL string) (storage.Object, error) {
	if url.Scheme(URL, "") == "" && r.SourceURL != "" {
		parent, _ := url.Split(r.SourceURL, file.Scheme)
		URL = url.Join(parent, URL)
	}
	var embedFs *embed.FS
	if r.FSEmbedder != nil {
		embedFs = r.FSEmbedder.EmbedFS()
	}
	fs := afs.New()
	data, err := fs.Object(ctx, URL, embedFs)
	return data, err
}

func (r *Resource) MergeFrom(resource *Resource, types *xreflect.Types) {
	r.mergeSubstitutes(resource)
	r.mergeViews(resource)
	r.mergeParameters(resource)
	r.mergeTypes(resource, types)
	r.mergeConnectors(resource)
	r.mergeMessageBuses(resource)
	r.mergeProviders(resource)
	r.mergePredicates(resource)
	r.mergeMetrics(resource)
}

func (r *Resource) mergeSubstitutes(resource *Resource) {
	r.Lock()
	defer r.Unlock()
	if len(resource.Substitutes) == 0 {
		return
	}
	if len(r.Substitutes) == 0 {
		r.Substitutes = make(Substitutes)
	}

	for k, v := range resource.Substitutes {
		r.Substitutes[k] = v
	}
}

func (r *Resource) mergeViews(resource *Resource) {
	if len(resource.Views) == 0 {
		return
	}
	views := r.viewByName()
	for i, candidate := range resource.Views {
		if _, ok := views[candidate.Name]; !ok {
			view := *resource.Views[i]
			r.Views = append(r.Views, &view)
		}
	}
}

func (r *Resource) mergeConnectors(resource *Resource) {
	if len(resource.Connectors) == 0 {
		return
	}
	connectors := r.ConnectorByName()
	for i, candidate := range resource.Connectors {
		if _, ok := connectors[candidate.Name]; !ok {
			connector := *resource.Connectors[i] //TODO why clone ?
			r.Connectors = append(r.Connectors, &connector)
		}
	}
}

func (r *Resource) mergeParameters(resource *Resource) {
	if len(resource.Parameters) == 0 {
		return
	}
	namedParameters := r.Parameters.Index()
	byKindNameParameters := r.Parameters.ByKindName()
	for i, candidate := range resource.Parameters {
		if _, ok := byKindNameParameters[candidate.In.Name]; ok {
			byKindNameParameters[candidate.In.Name].Value = candidate.Value
			continue
		}
		_, ok := namedParameters[candidate.Name]
		if !ok {
			param := *resource.Parameters[i]
			r.Parameters = append(r.Parameters, &param)
		} else {
			namedParameters[candidate.Name].Value = candidate.Value
		}

	}
}

func (r *Resource) mergeTypes(resource *Resource, types *xreflect.Types) {
	if len(resource.Types) == 0 {
		return
	}
	views := r.typeByName()

	for i, candidate := range resource.Types {
		if types.Has(candidate.TypeName()) {
			continue
		}
		if _, ok := views[candidate.Name]; !ok {
			typeDef := *resource.Types[i]
			r.Types = append(r.Types, &typeDef)
		}
	}
}

func (r *Resource) viewByName() map[string]*View {
	index := map[string]*View{}
	if len(r.Views) == 0 {
		return index
	}
	for i, view := range r.Views {
		index[view.Name] = r.Views[i]
	}

	return index
}

func (r *Resource) ConnectorByName() Connectors {
	ConnectorSlice(r.Connectors).Index()
	index := map[string]*Connector{}
	if len(r.Connectors) == 0 {
		return index
	}
	for i, item := range r.Connectors {
		index[item.Name] = r.Connectors[i]
	}
	return index
}

func (r *Resource) MBusResourceByName() MessageBuses {
	return MessageBusSlice(r.MessageBuses).Index()
}

func (r *Resource) typeByName() map[string]*TypeDefinition {
	index := map[string]*TypeDefinition{}
	if len(r.Parameters) == 0 {
		return index
	}
	for i, param := range r.Types {
		index[param.Name] = r.Types[i]
	}
	return index
}

// GetViews returns NamedViews supplied with the Resource
func (r *Resource) GetViews() NamedViews {
	if len(r._views) == 0 {
		r._views = NamedViews{}
		for i, view := range r.Views {
			r._views[view.Name] = r.Views[i]
		}
	}
	return r._views
}

func (r *Resource) IndexViews() {
	r._views = r.Views.Index()
}

// GetConnectors returns Connectors supplied with the Resource
func (r *Resource) GetConnectors() Connectors {
	if len(r.Connectors) > len(r._connectors) {
		r._connectors = ConnectorSlice(r.Connectors).Index()
	}
	return r._connectors
}

// Init initializes Resource
func (r *Resource) Init(ctx context.Context, options ...interface{}) error {
	opts := r.readOptions(options)
	r.indexProviders()
	r.Metrics = opts.Metrics
	r.codecs = opts.Codecs
	r.viewColumns = opts.Columns
	r._types = opts.Types

	r.Imports = Imports{}
	for _, definition := range r.Types {
		if definition.ModulePath != "" {
			r.Imports.Add(definition.ModulePath)
			if definition.Package != "" {
				r.Imports.AddWithAlias(definition.Package, definition.ModulePath)
			}
		}
	}

	for _, definition := range r.Types {
		if err := definition.Init(ctx, opts.Types.Lookup, r.Imports); err != nil {
			return err
		}
		//DO we really need cluster global namespace
		if err := r.TypeRegistry().Register(definition.Name, xreflect.WithReflectType(definition.Type())); err != nil {
			return err
		}
		if err := r.TypeRegistry().Register(definition.Name, xreflect.WithPackage(definition.Package), xreflect.WithModulePath(definition.ModulePath), xreflect.WithReflectType(definition.Type())); err != nil {
			return err
		}
		if index := strings.LastIndex(definition.Package, "/"); index != -1 {
			pkg := definition.Package[index+1:]
			if err := r.TypeRegistry().Register(definition.Name, xreflect.WithPackage(pkg), xreflect.WithModulePath(definition.ModulePath), xreflect.WithReflectType(definition.Type())); err != nil {
				return err
			}
		}
		if definition.Alias != "" {
			if err := r.TypeRegistry().Register(definition.Alias, xreflect.WithReflectType(definition.Type())); err != nil {
				return err
			}
		}
	}

	if err := r.initPredicates(opts.Predicates); err != nil {
		return err
	}

	var err error
	r.IndexViews()
	r._connectors = ConnectorSlice(r.Connectors).Index()
	r._messageBuses = MessageBusSlice(r.MessageBuses).Index()
	r._parameters = r.Parameters.Index()
	if err != nil {
		return err
	}

	r._loggers = r.Loggers.Index()

	if err = ConnectorSlice(r.Connectors).Init(ctx, r._connectors); err != nil {
		return err
	}

	r.Views.EnsureResource(r)
	if err = r.Views.Init(ctx, r, opts.TransformsIndex); err != nil {
		return err
	}

	if err = r.initDocs(ctx); err != nil {
		return err
	}

	return nil
}

func (r *Resource) readOptions(options []interface{}) *ResourceOptions {
	result := &ResourceOptions{}
	for _, option := range options {
		if option == nil {
			continue
		}
		switch actual := option.(type) {
		case *codec.Registry:
			result.Codecs = actual
		case map[string]Columns:
			result.Columns = actual
		case *xreflect.Types:
			result.Types = actual
		case marshal.TransformIndex:
			result.TransformsIndex = actual
		case *extension.PredicateRegistry:
			result.Predicates = actual
		case extension.PredicateRegistry:
			result.Predicates = &actual
		case *Metrics:
			result.Metrics = actual
		case *extension.Registry:
			if result.Predicates == nil {
				result.Predicates = actual.Predicates
			}

			if result.Codecs == nil {
				result.Codecs = actual.Codecs
			}

			if result.Types == nil {
				result.Types = actual.Types
			}
		}
	}

	if r._types != nil {
		result.Types = r._types
	}

	if result.Codecs == nil {
		result.Codecs = extension.Config.Codecs
	}

	if result.Types == nil {
		result.Types = r.TypeRegistry()
	}

	if result.Docs == nil {
		result.Docs = extension.Config.Docs
	}

	return result
}

// View returns View with given name
func (r *Resource) View(name string) (*View, error) {
	ret, err := r._views.Lookup(name)
	if ret != nil || len(r.Views) == len(r._views) {
		return ret, err
	}
	r._views = r.Views.Index()
	return r._views.Lookup(name)
}

func (r *Resource) ViewSchemaPointer(ctx context.Context, name string) (*state.Schema, error) {
	aView, err := r.View(name)
	if err != nil {
		return nil, err
	}
	if aView.Schema == nil {
		aView.Schema = &state.Schema{}
	}
	return aView.Schema, nil
}

func (r *Resource) ViewSchema(ctx context.Context, name string) (*state.Schema, error) {
	aView, err := r.View(name)
	if err != nil {
		return nil, err
	}
	return aView.GetSchema(ctx)
}

// NewResourceFromURL loads and initializes Resource from file .yaml
func NewResourceFromURL(ctx context.Context, url string, types *xreflect.Types, codecs *codec.Registry) (*Resource, error) {
	resource, err := LoadResourceFromURL(ctx, url, afs.New())
	if err != nil {
		return nil, err
	}
	err = resource.Init(ctx, types, codecs, map[string]Columns{})
	return resource, err
}

// LoadResourceFromURL load resource from URL
func LoadResourceFromURL(ctx context.Context, URL string, fs afs.Service) (*Resource, error) {
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}

	transient := map[string]interface{}{}
	object, err := fs.Object(ctx, URL)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(data, &transient); err != nil {
		return nil, err
	}

	aMap := map[string]interface{}{}
	if err := yaml.Unmarshal(data, &aMap); err != nil {
		return nil, err
	}

	resource := &Resource{}
	err = toolbox.DefaultConverter.AssignConverted(resource, aMap)
	if err != nil {
		return nil, err
	}
	resource.fs = fs
	resource.SourceURL = URL
	resource.modTime = object.ModTime()
	return resource, err
}

func (r *Resource) FindConnector(view *View) (*Connector, error) {
	if view.Connector == nil {
		var connector *Connector

		for _, relView := range r.Views {
			if relView.Name == view.Name {
				continue
			}

			if isChildOfAny(view, relView.With) {
				connector = relView.Connector
				break
			}
		}

		if connector != nil {
			result := *connector
			return &result, nil
		}
	}

	if view.Connector == nil {
		if view.Ref != "" {
			if refView, _ := r.View(view.Ref); refView != nil {
				view.Connector = refView.Connector
			}
		}
	}

	if view.Connector != nil {
		if view.Connector.Ref != "" {
			return r.Connector(view.Connector.Ref)
		}

		if err := view.Connector.Validate(); err == nil {
			return view.Connector, nil
		}
	}

	return nil, fmt.Errorf("couldn't inherit connector for View %v from any other parent views", view.Name)
}

func isChildOfAny(view *View, with []*Relation) bool {
	for _, relation := range with {
		if relation.Of.View.Ref == view.Name {
			return true
		}

		if isChildOfAny(view, relation.Of.With) {
			return true
		}
	}

	return false
}

func EmptyResource() *Resource {
	return &Resource{
		Connectors:    make([]*Connector, 0),
		_connectors:   Connectors{},
		MessageBuses:  make([]*mbus.Resource, 0),
		_messageBuses: MessageBuses{},
		Views:         make([]*View, 0),
		_views:        NamedViews{},
		Parameters:    make([]*state.Parameter, 0),
		_parameters:   state.NamedParameters{},
		_types:        xreflect.NewTypes(),
	}
}

// NewResource creates a Resource and register provided Types
func NewResource(parent *xreflect.Types) *Resource {
	return &Resource{_types: xreflect.NewTypes(xreflect.WithRegistry(parent))}
}

// AddViews register views in the resource
func (r *Resource) AddViews(views ...*View) {
	if r.Views == nil {
		r.Views = make([]*View, 0)
	}

	r.Views = append(r.Views, views...)
}

// AddConnector adds connector
func (r *Resource) AddConnector(name string, driver string, dsn string, opts ...ConnectorOption) *Connector {
	connector := NewConnector(name, driver, dsn, opts...)
	r.AddConnectors(connector)
	return connector
}

// AddConnectors register connectors in the resource
func (r *Resource) AddConnectors(connectors ...*Connector) {
	if r.Connectors == nil {
		r.Connectors = make([]*Connector, 0)
		r._connectors = map[string]*Connector{}
	}

	for i, connector := range connectors {
		if _, ok := r._connectors[connector.Name]; ok {
			continue
		}

		r.Connectors = append(r.Connectors, connectors[i])
	}
}

func (r *Resource) AddMessageBus(messageBuses ...*mbus.Resource) {
	if len(r._messageBuses) == 0 {
		r.MessageBuses = make([]*mbus.Resource, 0)
		r._messageBuses = map[string]*mbus.Resource{}
	}

	for i, messageBus := range messageBuses {
		if _, ok := r._messageBuses[messageBus.ID]; ok {
			continue
		}
		r.MessageBuses = append(r.MessageBuses, messageBuses[i])
	}
}

// AddParameters register parameters in the resource
func (r *Resource) AddParameters(parameters ...*state.Parameter) {
	if r.Parameters == nil {
		r.Parameters = make([]*state.Parameter, 0)
	}

	r.Parameters = append(r.Parameters, parameters...)
}

// AddLoggers register loggers in the resource
func (r *Resource) AddLoggers(loggers ...*logger.Adapter) {
	r.Loggers = append(r.Loggers, loggers...)
}

func (r *Resource) SetTypes(types *xreflect.Types) {
	r._types = xreflect.NewTypes(xreflect.WithRegistry(types))
}

func (r *Resource) SetCodecs(codecs *codec.Registry) {
	r.codecs = codecs
}

func (r *Resource) TypeName(t reflect.Type) (string, bool) {
	info := r._types.Info(t)
	if info == nil {
		return "", false
	}
	return info.TypeName(), true
}

func (r *Resource) CodecByName(name string) (*codec.Codec, bool) {
	codec, err := r.codecs.Lookup(name)
	return codec, err == nil
}

func (r *Resource) CacheProvider(ref string) (*Cache, bool) {
	index, ok := r._cacheIndex[ref]
	if !ok {
		return nil, false
	}

	return r.CacheProviders[index], ok
}

func (r *Resource) indexProviders() {
	r.ensureCacheIndex()

	r._cacheIndex = map[string]int{}
	for i, provider := range r.CacheProviders {
		if provider.Name == "" {
			continue
		}

		r._cacheIndex[provider.Name] = i
	}
}

func (r *Resource) mergeProviders(resource *Resource) {
	r.ensureCacheIndex()

	if resource._cacheIndex == nil {
		resource._cacheIndex = map[string]int{}
	}

	for _, provider := range resource.CacheProviders {
		if _, ok := r.CacheProvider(provider.Name); ok {
			continue
		}

		r._cacheIndex[provider.Name] = len(r.CacheProviders)
		r.CacheProviders = append(r.CacheProviders, provider)
	}
}

func (r *Resource) ensureCacheIndex() {
	if r._cacheIndex == nil {
		r._cacheIndex = map[string]int{}
	}
}

func (r *Resource) ExistsConnector(name string) bool {
	lookup, err := r.Connector(name)
	return lookup != nil && err == nil
}

func (r *Resource) Connector(name string) (*Connector, error) {
	if len(r._connectors) == 0 {
		r._connectors = ConnectorSlice(r.Connectors).Index()
	}

	return r._connectors.Lookup(name)
}

func (r *Resource) MessageBus(name string) (*mbus.Resource, error) {
	if len(r._messageBuses) == 0 {
		r._messageBuses = MessageBusSlice(r.MessageBuses).Index()
	}
	return r._messageBuses.Lookup(name)
}

func (r *Resource) typeNotFound(packageName string, typeName string) error {
	if packageName == "" {
		return fmt.Errorf("not found type %v at Resource", typeName)
	}

	return fmt.Errorf("not found type %v under %v package", typeName, packageName)
}

func (r *Resource) ParamByName(name string) (*state.Parameter, error) {
	return r._parameters.Lookup(name)
}

func (r *Resource) mergeMessageBuses(resource *Resource) {
	if len(resource.MessageBuses) == 0 {
		return
	}
	messageBusByName := MessageBusSlice(r.MessageBuses).Index()
	for i, candidate := range resource.MessageBuses {
		if _, ok := messageBusByName[candidate.ID]; !ok {
			messageBus := *resource.MessageBuses[i]
			r.MessageBuses = append(r.MessageBuses, &messageBus)
		}
	}
}

func (r *Resource) initPredicates(registry *extension.PredicateRegistry) error {
	if registry != nil {
		r._predicates = registry.Scope()
	}
	r.ensurePredicateIndex()
	for _, aPredicate := range r.Predicates {
		r._predicates.Add(aPredicate)
	}
	return nil
}

func (r *Resource) ensurePredicateIndex() {
	if r._predicates == nil {
		r._predicates = extension.NewPredicates()
	}
}

func (r *Resource) mergePredicates(resource *Resource) {
	r.ensurePredicateIndex()
	for _, aPredicate := range resource.Predicates {
		r.addPredicate(aPredicate)
	}
}

func (r *Resource) addPredicate(aPredicate *predicate.Template) {
	r.Predicates = append(r.Predicates, aPredicate)
	r._predicates.Add(aPredicate)
}

func (r *Resource) initDocs(ctx context.Context) error {
	if r.Docs == nil {
		return nil
	}
	if r.Docs.BaseURL != "" && url.IsRelative(r.Docs.BaseURL) {
		r.Docs.BaseURL = path.Join(r.RepositoryURL(), r.Docs.BaseURL)
	}
	return r.Docs.Init(ctx, r.fs, r.Substitutes)
}

func (r *Resource) Doc() (docs.Service, bool) {
	return r._doc, r._doc != nil
}

func (r *Resource) mergeMetrics(resource *Resource) {
	r.Metrics = resource.Metrics
}
