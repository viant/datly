package view

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/datly/config"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/toolbox"
	"github.com/viant/xreflect"
	"gopkg.in/yaml.v3"
	"reflect"
	"strings"
	"time"
)

//Resource represents grouped view needed to build the View
//can be loaded from i.e. yaml file
type Resource struct {
	Metrics   *Metrics
	SourceURL string `json:",omitempty"`

	CacheProviders []*Cache
	_cacheIndex    map[string]int

	Connectors  []*Connector
	_connectors Connectors

	Views  []*View `json:",omitempty"`
	_views Views

	Parameters  []*Parameter `json:",omitempty"`
	_parameters ParametersIndex

	Types         []*TypeDefinition
	_types        Types
	_packageTypes map[string]Types
	_typesIndex   map[reflect.Type]string

	Loggers  logger.Adapters `json:",omitempty"`
	_loggers logger.AdapterIndex

	_visitors config.CodecsRegistry
	ModTime   time.Time `json:",omitempty"`

	_columnsCache map[string]Columns
	_typeLookup   xreflect.TypeLookupFn
}

func (r *Resource) LoadText(ctx context.Context, URL string) (string, error) {
	if url.Scheme(URL, "") == "" && r.SourceURL != "" {
		parent, _ := url.Split(r.SourceURL, file.Scheme)
		URL = url.Join(parent, URL)
	}

	fs := afs.New()
	data, err := fs.DownloadWithURL(ctx, URL)

	if err = r.updateTime(ctx, URL, err); err != nil {
		return "", err
	}

	return string(data), err
}

func (r *Resource) updateTime(ctx context.Context, URL string, err error) error {
	if !strings.HasSuffix(URL, ".sql") {
		return nil
	}

	object, err := r.LoadObject(ctx, URL)
	if err != nil {
		return err
	}

	if object.ModTime().After(r.ModTime) {
		r.ModTime = object.ModTime()
	}

	return nil
}

func (r *Resource) LoadObject(ctx context.Context, URL string) (storage.Object, error) {
	if url.Scheme(URL, "") == "" && r.SourceURL != "" {
		parent, _ := url.Split(r.SourceURL, file.Scheme)
		URL = url.Join(parent, URL)
	}

	fs := afs.New()
	data, err := fs.Object(ctx, URL)
	return data, err
}

func (r *Resource) MergeFrom(resource *Resource, types Types) {
	r.mergeViews(resource)
	r.mergeParameters(resource)
	r.mergeTypes(resource, types)
	r.mergeConnectors(resource)
	r.mergeProviders(resource)
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
			connector := *resource.Connectors[i]
			r.Connectors = append(r.Connectors, &connector)
		}
	}
}

func (r *Resource) mergeParameters(resource *Resource) {
	if len(resource.Parameters) == 0 {
		return
	}
	views := r.paramByName()
	for i, candidate := range resource.Parameters {
		if _, ok := views[candidate.Name]; !ok {
			param := *resource.Parameters[i]
			r.Parameters = append(r.Parameters, &param)
		}
	}
}

func (r *Resource) mergeTypes(resource *Resource, types Types) {
	if len(resource.Types) == 0 {
		return
	}
	views := r.typeByName()
	for i, candidate := range resource.Types {
		if _, ok := types[candidate.Name]; ok {
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

func (r *Resource) ConnectorByName() map[string]*Connector {
	index := map[string]*Connector{}
	if len(r.Connectors) == 0 {
		return index
	}
	for i, item := range r.Connectors {
		index[item.Name] = r.Connectors[i]
	}
	return index
}

func (r *Resource) paramByName() map[string]*Parameter {
	index := map[string]*Parameter{}
	if len(r.Parameters) == 0 {
		return index
	}
	for i, param := range r.Parameters {
		index[param.Name] = r.Parameters[i]
	}
	return index
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

//GetViews returns Views supplied with the Resource
func (r *Resource) GetViews() Views {
	if len(r._views) == 0 {
		r._views = Views{}
		for i, view := range r.Views {
			r._views[view.Name] = r.Views[i]
		}
	}
	return r._views
}

//GetConnectors returns Connectors supplied with the Resource
func (r *Resource) GetConnectors() Connectors {
	if len(r.Connectors) == 0 {
		r._connectors = Connectors{}
		for i, connector := range r.Connectors {
			r._connectors[connector.Name] = r.Connectors[i]
		}
	}
	return r._connectors
}

//Init initializes Resource
func (r *Resource) Init(ctx context.Context, options ...interface{}) error {

	types, visitors, cache, transforms := r.readOptions(options)
	r.indexProviders()

	r._typesIndex = map[reflect.Type]string{}
	r._types = types.copy()
	r._visitors = visitors
	r._columnsCache = cache
	r._packageTypes = map[string]Types{}

	for _, definition := range r.Types {
		if err := definition.Init(ctx, r.LookupType); err != nil {
			return err
		}

		if err := r.registerType(definition.Package, definition.Name, definition.Type()); err != nil {
			return err
		}
	}

	var err error
	r._views, err = ViewSlice(r.Views).Index()
	if err != nil {
		return err
	}

	r._connectors = ConnectorSlice(r.Connectors).Index()
	r._parameters, err = ParametersSlice(r.Parameters).Index()
	if err != nil {
		return err
	}

	r._loggers = r.Loggers.Index()

	if err = ConnectorSlice(r.Connectors).Init(ctx, r._connectors); err != nil {
		return err
	}

	if err = ViewSlice(r.Views).Init(ctx, r, transforms); err != nil {
		return err
	}

	return nil
}

func (r *Resource) registerType(packageName, typeName string, rType reflect.Type) error {
	typesIndex := r._types
	typeIndexName := typeName

	if packageName != "" {
		index, ok := r._packageTypes[packageName]
		if !ok {
			index = map[string]reflect.Type{}
			r._packageTypes[packageName] = index
		}

		typesIndex = index
		typeIndexName = packageName + "." + typeName
	}

	typesIndex.Register(typeName, rType)
	r._typesIndex[rType] = typeIndexName

	return nil
}

func (r *Resource) readOptions(options []interface{}) (Types, config.CodecsRegistry, map[string]Columns, marshal.TransformIndex) {
	var types = Types{}
	var visitors = config.CodecsRegistry{}
	var cache map[string]Columns
	var transformsIndex marshal.TransformIndex

	for _, option := range options {
		if option == nil {
			continue
		}
		switch actual := option.(type) {
		case config.CodecsRegistry:
			visitors = actual
		case map[string]Columns:
			cache = actual
		case Types:
			types = actual
		case marshal.TransformIndex:
			transformsIndex = actual
		}
	}

	return types, visitors, cache, transformsIndex
}

//View returns View with given name
func (r *Resource) View(name string) (*View, error) {
	return r._views.Lookup(name)
}

//NewResourceFromURL loads and initializes Resource from file .yaml
func NewResourceFromURL(ctx context.Context, url string, types Types, visitors config.CodecsRegistry) (*Resource, error) {
	resource, err := LoadResourceFromURL(ctx, url, afs.New())
	if err != nil {
		return nil, err
	}
	err = resource.Init(ctx, types, visitors, map[string]Columns{})
	return resource, err
}

//LoadResourceFromURL load resource from URL
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

	resource.SourceURL = URL
	resource.ModTime = object.ModTime()
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
			return r._connectors.Lookup(view.Connector.Ref)
		}

		if err := view.Connector.Validate(); err == nil {
			return view.Connector, nil
		}
	}

	return nil, fmt.Errorf("couldn't inherit connector for view %v from any other parent views", view.Name)
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
		Connectors:  make([]*Connector, 0),
		_connectors: Connectors{},
		Views:       make([]*View, 0),
		_views:      Views{},
		Parameters:  make([]*Parameter, 0),
		_parameters: ParametersIndex{},
		_types:      Types{},
		_typesIndex: map[reflect.Type]string{},
	}
}

//NewResource creates a Resource and register provided Types
func NewResource(types Types) *Resource {
	return &Resource{_types: types}
}

//AddViews register views in the resource
func (r *Resource) AddViews(views ...*View) {
	if r.Views == nil {
		r.Views = make([]*View, 0)
	}

	r.Views = append(r.Views, views...)
}

//AddConnectors register connectors in the resource
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

//AddParameters register parameters in the resource
func (r *Resource) AddParameters(parameters ...*Parameter) {
	if r.Parameters == nil {
		r.Parameters = make([]*Parameter, 0)
	}

	r.Parameters = append(r.Parameters, parameters...)
}

//AddLoggers register loggers in the resource
func (r *Resource) AddLoggers(loggers ...*logger.Adapter) {
	r.Loggers = append(r.Loggers, loggers...)
}

func (r *Resource) SetTypes(types Types) {
	r._types = types
}

func (r *Resource) GetTypes() Types {
	return r._types
}

func (r *Resource) TypeName(p reflect.Type) (string, bool) {
	name, ok := r._typesIndex[p]
	return name, ok
}

func (r *Resource) CodecByName(name string) (config.BasicCodec, bool) {
	codec, err := r._visitors.LookupCodec(name)
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
	lookup, err := r._connectors.Lookup(name)
	return lookup != nil && err == nil
}

func (r *Resource) Connector(name string) (*Connector, error) {
	if r._connectors == nil {
		r._connectors = ConnectorSlice(r.Connectors).Index()
	}

	return r._connectors.Lookup(name)
}

func (r *Resource) SetTypeLookup(lookup xreflect.TypeLookupFn) {
	r._typeLookup = lookup
}

func (r *Resource) LookupType(packageIdentifier, packageName, typeName string) (reflect.Type, error) {
	if r._typeLookup != nil {
		lookup, err := r._typeLookup(packageIdentifier, packageName, typeName)
		if err == nil {
			return lookup, nil
		}
	}

	lookup, err := r._types.Lookup(typeName)
	if err == nil {
		return lookup, err
	}

	if packageName != "" {
		registry, ok := r._packageTypes[packageName]
		if !ok {
			return nil, fmt.Errorf("didn't found package %v", packageName)
		}

		rType, ok := registry[typeName]
		if ok {
			return rType, nil
		}

		return nil, r.typeNotFound(packageName, typeName)
	}

	rType, _ := r._types.Lookup(typeName)
	for _, registry := range r._packageTypes {
		packageType := registry[typeName]
		if packageType == nil {
			continue
		}

		if rType != nil && packageType != nil && rType != packageType {
			return nil, fmt.Errorf("ambigious type %v, please specify package name", typeName)
		}

		if rType == nil {
			rType = packageType
		}
	}

	if rType != nil {
		return rType, nil
	}

	return nil, r.typeNotFound(packageName, typeName)
}

func (r *Resource) typeNotFound(packageName string, typeName string) error {
	if packageName == "" {
		return fmt.Errorf("not found type %v", typeName)
	}

	return fmt.Errorf("not found type %v under %v package", typeName, packageName)
}
