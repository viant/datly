package router

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/url"
	"github.com/viant/datly/config"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/gateway/router/openapi3"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/toolbox"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"gopkg.in/yaml.v3"
	"strings"
	"time"
)

type (
	Resource struct {
		URL          string   `json:",omitempty" yaml:",omitempty"`
		MetaCacheURI string   `json:",omitempty"`
		SourceURL    string   `json:",omitempty"`
		With         []string //list of resource to inherit from  `json:",omitempty"`
		Routes       Routes
		Compression  *Compression `json:",omitempty"`
		Redirect     *Redirect    `json:",omitempty"`
		Logger       *Logger      `json:",omitempty"` //connect, dataview, time, SQL with params if exceeded time  `json:",omitempty"`
		Cors         *Cors        `json:",omitempty"`

		ColumnsCache *discover.Columns `json:",omitempty"`
		RevealMetric *bool             `json:",omitempty"`

		Info openapi3.Info `json:",omitempty"`

		ColumnsDiscovery bool              `json:",omitempty"`
		EnableDebug      *bool             `json:",omitempty"`
		Interceptor      *RouteInterceptor `json:",omitempty"`

		_codecs      *codec.Registry
		_initialised bool

		fs       afs.Service
		Resource *view.Resource
	}

	Logger struct {
		MinExecutionMs *int
	}

	Compression struct {
		MinSizeKb int
	}

	Redirect struct {
		StorageURL   string ///github.com/viant/datly/v0/app/lambda/lambda/proxy.go
		MinSizeKb    int
		TimeToLiveMs int
	}
)

func normalizeStorageURL(part string) string {
	part = strings.ReplaceAll(part, "-", "")
	part = strings.ReplaceAll(part, "_", "")
	return part
}

func (r *Redirect) TimeToLive() time.Duration {
	return time.Duration(r.TimeToLiveMs) * time.Millisecond
}

func (r *Redirect) Apply(ctx context.Context, viewName string, payload PayloadReader) (*option.PreSign, error) {
	fs := afs.New()
	UUID := uuid.New()
	URL := url.Join(r.StorageURL, normalizeStorageURL(viewName), normalizeStorageURL(UUID.String())) + ".json"
	preSign := option.NewPreSign(r.TimeToLive())
	kv := []string{content.Type, httputils.ContentTypeJSON}
	compressionType := payload.CompressionType()

	if compressionType != "" {
		kv = append(kv, content.Encoding, compressionType)
	}
	meta := content.NewMeta(kv...)
	err := fs.Upload(ctx, URL, file.DefaultFileOsMode, payload, preSign, meta)
	return preSign, err
}

func (r *Resource) Init(ctx context.Context) error {
	if r._initialised {
		return nil
	}
	transforms := marshal.TransformIndex{}
	for _, route := range r.Routes {
		if err := route.normalizePaths(); err != nil {
			return err
		}
		if route.View.Ref == "" {
			continue
		}
		route.Transforms, transforms[route.View.Ref] = r.filterTransforms(route)
	}
	r._initialised = true
	var columnCacheExists bool
	if r.ColumnsDiscovery {
		parent, name := url.Split(r.SourceURL, file.Scheme)
		metaURL := url.Join(parent, ".meta", name)
		r.ColumnsCache = discover.New(metaURL, r.fs)
		if columnCacheExists = r.ColumnsCache.Exists(ctx); columnCacheExists {
			if err := r.ColumnsCache.Load(ctx); err != nil {
				return err
			}
			r.ColumnsCache.SourceURL = metaURL
		}
	}

	for _, route := range r.Routes {
		route._resource = r.Resource
		if route.Output.RevealMetric == nil {
			route.Output.RevealMetric = r.RevealMetric
		}
		route.EnableDebug = r.EnableDebug
	}

	columnsCache := map[string]view.Columns{}
	if r.ColumnsDiscovery {
		columnsCache = r.ColumnsCache.Items
	}

	if err := r.Resource.Init(ctx, r.Resource.TypeRegistry(), r._codecs, columnsCache, transforms, config.Config.Predicates); err != nil {
		return err
	}

	for _, route := range r.Routes {
		if err := route.Init(ctx, r); err != nil {
			return err
		}
	}

	if err := r.addLoggersIfNeeded(); err != nil {
		return err
	}

	if r.ColumnsDiscovery && (!columnCacheExists || r.Resource.ModTime.After(r.ColumnsCache.ModTime)) {
		r.ColumnsCache.ModTime = r.Resource.ModTime
		if err := r.ColumnsCache.Store(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (r *Resource) filterTransforms(route *Route) (routeTransforms marshal.Transforms, viewTransforms marshal.Transforms) {
	for _, transform := range route.Transforms {
		if transform.ParamName == "" {
			viewTransforms = append(viewTransforms, transform)
		} else {
			routeTransforms = append(routeTransforms, transform)
		}
	}

	return routeTransforms, viewTransforms
}

func (r *Resource) addLoggersIfNeeded() error {
	if r.Logger == nil {
		return nil
	}

	if r.Logger.MinExecutionMs == nil {
		return fmt.Errorf("unspecified logger MinExecutionMs")
	}

	duration := time.Millisecond * (time.Duration(*r.Logger.MinExecutionMs))
	timeLogger := logger.NewLogger("TimeLogger", logger.NewTimeLogger(duration, duration))

	for _, aRoute := range r.Routes {
		r.addLogger(aRoute.View, timeLogger)
	}

	return nil
}

func (r *Resource) addLogger(aView *view.View, timeLogger *logger.Adapter) {
	if aView.Logger != nil {
		aView.Logger = timeLogger
	}
	for _, relation := range aView.With {
		r.addLogger(&relation.Of.View, timeLogger)
	}
}

func NewResourceFromURL(ctx context.Context, fs afs.Service, URL string, useColumnCache bool, options ...interface{}) (*Resource, error) {
	resource, err := LoadResource(ctx, fs, URL, useColumnCache, options...)
	if err != nil {
		return nil, err
	}

	if err := resource.Init(ctx); err != nil {
		return nil, err
	}
	resource.Resource.SetFs(fs)
	return resource, err
}

func LoadResource(ctx context.Context, fs afs.Service, URL string, useColumnCache bool, options ...interface{}) (*Resource, error) {
	visitors, types, resources, metrics := readOptions(options)
	resourceData, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}

	aMap := map[string]interface{}{}
	if err := yaml.Unmarshal(resourceData, &aMap); err != nil {
		return nil, err
	}

	resource := &Resource{SourceURL: URL, fs: fs}
	err = toolbox.DefaultConverter.AssignConverted(resource, aMap)
	if err != nil {
		return nil, err
	}

	resource.fs = fs
	if resource.Resource == nil {
		return nil, fmt.Errorf("resource was empty: %v", URL)
	}

	if err = mergeResources(resource, resources, types); err != nil {
		return nil, err
	}
	resource.SourceURL = URL
	resource._codecs = visitors
	resource.Resource.Metrics = metrics
	resource.Resource.SourceURL = URL
	resource.Resource.SetTypes(types)
	resource.ColumnsDiscovery = useColumnCache
	object, _ := fs.Object(ctx, URL)
	resource.Resource.ModTime = object.ModTime()
	return resource, nil
}

func readOptions(options []interface{}) (*codec.Registry, *xreflect.Types, map[string]*view.Resource, *view.Metrics) {
	var codecRegistry *codec.Registry
	var resources map[string]*view.Resource
	var metrics *view.Metrics
	var types *xreflect.Types
	for _, anOption := range options {
		switch actual := anOption.(type) {
		case *codec.Registry:
			codecRegistry = actual
		case *xreflect.Types:
			types = actual
		case map[string]*view.Resource:
			resources = actual
		case *view.Metrics:
			metrics = actual
		case *config.Registry:
			types = actual.Types
			codecRegistry = actual.Codecs
		}
	}

	if resources == nil {
		resources = map[string]*view.Resource{}
	}
	if codecRegistry == nil {
		codecRegistry = codec.NewRegistry()
	}
	if types == nil {
		types = xreflect.NewTypes(xreflect.WithRegistry(config.Config.Types))
	}
	return codecRegistry, types, resources, metrics
}

func mergeResources(resource *Resource, resources map[string]*view.Resource, types *xreflect.Types) error {
	if len(resource.With) > 0 {
		for _, ref := range resource.With {
			refResource, ok := resources[ref]
			if !ok {
				return fmt.Errorf("invalid 'with' resource ref: %v", ref)
			}
			resource.Resource.MergeFrom(refResource, types)
		}
	}
	return nil
}