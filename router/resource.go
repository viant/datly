package router

import (
	"bytes"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/option/content"
	"github.com/viant/afs/url"
	"github.com/viant/datly/codec"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/router/cache"
	"github.com/viant/datly/router/openapi3"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"net/http"
	"strings"
	"time"
)

type (
	Resource struct {
		initialised  bool
		APIURI       string
		MetaCacheURI string
		SourceURL    string
		With         []string //list of resource to inherit from
		Routes       Routes
		Resource     *view.Resource
		Compression  *Compression
		Redirect     *Redirect
		Cache        *cache.Cache
		Logger       *Logger //connect, dataview, time, SQL with params if exceeded time
		Cors         *Cors

		ColumnsCache     *discover.Cache
		ParamStatusError *int

		Info             openapi3.Info
		_visitors        codec.Visitors
		ColumnsDiscovery bool
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

func (r *Redirect) Apply(ctx context.Context, viewName string, payload []byte, encoding string, response http.ResponseWriter) (*option.PreSign, error) {
	fs := afs.New()
	UUID := uuid.New()
	URL := url.Join(r.StorageURL, normalizeStorageURL(viewName), normalizeStorageURL(UUID.String())) + ".json"
	preSign := option.NewPreSign(r.TimeToLive())
	kv := []string{content.Type, ContentTypeJSON}
	if encoding != "" {
		kv = append(kv, content.Encoding, encoding)
	}
	meta := content.NewMeta(kv...)
	err := fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(payload), preSign, meta)
	return preSign, err
}

func (r *Resource) Init(ctx context.Context) error {
	if r.initialised {
		return nil
	}
	r.initialised = true

	var columnCacheExists bool
	if r.ColumnsDiscovery {
		r.ColumnsCache = discover.New(strings.Replace(r.SourceURL, ".yaml", "_meta.yml", 1))
		if columnCacheExists = r.ColumnsCache.Exists(ctx); columnCacheExists {
			if err := r.ColumnsCache.Load(ctx); err != nil {
				return err
			}
		}
	}

	columnsCache := map[string]view.Columns{}
	if r.ColumnsDiscovery {
		columnsCache = r.ColumnsCache.Items
	}

	if err := r.Resource.Init(ctx, r.Resource.GetTypes(), r._visitors, columnsCache); err != nil {
		return err
	}

	for _, route := range r.Routes {
		if route.ParamStatusError == nil {
			route.ParamStatusError = r.ParamStatusError
		}
		
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

func NewResourceFromURL(ctx context.Context, fs afs.Service, URL string, visitors codec.Visitors, types view.Types, resources map[string]*view.Resource, metrics *view.Metrics, useColumnCache bool) (*Resource, error) {
	resourceData, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}

	transient := map[string]interface{}{}
	if err := yaml.Unmarshal(resourceData, &transient); err != nil {
		return nil, err
	}

	aMap := map[string]interface{}{}
	if err := yaml.Unmarshal(resourceData, &aMap); err != nil {
		return nil, err
	}

	resource := &Resource{SourceURL: URL}
	err = toolbox.DefaultConverter.AssignConverted(resource, aMap)
	if err != nil {
		return nil, err
	}
	if resource.Resource == nil {
		return nil, fmt.Errorf("resource was empty: %v", URL)
	}
	if err = mergeResources(resource, resources, types); err != nil {
		return nil, err
	}
	resource.SourceURL = URL
	resource._visitors = visitors
	resource.Resource.Metrics = metrics
	resource.Resource.SourceURL = URL
	resource.Resource.SetTypes(types)
	resource.ColumnsDiscovery = useColumnCache

	object, _ := fs.Object(ctx, URL)
	resource.Resource.ModTime = object.ModTime()

	if err := resource.Init(ctx); err != nil {
		return nil, err
	}
	return resource, err
}

func mergeResources(resource *Resource, resources map[string]*view.Resource, types view.Types) error {
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
