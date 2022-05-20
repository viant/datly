package router

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/router/cache"
	"github.com/viant/datly/view"
	"github.com/viant/datly/visitor"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
)

type (
	Resource struct {
		initialised bool
		APIURI      string
		SourceURL   string
		With        []string //list of resource to inherit from
		Routes      Routes
		Resource    *view.Resource
		_visitors   visitor.Visitors
		Compression *Compression
		Redirect    *Redirect
		Cache       *cache.Cache
		Logger      *Logger //connect, dataview, time, SQL with params if exceeded time
		Cors        *Cors   //TODO github.com/viant/datly/v0/app/lambda/bridge/cors.go
		/*
			See https://cloud.google.com/functions/docs/writing/http#writing_http_content-go

				// CORSEnabledFunction is an example of setting CORS headers.
				// For more information about CORS and CORS preflight requests, see
				// https://developer.mozilla.org/en-US/docs/Glossary/Preflight_request.
				func CORSEnabledFunction(w http.ResponseWriter, r *http.Request) {
				        // Set CORS headers for the preflight request
				        if r.Method == http.MethodOptions {
				                w.Header().Set("Access-Control-Allow-Origin", "*")
				                w.Header().Set("Access-Control-Allow-Methods", "POST")
				                w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
				                w.Header().Set("Access-Control-Max-Age", "3600")
				                w.WriteHeader(http.StatusNoContent)
				                return
				        }
				        // Set CORS headers for the main request.
				        w.Header().Set("Access-Control-Allow-Origin", "*")
				        fmt.Fprint(w, "Hello, World!")
				}
		*/

	}

	Logger struct {
		MinExecutionMs int
	}

	Compression struct {
		MinSizeKb int //github.com/viant/datly/v0/app/lambda/lambda/compress.go
	}

	Redirect struct {
		StorageURL string ///github.com/viant/datly/v0/app/lambda/lambda/proxy.go
		MinSizeKb  int
	}
)

func (r *Resource) Init(ctx context.Context) error {
	if r.initialised {
		return nil
	}

	if err := r.Resource.Init(ctx, r.Resource.GetTypes(), r._visitors); err != nil {
		return err
	}

	for _, route := range r.Routes {
		if err := route.Init(ctx, r); err != nil {
			return err
		}
	}
	r.initialised = true
	return nil
}

func NewResourceFromURL(ctx context.Context, fs afs.Service, URL string, visitors visitor.Visitors, types view.Types, resources map[string]*view.Resource, metrics *view.Metrics) (*Resource, error) {
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
	if err = mergeResources(resource, resources, types); err != nil {
		return nil, err
	}
	resource.SourceURL = URL
	resource._visitors = visitors
	resource.Resource.Metrics = metrics
	resource.Resource.SourceURL = URL
	resource.Resource.SetTypes(types)
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
