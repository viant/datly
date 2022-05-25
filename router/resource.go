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
		Compression *Compression
		Redirect    *Redirect
		Cache       *cache.Cache
		Logger      *Logger //connect, dataview, time, SQL with params if exceeded time
		Cors        *Cors   //TODO github.com/viant/datly/v0/app/lambda/bridge/cors.go

		_visitors visitor.Visitors
	}

	Logger struct {
		MinExecutionMs int
	}

	Compression struct {
		MinSizeKb int
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
