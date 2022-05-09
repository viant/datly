package router

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/data"
	"github.com/viant/datly/visitor"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
)

type (
	Resource struct {
		initialised      bool
		APIURI           string
		SourceURL        string
		With             []string //list of resource to inherit from
		Routes           Routes
		Resource         *data.Resource
		_visitors        visitor.Visitors
		Compression      *Compression
		Redirect         *Redirect
		Cache            *Cache
		Logger           *Logger //connect, dataview, time, SQL with params if exceeded time
		EnableCORSHeader bool    //TODO github.com/viant/datly/v0/app/aws/bridge/cors.go

	}

	Cache struct {
		TimeToLiveMs int
		StorageURL   string
	}

	Logger struct {
		MinExecutionMs int
	}

	Compression struct {
		MinSizeKb int //github.com/viant/datly/v0/app/aws/lambda/compress.go
	}

	Redirect struct {
		StorageURL string ///github.com/viant/datly/v0/app/aws/lambda/proxy.go
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

func NewResourceFromURL(ctx context.Context, fs afs.Service, url string, visitors visitor.Visitors, types data.Types, resources map[string]*data.Resource, metrics *data.Metrics) (*Resource, error) {
	resourceData, err := fs.DownloadWithURL(ctx, url)
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

	resource := &Resource{}
	err = toolbox.DefaultConverter.AssignConverted(resource, aMap)
	if err != nil {
		return nil, err
	}

	if err = mergeResources(resource, resources, types); err != nil {
		return nil, err
	}
	resource._visitors = visitors
	resource.Resource.Metrics = metrics
	resource.Resource.SetTypes(types)
	if err := resource.Init(ctx); err != nil {
		return nil, err
	}
	return resource, err
}

func mergeResources(resource *Resource, resources map[string]*data.Resource, types data.Types) error {
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
