package router

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/datly/data"
	"github.com/viant/datly/visitor"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
)

type Resource struct {
	APIURI    string
	SourceURL string
	Routes    Routes
	Resource  *data.Resource
	_visitors visitor.Visitors
}

func (r *Resource) Init(ctx context.Context) error {
	if err := r.Resource.Init(ctx, r.Resource.GetTypes(), r._visitors); err != nil {
		return err
	}

	for _, route := range r.Routes {
		if err := route.Init(ctx, r); err != nil {
			return err
		}
	}

	return nil
}

func NewResourceFromURL(ctx context.Context, fs afs.Service, url string, visitors visitor.Visitors, types data.Types) (*Resource, error) {
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

	resource._visitors = visitors
	resource.Resource.SetTypes(types)
	if err := resource.Init(ctx); err != nil {
		return nil, err
	}
	return resource, err
}
