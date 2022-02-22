package data

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/datly/config"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
)

//Resource represents grouped data needed to build the View
//can be loaded from i.e. yaml file
type Resource struct {
	Connectors  []*config.Connector
	_connectors config.Connectors

	Views  []*View
	_views Views

	Parameters  []*Parameter
	_parameters Parameters

	types Types
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
func (r *Resource) GetConnectors() config.Connectors {
	if len(r.Connectors) == 0 {
		r._connectors = config.Connectors{}
		for i, connector := range r.Connectors {
			r._connectors[connector.Name] = r.Connectors[i]
		}
	}
	return r._connectors
}

//Init initializes Resource
func (r *Resource) Init(ctx context.Context) error {
	r._views = ViewSlice(r.Views).Index()
	r._connectors = config.ConnectorSlice(r.Connectors).Index()

	if err := config.ConnectorSlice(r.Connectors).Init(ctx, r._connectors); err != nil {
		return err
	}

	if err := ViewSlice(r.Views).Init(ctx, r); err != nil {
		return err
	}

	return nil
}

//View returns View with given name
func (r *Resource) View(name string) (*View, error) {
	return r._views.Lookup(name)
}

//NewResourceFromURL loads and initializes Resource from file .yaml
func NewResourceFromURL(ctx context.Context, url string, types Types) (*Resource, error) {
	fs := afs.New()
	data, err := fs.DownloadWithURL(ctx, url)
	if err != nil {
		return nil, err
	}

	transient := map[string]interface{}{}
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

	resource.types = types
	resource._parameters = ParametersSlice(resource.Parameters).Index()
	err = resource.Init(ctx)

	return resource, err
}
