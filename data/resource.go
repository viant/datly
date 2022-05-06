package data

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/config"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/visitor"
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
	_parameters ParametersIndex

	Types  []*Definition
	_types Types

	Loggers  logger.Adapters
	_loggers logger.AdapterIndex

	_visitors visitor.Visitors
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
func (r *Resource) Init(ctx context.Context, types Types, visitors visitor.Visitors) error {
	r._types = types.copy()
	r._visitors = visitors

	for _, definition := range r.Types {
		if err := definition.Init(ctx, types); err != nil {
			return err
		}

		_, err := r._types.Lookup(definition.Name)
		if err == nil {
			return fmt.Errorf("%v type is already registered", definition.Name)
		}

		r._types.Register(definition.Name, definition.Type())
	}

	r._views = ViewSlice(r.Views).Index()
	r._connectors = config.ConnectorSlice(r.Connectors).Index()
	r._parameters = ParametersSlice(r.Parameters).Index()
	r._loggers = r.Loggers.Index()

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
func NewResourceFromURL(ctx context.Context, url string, types Types, visitors visitor.Visitors) (*Resource, error) {
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

	err = resource.Init(ctx, types, visitors)

	return resource, err
}

func (r *Resource) FindConnector(view *View) (*config.Connector, error) {
	if view.Connector == nil {
		var connector *config.Connector

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
		Connectors:  make([]*config.Connector, 0),
		_connectors: config.Connectors{},
		Views:       make([]*View, 0),
		_views:      Views{},
		Parameters:  make([]*Parameter, 0),
		_parameters: ParametersIndex{},
		_types:      Types{},
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
func (r *Resource) AddConnectors(connectors ...*config.Connector) {
	if r.Connectors == nil {
		r.Connectors = make([]*config.Connector, 0)
	}

	r.Connectors = append(r.Connectors, connectors...)
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
