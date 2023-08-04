package view

import (
	"context"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

type (
	Resourcelet interface {
		LookupParameter(name string) (*Parameter, error)
		ViewSchema(ctx context.Context, schema string) (*Schema, error)
		LookupType() xreflect.LookupType
		LoadText(ctx context.Context, URL string) (string, error)
		NamedCodecs() *codec.Registry
		IndexedColumns() NamedColumns
	}
)

type resourcelet struct {
	*View
	*Resource
}

func (r *resourcelet) LookupParameter(name string) (*Parameter, error) {
	parameter, err := r.lookupParameter(name)
	return parameter, err
}

func (r *resourcelet) lookupParameter(name string) (*Parameter, error) {
	var viewParameter *Parameter
	if r.View != nil && r.View.Template != nil && len(r.View.Template.Parameters) > 0 {
		if len(r.View.Template._parametersIndex) == 0 {
			r.View.Template._parametersIndex = Parameters(r.View.Template.Parameters).Index()
		}
		if viewParameter, _ = r.View.Template._parametersIndex[name]; viewParameter != nil && viewParameter.Ref == "" {
			return viewParameter, nil
		}
	}
	ret, err := r._parameters.Lookup(name)
	if ret == nil && viewParameter != nil {
		return viewParameter, nil
	}
	return ret, err
}

func (r *resourcelet) NamedCodecs() *codec.Registry {
	return r._visitors
}

func (r resourcelet) IndexedColumns() NamedColumns {
	if r.View == nil {
		return nil
	}
	return r.View.IndexedColumns()
}

func NewResourcelet(resource *Resource, view *View) *resourcelet {
	return &resourcelet{View: view, Resource: resource}
}
