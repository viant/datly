package view

import (
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/xdatly/codec"
)

type Resourcelet struct {
	*View
	*Resource
}

func (r *Resourcelet) LookupParameter(name string) (*state.Parameter, error) {
	parameter, err := r.lookupParameter(name)
	return parameter, err
}

func (r *Resourcelet) AppendParameter(parameter *state.Parameter) {
	r.Parameters.Append(parameter)
}

func (r *Resourcelet) lookupParameter(name string) (*state.Parameter, error) {
	var viewParameter *state.Parameter
	if r.View != nil && r.View.Template != nil && len(r.View.Template.Parameters) > 0 {
		if len(r.View.Template._parametersIndex) == 0 {
			r.View.Template._parametersIndex = state.Parameters(r.View.Template.Parameters).Index()
		}
		if viewParameter, _ = r.View.Template._parametersIndex[name]; viewParameter != nil && viewParameter.Ref == "" {
			return viewParameter, nil
		}
	}
	ret, err := r._parameters.Lookup(name)

	if ret == nil && viewParameter != nil {
		return viewParameter, nil
	}
	if ret = r.Parameters.Lookup(name); ret != nil {
		return ret, nil
	}
	return ret, err
}

func (r *Resourcelet) CodecOptions() *codec.Options {
	indexColumns := r.IndexedColumns()
	if indexColumns == nil {
		return nil
	}
	return &codec.Options{Options: []interface{}{indexColumns}}
}

func (r *Resourcelet) Codecs() *codec.Registry {
	ret := r.codecs
	if ret == nil {
		ret = extension.Config.Codecs
	}
	return ret
}

func (r Resourcelet) IndexedColumns() NamedColumns {
	if r.View == nil {
		return nil
	}
	return r.View.IndexedColumns()
}

func NewResources(resource *Resource, view *View) *Resourcelet {
	ret := &Resourcelet{View: view, Resource: resource}
	if view._resource != nil {
		ret.Resource = view._resource
	}
	return ret
}
