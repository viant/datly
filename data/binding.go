package data

import (
	"datly/base"
	"github.com/go-errors/errors"
)

//Binding represents data binding
type Binding struct {
	Name          string `json:",omitempty"`
	Placeholder   string `json:",omitempty"`
	Type          string `json:",omitempty"` //URI,QueryString,DataView,Parent
	DataType      string `json:",o mitempty"`
	ComponentType string `json:",omitempty"`
	DataView      string `json:",omitempty"`
	Default       interface{} `json:",omitempty"`
}

//Init initialises binding
func (b *Binding) Init() {
	if b.Type == "" {
		if b.DataView != "" {
			b.Type = base.BindingDataView
		}
	}
	if b.Name == "" && b.Placeholder != "" {
		b.Name = b.Placeholder
	}
	if b.Name != "" && b.Placeholder == "" {
		b.Placeholder = b.Name
	}
}

//Validate checks if binding is valid
func (b Binding) Validate() error {
	switch b.Type {
	case base.BindingQueryString, base.BindingURI, base.BindingData, base.BindingHeader:
	case base.BindingDataView:
		if b.DataView == "" {
			return errors.Errorf("dataView was empty for %v binding type", b.Type)
		}
	default:
		return errors.Errorf("unsupported binding.type: '%v'", b.Type)
	}
	return nil
}
