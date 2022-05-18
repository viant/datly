package data

import (
	"github.com/go-errors/errors"
	"github.com/viant/datly/v0/shared"
	"strings"
)

//Parameter represents view binding
type Parameter struct {
	Name          string      `json:",omitempty"` //placeholder name
	When          string      `json:",omitempty"` //applies binding when criteria is met
	From          string      `json:",omitempty"`
	Type          string      `json:",omitempty"` //Path,QueryString,DataView,Parent
	DataType      string      `json:",o mitempty"`
	ComponentType string      `json:",omitempty"`
	DataView      string      `json:",omitempty"`
	Expression    string      `json:",omitempty"`
	Default       interface{} `json:",omitempty"`
}

//Init initialises binding
func (b *Parameter) Init() {
	if b.Type == "" {
		if b.DataView != "" {
			b.Type = shared.BindingDataView
		}
	}
	if b.From == "" && b.Name != "" {
		b.From = b.Name
	}
	if b.From != "" && b.Name == "" {
		b.Name = b.From
	}
}

//Validate checks if binding is valid
func (b Parameter) Validate() error {
	switch b.Type {
	case shared.BindingQueryString, shared.BindingPath, shared.BindingDataPool, shared.BindingBodyData, shared.BindingHeader:
	case shared.BindingDataView:
		if b.DataView == "" {
			return errors.Errorf("dataView was empty for %v binding type", b.Type)
		}
	default:
		return errors.Errorf("unsupported binding.type: '%v'", b.Type)
	}

	if b.Expression != "" && !strings.Contains(b.Expression, "$value") {
		return errors.Errorf("invalid expression: %v, expected '$value' expression", b.Expression)
	}
	return nil
}
