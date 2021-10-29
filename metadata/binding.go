package metadata

import (
	"fmt"
	"github.com/viant/datly/shared"
	"strings"
)

//Binding represents data binding
type Binding struct {
	Name          string      `json:",omitempty"` //placeholder name
	Type          string      `json:",omitempty"` //Path,QueryString,DataView,ParentURL
	When          string      `json:",omitempty"` //applies binding when criteria is met
	From          string      `json:",omitempty"`
	DataType      string      `json:",omitempty"`
	ComponentType string      `json:",omitempty"`
	DataView      string      `json:",omitempty"`
	Expression    string      `json:",omitempty"`
	Default       interface{} `json:",omitempty"`
	Required      bool        `json:",omitempty"`
}



//Init initialises binding
func (b *Binding) Init() {
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
func (b Binding) Validate() error {
	switch b.Type {
	case shared.BindingQueryString, shared.BindingPath, shared.BindingDataPool, shared.BindingBodyData, shared.BindingHeader:
	case shared.BindingDataView:
		if b.DataView == "" {
			return fmt.Errorf("dataView was empty for %v binding type", b.Type)
		}
	default:
		return fmt.Errorf("unsupported binding.type: '%v'", b.Type)
	}

	if b.Expression != "" && !strings.Contains(b.Expression, "$value") {
		return fmt.Errorf("invalid expression: %v, expected '$value' expression", b.Expression)
	}
	return nil
}



