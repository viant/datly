package component

import (
	"github.com/viant/datly/service"
	"github.com/viant/datly/view/state"
)

type (
	//Style defines style
	//TODO deprecate with function on input parameters to determine style
	Style string

	Contract struct {
		Name    string `json:",omitempty" yaml:",omitempty"`
		Input   Input
		Output  Output
		Service service.Type `json:",omitempty"`
	}

	Input struct {
		Type state.Type
		//TODO add explicit body type when applicable
		//BodyType state.Type
	}
	// BodySelector deprecated,  use output parameter instead
	//deprecated
	BodySelector struct {
		StateValue string
	}
)

const (
	BasicStyle         Style = "Basic"
	ComprehensiveStyle Style = "Comprehensive"
)
