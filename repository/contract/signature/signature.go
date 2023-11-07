package signature

import (
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

// Signature defines contract signature
type Signature struct {
	URI       string
	Method    string
	Anonymous bool
	Types     []*view.TypeDefinition
	Output    *state.Schema
	Input     *state.Type
	Filter    *state.Schema
	//TODO add input, body with types def if needed
}
