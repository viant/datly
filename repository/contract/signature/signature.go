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

func (s *Signature) AdjustedRegisteredType(componentType string) {
	//originalType := s.Output.Name
	//s.Output.DataType = strings.Replace(s.Output.DataType, originalType, componentType, 1)
	//s.Output.Name = strings.Replace(s.Output.Name, originalType, componentType, 1)
	//for _, typeDef := range s.Types {
	//	typeDef.Name = strings.Replace(typeDef.Name, originalType, componentType, 1)
	//}

}
