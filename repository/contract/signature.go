package contract

import (
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"strings"
)

// Signature defines contract signature
type Signature struct {
	URI       string
	Method    string
	Anonymous bool
	Types     []*view.TypeDefinition
	Output    *state.Schema
	//TODO add input, body with types def if needed
}

func (s *Signature) AdjustOutputTypeName(componentType string) {
	originalType := s.Output.Name
	s.Output.DataType = strings.ReplaceAll(s.Output.DataType, originalType, componentType)
	s.Output.Name = strings.ReplaceAll(s.Output.Name, originalType, componentType)
	for _, typeDef := range s.Types {
		typeDef.Name = strings.ReplaceAll(typeDef.Name, originalType, componentType)
	}
}
