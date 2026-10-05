package compiler

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func mutationInput(p *spec.Parameter) bool { return p.IsMutationInput() }
func validateInternalRoot(c *spec.Component, operation plan.Operation) error {
	return spec.ValidateInternalMutationRoot(c, string(operation))
}
