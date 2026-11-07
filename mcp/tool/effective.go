package tool

import (
	"github.com/viant/datly/exec"
	"github.com/viant/mcp-protocol/schema"
)

// EffectiveMetadata recompiles only publication metadata. Invocation continues
// to own its canonical typed binding contract until it selects an effective plan.
func (p *Plan) EffectiveMetadata(effective *exec.EffectiveContract) (schema.Tool, error) {
	compiled, err := p.EffectivePlan(effective)
	if err != nil {
		return schema.Tool{}, err
	}
	result := p.Metadata()
	updated := compiled.Metadata()
	result.InputSchema = updated.InputSchema
	result.OutputSchema = updated.OutputSchema
	return result, nil
}

// EffectivePlan uses the same caller-specific projection for public arguments
// and discovery, while retaining canonical native destination bindings.
func (p *Plan) EffectivePlan(effective *exec.EffectiveContract) (*Plan, error) {
	input := p.compilerInput
	if effective == nil || effective.Input == input.Contract.Type() && effective.Output == input.OutputType && effective.InputRepresentation == nil && effective.OutputRepresentation == nil {
		return p, nil
	}
	var err error
	if effective.InputRepresentation != nil {
		input.Fields, err = input.Contract.MetadataRepresentation(effective.InputRepresentation, input.Fields)
	} else {
		input.Fields, err = input.Contract.MetadataFields(effective.Input, input.Fields)
	}
	if err != nil {
		return nil, err
	}
	input.OutputType = effective.Output
	input.Output, err = input.Output.WithContract(effective.Output, effective.OutputRepresentation)
	if err != nil {
		return nil, err
	}
	return NewCompiler(WithStrictArguments(p.strictArguments)).Compile(input)
}
