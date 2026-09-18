package transcribe

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
)

// RuntimeContract is the dynamic-reader contract compiled from DQL without
// emitting or loading a persistent generated Go package.
type RuntimeContract struct {
	Result     *Result
	Component  *spec.Component
	InputType  reflect.Type
	OutputType reflect.Type
	Resources  *resource.Store
	Types      *typecatalog.Catalog
	Plan       *gen.Plan
}

// RuntimeContracts compiles authored DQL and materializes generated input and
// output reflect types for an ephemeral reader execution. rootDir must be a
// module root so #package destination authority is resolved consistently with
// ordinary transcription.
func (c *Compiler) RuntimeContracts(ctx context.Context, rootDir string, source *Source) (*RuntimeContract, error) {
	if c == nil {
		c = NewCompiler()
	}
	if source == nil {
		return nil, fmt.Errorf("runtime contract source is required")
	}
	copy := *source
	if copy.Types == nil {
		copy.Types = typecatalog.NewCatalog()
	}
	compiled, err := c.Compile(ctx, &copy)
	if err != nil {
		return nil, err
	}
	input, _, err := generationInput(rootDir, "generated", compiled)
	if err != nil {
		return nil, err
	}
	generator := gen.New(input)
	inputType, err := generator.RuntimeInputType()
	if err != nil {
		return nil, err
	}
	outputType, err := generator.RuntimeOutputType()
	if err != nil {
		return nil, err
	}
	plan, err := generator.Plan()
	if err != nil {
		return nil, err
	}
	return &RuntimeContract{Result: compiled, Component: input.Component.Clone(), InputType: inputType, OutputType: outputType, Resources: input.Resources, Types: compiled.Source.Types, Plan: plan}, nil
}
