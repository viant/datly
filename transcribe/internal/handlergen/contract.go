package handlergen

import (
	"fmt"
	"strings"

	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
 "github.com/viant/datly/transcribe/internal/bodygraph"
	xshape "github.com/viant/x/shape"
)

// Generated write handlers copy the selected body into the selected output.
// Validate that assignment under final package/type authority before emitting.
func (g *Generation) validateContractAssignment(semantic *plan.Plan, generated *gen.Plan) error {
	// Native auxiliary component hooks own their result. The runtime copies a
	// body only when the canonical output actually has an entity-compatible slot.
	root := g.compiled.Component.RootView
	if g.options.Handler.Target == HandlerGo && g.options.Handler.Go.Execution == GoExecutionMutation && root != nil && strings.TrimSpace(root.EntityHooks) != "" && bodygraph.AllAuxiliary(root) {
		return nil
	}
	if semantic.Output == nil {
		return nil
	}
	input, err := g.inputFieldType(generated, semantic.Input.Path)
	if err != nil {
		return err
	}
	path := semantic.Output.Path
	if len(path) != 2 || path[0] != "Output" {
		return fmt.Errorf("generated output path %q is not a direct contract field", path)
	}
	output, ok := generated.Output.Field(path[1])
	if !ok {
		return fmt.Errorf("generated output field %s is missing", path[1])
	}
	if output.Type == "any" || output.Type == "interface{}" {
		return nil
	}
	imports := map[string]string{}
	for _, item := range generated.Imports {
		imports[item.Alias] = item.Package
	}
	resolver := xshape.Resolver{Package: g.input.TargetPackage, Imports: imports}
	from, err := resolver.Canonical(input)
	if err != nil {
		return err
	}
	to, err := resolver.Canonical(output.Type)
	if err != nil {
		return err
	}
	if from != to {
		return fmt.Errorf("generated write output %s type %q conflicts with body type %q", path[1], output.Type, input)
	}
	return nil
}
