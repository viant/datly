package transcribe

import (
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
)

func (g *handlerGeneration) prepareMutation(semantic *plan.Plan, config handlergo.Config) error {
	if err := handlergo.ValidateUniversalWriterPlan(semantic, config); err != nil {
		return err
	}
	indexes, err := handlergo.ReadIndexes(semantic, config)
	if err != nil {
		return err
	}
	if indexes != nil {
		g.input.ReadIndexes = &gen.ReadIndexSource{
			Package: indexes.PackagePath, TypeName: indexes.TypeName, CacheField: indexes.CacheField,
			Source: gen.MutationSource{Role: "indexes", Destination: "indexes.go", File: indexes.File},
		}
	}
	setters, err := handlergo.EntitySupport(semantic, config)
	if err != nil {
		return err
	}
	if err = g.applyEntitySetters(setters); err != nil {
		return err
	}
	if g.options.Handler.Hooks.Scaffold {
		semantic, err = g.prepareMutationScaffold(semantic, config)
		if err != nil {
			return err
		}
	}
	// The canonical semantic plan is interpreted by runtime/handler/writer.
	// Component packages own shapes, metadata, resources and optional hooks;
	// they never receive component-specific phase/layout/action implementations.
	g.input.MutationHandler = nil
	return nil
}
