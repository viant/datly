package handlergen

import (
	"go/ast"

	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
)

func (g *Generation) prepareMutation(semantic *plan.Plan, config handlergo.Config, contracts *gen.Plan) error {
	if err := handlergo.ValidateUniversalWriterPlan(semantic, config); err != nil {
		return err
	}
	indexes, err := handlergo.ReadIndexes(semantic, config)
	if err == nil && indexes != nil && config.ReadIndexes != nil && !config.ReadIndexes.Owned && g.Directory != "" {
		candidateConfig := config
		target := *config.ReadIndexes
		target.Owned = true
		candidateConfig.ReadIndexes = &target
		candidate, candidateErr := handlergo.ReadIndexes(semantic, candidateConfig)
		if candidateErr != nil {
			return candidateErr
		}
		destination := "indexes.go"
		if settings := g.compiled.Component.Settings; settings != nil {
			destination = settings.Generation.File("indexes", destination)
		}
		inputName := ""
		if candidate != nil {
			for _, decl := range candidate.File.Decls {
				if method, ok := decl.(*ast.FuncDecl); ok && method.Recv != nil && len(method.Recv.List) == 1 {
					if ptr, ok := method.Recv.List[0].Type.(*ast.StarExpr); ok {
						if local, ok := ptr.X.(*ast.Ident); ok {
							inputName = local.Name
						}
					}
				}
			}
		}
		if candidate != nil && inputName != "" && gen.RetainsReadIndexCache(g.Directory, destination, contracts.Input.Destination, g.compiled.Component.Name, inputName, candidate.CacheField, candidate.TypeName, candidate.File) {
			indexes = candidate
		}
	}
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
