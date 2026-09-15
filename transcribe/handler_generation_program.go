package transcribe

import (
	"go/ast"

	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
)

func (g *handlerGeneration) prepareMutation(semantic *plan.Plan, config handlergo.Config) error {
	if g.options.Handler.Hooks.Scaffold {
		var err error
		semantic, err = g.prepareMutationScaffold(semantic, config)
		if err != nil {
			return err
		}
	}
	program, err := handlergo.MutationProgram(semantic, config)
	if err != nil {
		return err
	}
	// EntitySupport retains the existing generated method/association owner;
	// other policy phases are companion files in the same artifact transaction.
	g.applyEntitySupport(program.Entities)
	asset := &gen.MutationHandlerAsset{Destination: g.options.Handler.Go.Destination, Factory: program.Factory, File: program.File}
	sources := []struct {
		role string
		file *ast.File
	}{
		{"frames", program.Frames.File},
		{"previous", program.Frames.Previous.File},
		{"layout", program.Frames.Layout.File},
		{"actions", program.Actions.File},
		{"mutation_output", program.Output.File},
		{"validation", program.Validation.File},
	}
	if program.Indexes != nil {
		asset.ReadIndexes = &gen.ReadIndexSource{Package: program.Indexes.PackagePath, TypeName: program.Indexes.TypeName, CacheField: program.Indexes.CacheField, Source: gen.MutationSource{Role: "indexes", Destination: "indexes.go", File: program.Indexes.File}}
	}
	if program.Hooks != nil {
		sources = append(sources, struct {
			role string
			file *ast.File
		}{"hooks", program.Hooks.File})
	}
	if program.Invariants != nil {
		sources = append(sources, struct {
			role string
			file *ast.File
		}{"invariants", program.Invariants.File})
	}
	for _, source := range sources {
		asset.Support = append(asset.Support, gen.MutationSource{Role: source.role, Destination: source.role + ".go", File: source.file})
	}
	g.input.MutationHandler = asset
	return nil
}
