package transcribe

import (
	"fmt"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	handlerplan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
	"github.com/viant/datly/typecatalog"
)

// prepareFactoryCompanions reuses generated entity and read helpers without
// selecting generated execution. The body projection remains authoring-only;
// the application's factory retains sole ownership of persistence.
func prepareFactoryCompanions(compiled *Result, input *gen.Input) error {
	if compiled.ExternalHandler == nil || !compiled.ExternalHandler.GeneratedContracts || compiled.ExternalHandler.InputShape == nil {
		return nil
	}
	contracts, err := gen.New(*input).Plan()
	if err != nil {
		return err
	}
	staged := *compiled
	staged.Component = compiled.Component.Clone()
	staged.Component.RootView = compiled.ExternalHandler.InputShape.Clone()
	// Markers remain in the canonical generated contract. The detached helper
	// graph needs their fields, but must not select automatic write decisions.
	var shapeOnly func(*spec.View)
	shapeOnly = func(view *spec.View) {
		if view == nil {
			return
		}
		for _, column := range view.Columns {
			if column != nil {
				column.DeleteMarker = false
				column.ConcurrencyToken = false
			}
		}
		for _, relation := range view.Relations {
			if relation != nil {
				shapeOnly(relation.View)
			}
		}
	}
	shapeOnly(staged.Component.RootView)
	helpers := newHandlerGeneration(&staged, input, Options{Handler: HandlerOptions{Operation: handlerplan.OperationPost}})
	semantic, err := helpers.compilePlan()
	if err != nil {
		return fmt.Errorf("factory companion graph: %w", err)
	}
	semantic, err = helpers.withGeneratedPresenceForGraph(semantic, contracts, true)
	if err != nil {
		return err
	}
	records, err := helpers.recordTypes(semantic, contracts)
	if err != nil {
		return err
	}
	if err = helpers.readCollections(semantic, contracts); err != nil {
		return err
	}
	config := handlergo.Config{Package: contracts.PackageName(), PackagePath: input.TargetPackage,
		Factory:   "New" + typecatalog.FieldName(compiled.Component.Name) + "Handler",
		InputType: contracts.Input.Type, OutputType: contracts.Output.Type, Records: records, Imports: contracts.Imports,
		ReadIndexes: &handlergo.ReadIndexConfig{Package: contracts.PackageName(), PackagePath: input.TargetPackage, InputType: contracts.Input.Type, Owned: true}}
	if input.TypeResolver != nil {
		config.ReadIndexes.Types = input.TypeResolver.Descriptor
	}
	indexes, err := handlergo.ReadIndexes(semantic, config)
	if err != nil {
		return err
	}
	if indexes != nil {
		input.ReadIndexes = &gen.ReadIndexSource{Package: indexes.PackagePath, TypeName: indexes.TypeName, CacheField: indexes.CacheField, Source: gen.MutationSource{Role: "indexes", Destination: "indexes.go", File: indexes.File}}
	}
	setters, err := handlergo.EntitySupport(semantic, config)
	if err != nil {
		return err
	}
	return helpers.applyEntitySetters(setters)
}
