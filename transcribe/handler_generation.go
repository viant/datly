package transcribe

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	gen "github.com/viant/datly/transcribe/generate"
	handlerplan "github.com/viant/datly/transcribe/handler/ast"
	handlercompiler "github.com/viant/datly/transcribe/handler/compiler"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
	handlervelty "github.com/viant/datly/transcribe/handler/velty"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// handlerGeneration owns the build-stage composition between canonical
// contracts, target-neutral handler semantics, and one selected target.
type handlerGeneration struct {
	compiled  *Result
	input     *gen.Input
	options   Options
	directory string
}

func newHandlerGeneration(compiled *Result, input *gen.Input, options Options) *handlerGeneration {
	return &handlerGeneration{compiled: compiled, input: input, options: options}
}

func (g *handlerGeneration) prepare() error {
	if g == nil || g.compiled == nil || g.compiled.Component == nil || g.input == nil {
		return fmt.Errorf("compiled transcribe result and generation input are required")
	}
	if err := g.applyContractOption(); err != nil {
		return err
	}
	if g.options.Handler.Target == HandlerNone {
		return nil
	}
	if g.compiled.GoHandler != nil || g.compiled.VeltyHandler != nil {
		return fmt.Errorf("generated handler target %q is mutually exclusive with supplied or authored handler logic", g.options.Handler.Target)
	}
	semantic, err := g.compilePlan()
	if err != nil {
		return err
	}
	g.input.SetMarkerViews = g.setMarkerViews(semantic)
	switch g.options.Handler.Target {
	case HandlerVelty:
		return g.prepareVelty(semantic)
	case HandlerGo:
		return g.prepareGo(semantic)
	default:
		return fmt.Errorf("unsupported generated handler target %q", g.options.Handler.Target)
	}
}

func (g *handlerGeneration) diagnostic(cause error) error {
	if cause == nil || g == nil || g.compiled == nil || g.compiled.Source == nil {
		return cause
	}
	span := pointSpan(g.compiled.Source.Text, 0)
	return &CompileError{Cause: cause, Diagnostics: []*Diagnostic{{
		Code: "DQL-HANDLER", Severity: SeverityError, Message: cause.Error(),
		Path: g.compiled.Source.Path, Span: span,
	}}}
}

func (g *handlerGeneration) applyContractOption() error {
	switch g.options.Contracts {
	case ContractsAuto:
		return nil
	case ContractsLinked:
		if g.input.Contracts.Input == nil || g.input.Contracts.Output == nil {
			return fmt.Errorf("linked contracts require package-authoritative input and output contract references")
		}
		return nil
	case ContractsGenerated:
		g.input.Contracts = gen.ContractReferences{}
		delete(g.input.Views, gen.RootViewPath)
		if g.input.Component.Settings != nil {
			if g.compiled.ContractTypeOverrides.Input == "" {
				g.input.Component.Settings.InputType = ""
			}
			if g.compiled.ContractTypeOverrides.Output == "" {
				g.input.Component.Settings.OutputType = ""
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported contract option %q", g.options.Contracts)
	}
}

func (g *handlerGeneration) compilePlan() (*handlerplan.Plan, error) {
	component := g.compiled.Component
	options := g.options.Handler
	if options.RootView != "" {
		if component.RootView == nil {
			return nil, fmt.Errorf("generated handler root view %q was not found", options.RootView)
		}
		identity, err := component.RootView.Identity()
		if err != nil {
			return nil, err
		}
		if options.RootView != identity {
			return nil, fmt.Errorf("generated handler root view %q does not match canonical identity %q", options.RootView, identity)
		}
	}
	return (&handlercompiler.Compiler{}).Compile(handlercompiler.Request{
		Component: component, ViewBindings: g.compiled.ViewBindings,
		Operation: options.Operation, Input: options.Input, Output: options.Output,
		Current: options.Current, Currents: options.Currents,
		Table: options.Table, Key: options.Key,
	})
}

func (g *handlerGeneration) prepareVelty(semantic *handlerplan.Plan) error {
	contractPlan, err := gen.New(*g.input).Plan()
	if err != nil {
		return fmt.Errorf("plan generated Velty handler contracts: %w", err)
	}
	if semantic, err = g.withGeneratedPresence(semantic, contractPlan); err != nil {
		return err
	}
	records, err := g.recordTypes(semantic, contractPlan)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.CurrentValue != "" {
			return fmt.Errorf("generated Velty write handler requires direct current rows; carrier %s requires the metadata-aware mutation program", record.CurrentValue)
		}
	}
	template, err := handlervelty.Render(semantic)
	if err != nil {
		return err
	}
	hasService, err := validateVeltyHandlerTemplate(template, g.compiled.Component)
	if err != nil {
		return fmt.Errorf("validate generated Velty handler: %w", err)
	}
	if !hasService && !semantic.Root.Auxiliary {
		return fmt.Errorf("generated Velty handler contains no DML service call")
	}
	options := g.options.Handler.Velty
	g.input.VeltyHandler = &gen.VeltyHandlerAsset{
		Template: template, Factory: options.Factory,
		GoDestination: options.GoDestination, ResourceDestination: options.ResourceDestination,
	}
	resolved, err := gen.New(*g.input).Plan()
	if err != nil {
		return err
	}
	entities, err := handlergo.EntitySupport(semantic, handlergo.Config{Package: "generated", Factory: resolved.VeltyHandler.Factory, InputType: contractPlan.Input.Type, OutputType: contractPlan.Output.Type, Records: records, Imports: contractPlan.Imports})
	if err != nil {
		return err
	}
	g.applyEntitySupport(entities)
	return nil
}

func (g *handlerGeneration) prepareGo(semantic *handlerplan.Plan) error {
	if semantic == nil {
		return fmt.Errorf("generated Go handler plan is required")
	}
	contractPlan, err := gen.New(*g.input).Plan()
	if err != nil {
		return fmt.Errorf("plan generated Go handler contracts: %w", err)
	}
	if semantic, err = g.withGeneratedPresence(semantic, contractPlan); err != nil {
		return err
	}
	records, err := g.recordTypes(semantic, contractPlan)
	if err != nil {
		return err
	}
	options := g.options.Handler
	factory := options.Go.Factory
	if factory == "" {
		name := typecatalog.FieldName(g.compiled.Component.Name)
		if name == "" {
			return fmt.Errorf("generated Go handler component name is required")
		}
		factory = "New" + name + "Handler"
	}
	config := handlergo.Config{
		Package: "generated", PackagePath: g.input.TargetPackage, Factory: factory, Handler: options.Go.Handler,
		InputType: contractPlan.Input.Type, OutputType: contractPlan.Output.Type,
		Records: records, Imports: contractPlan.Imports,
	}
	if options.Go.Execution == GoExecutionMutation {
		config.Package = contractPlan.PackageName()
		return g.prepareMutation(semantic, config)
	}
	asset, err := handlergo.Lower(semantic, config)
	if err != nil {
		return err
	}
	g.input.ContractHandler = &gen.ContractHandlerAsset{
		Destination: options.Go.Destination, Factory: asset.Factory, File: asset.File,
	}
	g.applyEntitySupport(asset.Entities)
	if !options.Hooks.Scaffold {
		return nil
	}
	hooks, err := handlergo.ScaffoldHooks(config)
	if err != nil {
		return err
	}
	g.input.HookScaffold = &gen.HookScaffoldAsset{Destination: options.Hooks.Destination, File: hooks}
	return nil
}

func (g *handlerGeneration) setMarkerViews(plan *handlerplan.Plan) map[string]bool {
	if plan == nil || plan.Root == nil {
		return nil
	}
	result := map[string]bool{}
	var visit func(*handlerplan.RecordPlan)
	visit = func(record *handlerplan.RecordPlan) {
		if record == nil || record.Auxiliary {
			return
		}
		if identity := strings.TrimSpace(record.Identity); identity != "" {
			result[identity] = true
		}
		for _, relation := range record.Relations {
			if relation != nil {
				visit(relation.Child)
			}
		}
	}
	visit(plan.Root)
	return result
}

func (g *handlerGeneration) withGeneratedPresence(semantic *handlerplan.Plan, generated *gen.Plan) (*handlerplan.Plan, error) {
	if semantic == nil || semantic.Root == nil {
		return semantic, nil
	}
	if generated == nil {
		return nil, fmt.Errorf("generated entity presence plan is required")
	}
	refined := semantic.Clone()
	fieldsByIdentity := map[string][]string{}
	viewsByIdentity := map[string]*gen.ViewPlan{}
	for _, view := range generated.Views {
		if view.Ownership == gen.ViewGenerated && len(view.SetMarkerFields) != 0 {
			fieldsByIdentity[view.Identity] = append([]string(nil), view.SetMarkerFields...)
			viewCopy := view
			viewsByIdentity[view.Identity] = &viewCopy
		}
	}
	var apply func(*handlerplan.RecordPlan) error
	apply = func(record *handlerplan.RecordPlan) error {
		if record == nil {
			return fmt.Errorf("generated entity presence plan contains a nil record")
		}
		if record.Auxiliary {
			return nil
		}
		record.PresenceFields = append([]string(nil), fieldsByIdentity[record.Identity]...)
		if view := viewsByIdentity[record.Identity]; view != nil {
			entity := &handlerplan.EntityPlan{MarkerField: "Has", MarkerPointer: true, Owned: true, MarkerType: spec.TypeRef{Name: view.Name + "Has"}}
			for _, name := range view.SetMarkerFields {
				field, ok := view.Field(name)
				if !ok {
					return fmt.Errorf("entity %s marker field %s is missing", view.Name, name)
				}
				planned := handlerplan.EntityField{Name: name, Path: handlerplan.FieldPath{name}, Type: spec.TypeRef{Name: field.Type}, Writable: true}
				fieldTags := reflect.StructTag(field.Tag)
				if _, ok := fieldTags.Lookup(tag.ViewName); ok {
					planned.Relation = true
					planned.Writable = false
				}
				if value, ok := fieldTags.Lookup(tag.SelfName); ok {
					if _, err := tag.ParseSelf(value); err != nil {
						return err
					}
					planned.Relation = true
					planned.Self = true
					planned.Writable = true
				}
				if value, ok := reflect.StructTag(field.Tag).Lookup(tag.InvariantName); ok {
					var err error
					planned.Invariant, err = tag.ParseInvariant(value)
					if err != nil {
						return err
					}
				}
				for _, key := range record.Keys {
					if key.Field == name {
						planned.Identity = true
					}
				}
				for _, relation := range record.Relations {
					if relation != nil && len(relation.FieldPath) == 1 && relation.FieldPath[0] == name {
						planned.Relation = true
						planned.Writable = true
						if relation.Child != nil && relation.Child.Auxiliary {
							planned.Writable = false
						}
					}
				}
				entity.Fields = append(entity.Fields, planned)
			}
			for _, key := range record.Keys {
				if field, ok := view.Field(key.Field); ok {
					key.Type = spec.TypeRef{Name: field.Type}
				}
				entity.Keys = append(entity.Keys, key)
			}
			groups, err := handlercompiler.EntityInvariants(entity)
			if err != nil {
				return err
			}
			entity.Invariants = groups
			record.Entity = entity
		}
		for _, relation := range record.Relations {
			if relation == nil || relation.Child == nil {
				return fmt.Errorf("generated PATCH presence plan contains an incomplete relation")
			}
			if err := apply(relation.Child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := apply(refined.Root); err != nil {
		return nil, err
	}
	return refined, nil
}

func (g *handlerGeneration) recordTypes(semantic *handlerplan.Plan, generated *gen.Plan) ([]handlergo.RecordType, error) {
	if semantic == nil || semantic.Root == nil || generated == nil {
		return nil, fmt.Errorf("semantic handler and generated contract plans are required")
	}
	rootType, err := g.inputFieldType(generated, semantic.Root.InputPath)
	if err != nil {
		return nil, err
	}
	result := make([]handlergo.RecordType, 0)
	if err = g.appendRecordType(&result, semantic, generated, semantic.Root, rootType); err != nil {
		return nil, err
	}
	if err = g.compileEntityHooks(semantic, generated, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (g *handlerGeneration) appendRecordType(result *[]handlergo.RecordType, semantic *handlerplan.Plan, generated *gen.Plan, record *handlerplan.RecordPlan, valueType string) error {
	if record == nil {
		return fmt.Errorf("generated Go record plan is required")
	}
	currentType := ""
	currentValue := ""
	if record.Current != nil {
		var err error
		currentType, currentValue, err = g.currentRecordTypes(generated, record.Current)
		if err != nil {
			return err
		}
	} else if semantic.Operation == handlerplan.OperationPatch && !record.Auxiliary {
		return fmt.Errorf("generated Go PATCH record requires current input authority")
	}
	*result = append(*result, handlergo.RecordType{
		Identity: record.Identity, Path: append(handlerplan.FieldPath(nil), record.InputPath...),
		Value: valueType, Current: currentType, CurrentValue: currentValue,
	})
	base, err := g.recordBase(valueType, record.Cardinality)
	if err != nil {
		return fmt.Errorf("generated record %q: %w", record.Identity, err)
	}
	view := generated.ViewByType(base)
	if view == nil || view.Ownership == gen.ViewLinked {
		if err := g.refineLinkedEntity(record, base, generated.Imports); err != nil {
			return err
		}
	}
	if record.Entity != nil {
		if view != nil && view.Ownership == gen.ViewGenerated {
			packagePath := view.Package
			if packagePath == "" {
				packagePath = g.input.TargetPackage
			}
			record.Entity.Type = spec.TypeRef{Package: packagePath, Name: base}
		} else if g.input.TypeResolver != nil {
			resolved, err := g.input.TypeResolver.ResolveShape(base)
			if err != nil {
				return err
			}
			if resolved != nil && resolved.Descriptor != nil {
				reference, err := (xshape.Resolver{}).Reference(resolved.Identity)
				if err != nil {
					return err
				}
				record.Entity.Type = spec.TypeRef{Package: reference.Qualifier, Name: reference.Name}
			}
		}
	}
	if err := g.refineCurrentProjection(record, generated, valueType, currentType); err != nil {
		return err
	}
	if err := g.refineMutationFields(record, generated, valueType); err != nil {
		return err
	}
	if err := g.refineLateWriteEffects(record, generated, valueType); err != nil {
		return err
	}
	if len(record.Relations) != 0 && view == nil {
		return fmt.Errorf("generated record %q type %q has no final view plan", record.Identity, base)
	}
	for _, relation := range record.Relations {
		if relation == nil || relation.Child == nil || len(relation.FieldPath) != 1 {
			return fmt.Errorf("generated record %q contains an incomplete relation", record.Identity)
		}
		field, ok := view.Field(relation.FieldPath[0])
		if !ok {
			return fmt.Errorf("generated view %q has no relation field %q", view.Type, relation.FieldPath)
		}
		if err = g.appendRecordType(result, semantic, generated, relation.Child, field.Type); err != nil {
			return err
		}
		if err = g.refineRelationMutationFields(relation, record, generated, valueType, field.Type); err != nil {
			return err
		}
	}
	return nil
}

func (g *handlerGeneration) inputFieldType(plan *gen.Plan, path handlerplan.FieldPath) (string, error) {
	if plan == nil || len(path) != 2 || path[0] != "Input" {
		return "", fmt.Errorf("generated input field path %q is not a direct contract field", path)
	}
	field, ok := plan.Input.Field(path[1])
	if !ok {
		return "", fmt.Errorf("generated input field %q was not found", path[1])
	}
	return field.Type, nil
}

func (g *handlerGeneration) recordBase(source string, cardinality spec.Cardinality) (string, error) {
	reference, err := (xshape.Resolver{}).Reference(source)
	if err != nil {
		return "", fmt.Errorf("parse record type %q: %w", source, err)
	}
	wrappers := reference.Wrappers
	many := false
	if len(wrappers) != 0 && (wrappers[0].Kind == xshape.WrapperSlice || wrappers[0].Kind == xshape.WrapperArray) {
		many = true
		wrappers = wrappers[1:]
	}
	if len(wrappers) != 0 && wrappers[0].Kind == xshape.WrapperPointer {
		wrappers = wrappers[1:]
	}
	if many != (cardinality == spec.CardinalityMany) {
		return "", fmt.Errorf("record type %q does not match %s cardinality", source, cardinality)
	}
	if len(wrappers) != 0 {
		return "", fmt.Errorf("record type %q must resolve to a direct named type", source)
	}
	return reference.QualifiedName(), nil
}
