package transcribe

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	handlerplan "github.com/viant/datly/transcribe/handler/ast"
)

func TestNormalizeOptionsFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		match   string
	}{
		{name: "contract", options: Options{Contracts: "compat"}, match: "contract option"},
		{name: "target", options: Options{Handler: HandlerOptions{Target: "script", Operation: WritePatch}}, match: "handler target"},
		{name: "operation", options: Options{Handler: HandlerOptions{Target: HandlerGo}}, match: "operation is required"},
		{name: "intent without target", options: Options{Handler: HandlerOptions{Operation: WritePatch}}, match: "target is required"},
		{name: "Go option on Velty", options: Options{Handler: HandlerOptions{Target: HandlerVelty, Operation: WritePatch, Go: GoHandlerOptions{Factory: "NewHandler"}}}, match: "require target \"go\""},
		{name: "Velty option on Go", options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Velty: VeltyHandlerOptions{Factory: "NewHandler"}}}, match: "require target \"velty\""},
		{name: "unknown execution", options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Go: GoHandlerOptions{Execution: "unknown"}}}, match: "unsupported Go execution"},
		{name: "mutation direct type", options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Go: GoHandlerOptions{Execution: GoExecutionMutation, Handler: "Custom"}}}, match: "direct Go handler type"},
		{name: "mutation scaffold destination", options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Go: GoHandlerOptions{Execution: GoExecutionMutation}, Hooks: HookOptions{Destination: "hooks.go"}}}, match: "requires hook scaffolding"},
		{name: "direct scaffold destination", options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Hooks: HookOptions{Destination: "hooks.go"}}}, match: "requires hook scaffolding"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := normalizeOptions(testCase.options)
			if err == nil || !strings.Contains(err.Error(), testCase.match) {
				t.Fatalf("normalizeOptions() error = %v, want %q", err, testCase.match)
			}
		})
	}
}

func TestNormalizeOptionsOwnsCurrentBindings(t *testing.T) {
	currents := []CurrentBinding{{ViewIdentity: "  view:Items ", Param: " CurrentItems "}}
	actual, err := normalizeOptions(Options{Handler: HandlerOptions{
		Target: HandlerVelty, Operation: WritePatch, Currents: currents,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(currents, []CurrentBinding{{ViewIdentity: "  view:Items ", Param: " CurrentItems "}}) {
		t.Fatalf("caller bindings mutated: %+v", currents)
	}
	if !reflect.DeepEqual(actual.Handler.Currents, []CurrentBinding{{ViewIdentity: "view:Items", Param: "CurrentItems"}}) {
		t.Fatalf("normalized bindings = %+v", actual.Handler.Currents)
	}
}

func TestNormalizeOptionsUsesExplicitZeroValueModes(t *testing.T) {
	actual, err := normalizeOptions(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if actual.Contracts != ContractsAuto || actual.Handler.Target != HandlerNone {
		t.Fatalf("normalized zero options = %+v", actual)
	}
	actual, err = normalizeOptions(Options{Contracts: " AUTO ", Handler: HandlerOptions{Target: " NONE "}})
	if err != nil || actual.Contracts != ContractsAuto || actual.Handler.Target != HandlerNone {
		t.Fatalf("normalized explicit options = %+v, %v", actual, err)
	}
}

func TestCompileHandlerPlanRequiresExactRootIdentity(t *testing.T) {
	component := writeGenerationComponent()
	identity, err := component.RootView.Identity()
	if err != nil {
		t.Fatal(err)
	}
	compiled := &Result{Component: component}
	generation := newHandlerGeneration(compiled, &gen.Input{}, Options{Handler: HandlerOptions{Operation: WritePost, RootView: identity}})
	if _, err = generation.compilePlan(); err != nil {
		t.Fatalf("compile exact root: %v", err)
	}
	generation.options.Handler.RootView = component.RootView.Name
	if _, err = generation.compilePlan(); err == nil || !strings.Contains(err.Error(), "canonical identity") {
		t.Fatalf("non-canonical root error = %v", err)
	}
}

func TestPrepareGoTargetAcceptsPost(t *testing.T) {
	compiled := &Result{Component: writeGenerationComponent()}
	compiled.Component.Parameters = compiled.Component.Parameters[:3]
	compiled.Component.Views = nil
	options := HandlerOptions{Target: HandlerGo, Operation: WritePost}
	input := &gen.Input{Component: compiled.Component, TargetPackage: "example.com/generated/events"}
	generation := newHandlerGeneration(compiled, input, Options{Handler: options})
	semantic, err := generation.compilePlan()
	if err != nil {
		t.Fatal(err)
	}
	if err = generation.prepareGo(semantic); err != nil {
		t.Fatalf("prepareGo() error = %v", err)
	}
	if input.ContractHandler == nil {
		t.Fatal("generated Go POST handler was not prepared")
	}
}

func TestPrepareMutationExecutionKeepsSeparateProducts(t *testing.T) {
	compiled := &Result{Component: writeGenerationComponent()}
	compiled.Component.Parameters = compiled.Component.Parameters[:3]
	compiled.Component.Views = nil
	options, err := normalizeOptions(Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}}})
	if err != nil {
		t.Fatal(err)
	}
	input := &gen.Input{Component: compiled.Component, TargetPackage: "example.com/generated/events"}
	generation := newHandlerGeneration(compiled, input, options)
	if err = generation.prepare(); err != nil {
		t.Fatal(err)
	}
	if input.MutationHandler != nil || input.ContractHandler != nil || input.Component.Settings == nil || input.Component.Settings.Mutation != "post" {
		t.Fatalf("universal mutation metadata was not prepared: %+v", input)
	}
	if _, err = gen.New(*input).Plan(); err != nil {
		t.Fatalf("mutation artifact plan: %v", err)
	}
}

func TestGeneratedSetMarkerViewsCoverMutationEntitiesRecursively(t *testing.T) {
	root := &handlerplan.RecordPlan{
		Identity: "view:Orders",
		Relations: []*handlerplan.RelationPlan{{
			Child: &handlerplan.RecordPlan{Identity: "view:Items"},
		}},
	}
	generation := newHandlerGeneration(nil, nil, Options{})
	want := map[string]bool{"view:Orders": true, "view:Items": true}
	for _, operation := range []handlerplan.Operation{handlerplan.OperationPost, handlerplan.OperationPut, handlerplan.OperationPatch} {
		if actual := generation.setMarkerViews(&handlerplan.Plan{Operation: operation, Root: root}); !reflect.DeepEqual(actual, want) {
			t.Fatalf("%s set marker views = %v, want %v", operation, actual, want)
		}
	}
	root.Relations[0].Child.Auxiliary = true
	if actual := generation.setMarkerViews(&handlerplan.Plan{Operation: handlerplan.OperationPatch, Root: root}); !reflect.DeepEqual(actual, map[string]bool{"view:Orders": true}) {
		t.Fatalf("auxiliary marker ownership %v", actual)
	}
}

func TestHandlerPlanWithGeneratedPresenceHonorsOwnershipAndImmutability(t *testing.T) {
	child := &handlerplan.RecordPlan{Identity: "view:Items"}
	root := &handlerplan.RecordPlan{
		Identity:  "view:Orders",
		Relations: []*handlerplan.RelationPlan{{Child: child}},
	}
	semantic := &handlerplan.Plan{Operation: handlerplan.OperationPatch, Root: root}
	generated := &gen.Plan{Views: []gen.ViewPlan{
		{Identity: root.Identity, Name: "Orders", Ownership: gen.ViewGenerated, SetMarkerFields: []string{"Id", "Name"}, Fields: []gen.Field{{Name: "Id", Type: "int"}, {Name: "Name", Type: "string"}}},
		{Identity: child.Identity, Ownership: gen.ViewLinked, SetMarkerFields: []string{"Id", "OrderId"}},
	}}
	refined, err := newHandlerGeneration(nil, nil, Options{}).withGeneratedPresence(semantic, generated)
	if err != nil {
		t.Fatal(err)
	}
	if root.PresenceFields != nil || child.PresenceFields != nil {
		t.Fatalf("compiler plan was mutated: root=%v child=%v", root.PresenceFields, child.PresenceFields)
	}
	if !reflect.DeepEqual(refined.Root.PresenceFields, []string{"Id", "Name"}) || refined.Root.Relations[0].Child.PresenceFields != nil {
		t.Fatalf("refined presence fields root=%v child=%v", refined.Root.PresenceFields, refined.Root.Relations[0].Child.PresenceFields)
	}
}

func TestApplyContractOptions(t *testing.T) {
	t.Run("generated", func(t *testing.T) {
		component := specComponentWithContractTypes()
		compiled := &Result{Component: &component, ContractTypeOverrides: ContractTypeOverrides{Input: "AuthoredInput"}}
		input := &gen.Input{
			Component: compiled.Component.Clone(),
			Contracts: gen.ContractReferences{Input: &gen.ContractReference{}, Output: &gen.ContractReference{}},
			Views:     gen.ViewReferences{gen.RootViewPath: &gen.ViewReference{}, "view:Other": &gen.ViewReference{}},
		}
		generation := newHandlerGeneration(compiled, input, Options{Contracts: ContractsGenerated})
		if err := generation.applyContractOption(); err != nil {
			t.Fatal(err)
		}
		if input.Contracts.Input != nil || input.Contracts.Output != nil || input.Views[gen.RootViewPath] != nil || input.Views["view:Other"] == nil {
			t.Fatalf("generated authority = contracts:%+v views:%+v", input.Contracts, input.Views)
		}
		if input.Component.Settings.InputType != "AuthoredInput" || input.Component.Settings.OutputType != "" {
			t.Fatalf("generated contract types = %+v", input.Component.Settings)
		}
	})
	t.Run("linked", func(t *testing.T) {
		generation := newHandlerGeneration(&Result{Component: writeGenerationComponent()}, &gen.Input{}, Options{Contracts: ContractsLinked})
		if err := generation.applyContractOption(); err == nil || !strings.Contains(err.Error(), "package-authoritative") {
			t.Fatalf("linked contract error = %v", err)
		}
	})
}

func specComponentWithContractTypes() spec.Component {
	return spec.Component{Name: "Events", Settings: &spec.Settings{InputType: "AuthoredInput", OutputType: "PackageOutput"}}
}
