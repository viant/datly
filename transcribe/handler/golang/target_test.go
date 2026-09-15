package golang

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestLowerRootPatchGeneratesCompilableTypedHandler(t *testing.T) {
	semantic := compilePatchPlan(t, false)
	asset, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
		Records: rootRecordTypes(semantic, "[]*Event", "[]*CurrentEvent"),
	})
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, expected := range []string{
		"map[int64]*CurrentEvent",
		`"parameter:\",kind=dml\""`,
		`"parameter:\",kind=sequencer\""`,
		"dependencies.Sequencer.Allocate(ctx, \"EVENTS\", input.Events, \"Id\")",
		"dependencies.DML.Update(\"EVENTS\", record0)",
		"dependencies.DML.Insert(\"EVENTS\", record0)",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("generated source missing %q:\n%s", expected, text)
		}
	}
	compileGeneratedHandler(t, source, `package events

type Event struct {
	Id   *int64
	Name string
}

type CurrentEvent struct {
	Id   *int64
	Name string
}

type Input struct {
	Events        []*Event
	CurrentEvents []*CurrentEvent
}

type Output struct {
	Data []*Event
}
`)
}

func TestLowerPostAndPutGenerateDirectTypedDML(t *testing.T) {
	tests := []struct {
		name      string
		operation plan.Operation
		method    string
	}{
		{name: "post", operation: plan.OperationPost, method: "Insert"},
		{name: "put", operation: plan.OperationPut, method: "Update"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			semantic := rootSemanticPlan(testCase.operation, false)
			config := Config{
				Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
				Records: rootRecordTypes(semantic, "[]*Event", ""),
			}
			asset, err := Lower(semantic, config)
			if err != nil {
				t.Fatalf("Lower() error = %v", err)
			}
			source, err := asset.Source()
			if err != nil {
				t.Fatal(err)
			}
			text := string(source)
			if !strings.Contains(text, "dependencies.DML."+testCase.method+`("EVENTS", record0)`) {
				t.Fatalf("generated source does not call %s:\n%s", testCase.method, text)
			}
			for _, forbidden := range []string{"CurrentKey", "currentByKey", "recordExists", `"fmt"`} {
				if strings.Contains(text, forbidden) {
					t.Fatalf("generated %s source contains PATCH-only %q:\n%s", testCase.operation, forbidden, text)
				}
			}
			compileGeneratedHandler(t, source, `package events

type Event struct {
	Id   *int64
	Name string
}

type Input struct { Events []*Event }
type Output struct { Data []*Event }
`)
			match := "insert-only"
			semantic.Root.Write.Existing = plan.ActionUpdate
			if testCase.operation == plan.OperationPut {
				match = "update-only"
				semantic.Root.Write.Existing = plan.ActionUpdate
				semantic.Root.Write.Missing = plan.ActionInsert
			}
			if _, err = Lower(semantic, config); err == nil || !strings.Contains(err.Error(), match) {
				t.Fatalf("contradictory %s policy error = %v, want %q", testCase.operation, err, match)
			}
		})
	}
}

func TestLowerPostDoesNotImportRecordTypeUsedOnlyByLocalContracts(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPost, false)
	asset, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
		Records: rootRecordTypes(semantic, "[]*model.Event", ""),
		Imports: []spec.ImportSpec{{Alias: "model", Package: "github.com/viant/datly/transcribe/testdata/comprehensive"}},
	})
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(source), "transcribe/testdata/comprehensive") {
		t.Fatalf("generated POST handler imported a type referenced only by local contracts:\n%s", source)
	}
	compileGeneratedHandler(t, source, `package events

import model "github.com/viant/datly/transcribe/testdata/comprehensive"

type Input struct { Events []*model.Event }
type Output struct { Data []*model.Event }
`)
}

func TestLowerPutCurrentDoesNotImportUnusedRecordTypes(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	semantic.Operation = plan.OperationPut
	semantic.Root.Sequence = nil
	semantic.Root.Write = fixtureWritePolicy(plan.OperationPut, semantic.Root.InputPath, 0)
	asset, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
		Imports: []spec.ImportSpec{{Alias: "model", Package: "github.com/viant/datly/transcribe/testdata/comprehensive"}},
		Records: rootRecordTypes(semantic, "[]*model.Event", "[]*model.Event"),
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(source), "transcribe/testdata/comprehensive") {
		t.Fatalf("direct PUT imported types referenced only by local contracts:\n%s", source)
	}
	compileGeneratedHandler(t, source, `package events
import model "github.com/viant/datly/transcribe/testdata/comprehensive"
type Input struct { Events []*model.Event; CurrentEvents []*model.Event }
type Output struct { Data []*model.Event }
`)
}

func TestLowerRootPatchGeneratesComparableCompoundKey(t *testing.T) {
	semantic := compilePatchPlan(t, true)
	asset, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
		Records: rootRecordTypes(semantic, "[]*Event", "[]*CurrentEvent"),
	})
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if !strings.Contains(text, "type eventsHandlerContractKey struct") ||
		!strings.Contains(text, "map[eventsHandlerContractKey]*CurrentEvent") ||
		!strings.Contains(text, "TenantId: value.TenantId") {
		t.Fatalf("compound generated source:\n%s", text)
	}
	compileGeneratedHandler(t, source, `package events

type Event struct {
	Id       *int64
	TenantId int64
}

type CurrentEvent struct {
	Id       *int64
	TenantId int64
}

type Input struct {
	Events        []*Event
	CurrentEvents []*CurrentEvent
}

type Output struct {
	Data []*Event
}
`)
}

func TestLowerUsesCanonicalCurrentKeyFieldIndependentlyFromRoot(t *testing.T) {
	semantic := compilePatchPlan(t, false)
	semantic.Root.Current.Keys[0].Field = "EventId"
	asset, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
		Records: rootRecordTypes(semantic, "[]*Event", "[]*CurrentEvent"),
	})
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if !strings.Contains(text, "return *value.EventId, true") || !strings.Contains(text, "return *value.Id, true") {
		t.Fatalf("generated key functions do not preserve distinct canonical fields:\n%s", text)
	}
	compileGeneratedHandler(t, source, `package events

type Event struct { Id *int64 }
type CurrentEvent struct { EventId *int64 }
type Input struct {
	Events []*Event
	CurrentEvents []*CurrentEvent
}
type Output struct { Data []*Event }
`)
}

func TestLowerRejectsContradictoryWritePolicy(t *testing.T) {
	semantic := compilePatchPlan(t, false)
	semantic.Root.Write.Allowed = []plan.Action{plan.ActionInsert}
	_, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
		Records: rootRecordTypes(semantic, "[]*Event", "[]*CurrentEvent"),
	})
	if err == nil || !strings.Contains(err.Error(), `action "update" is not allowed`) {
		t.Fatalf("Lower() error = %v", err)
	}
}

func TestLowerRejectsKnownNonComparableKeyType(t *testing.T) {
	for _, typeName := range []string{"[]byte", "map[string]int", "func()", "struct{ Values []string }"} {
		t.Run(typeName, func(t *testing.T) {
			semantic := compilePatchPlan(t, false)
			semantic.Root.Keys[0].Type = spec.TypeRef{Name: typeName}
			semantic.Root.Current.Keys[0].Type = spec.TypeRef{Name: typeName}
			_, err := Lower(semantic, Config{
				Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
				Records: rootRecordTypes(semantic, "[]*Event", "[]*CurrentEvent"),
			})
			if err == nil || !strings.Contains(err.Error(), "is not comparable") {
				t.Fatalf("Lower() error = %v", err)
			}
		})
	}
}

func TestLowerOmitsUnusedSequencerCapability(t *testing.T) {
	semantic := compilePatchPlan(t, false)
	semantic.Root.Sequence = nil
	asset, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
		Records: rootRecordTypes(semantic, "[]*Event", "[]*CurrentEvent"),
	})
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(source), "Sequencer") || strings.Contains(string(source), "kind=sequencer") {
		t.Fatalf("unused sequencer capability was generated:\n%s", source)
	}
}

func TestLowerRejectsCurrentKeyTypeMismatch(t *testing.T) {
	semantic := compilePatchPlan(t, false)
	semantic.Root.Current.Keys[0].Type = spec.TypeRef{Name: "string"}
	_, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
		Records: rootRecordTypes(semantic, "[]*Event", "[]*CurrentEvent"),
	})
	if err == nil || !strings.Contains(err.Error(), "current key types differ") {
		t.Fatalf("Lower() error = %v", err)
	}
}

func TestLowerAvoidsSystemImportAliasCollisions(t *testing.T) {
	semantic := compilePatchPlan(t, false)
	asset, err := Lower(semantic, Config{
		Package: "events", Factory: "NewFmt", Handler: "fmt", InputType: "Input", OutputType: "Output",
		Records: rootRecordTypes(semantic, "[]*Event", "[]*CurrentEvent"),
	})
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), `fmt2 "fmt"`) || !strings.Contains(string(source), "type fmt struct") {
		t.Fatalf("generated alias collision was not resolved:\n%s", source)
	}
}

func TestLowerRejectsConfiguredImportDeclarationCollision(t *testing.T) {
	semantic := compilePatchPlan(t, false)
	_, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
		Records: rootRecordTypes(semantic, "[]*Event", "[]*CurrentEvent"),
		Imports: []spec.ImportSpec{{Alias: "eventsHandlerContract", Package: "example.com/contracts"}},
	})
	if err == nil || !strings.Contains(err.Error(), "conflicts with import") {
		t.Fatalf("Lower() error = %v", err)
	}
}

func TestLowerRejectsIncompleteRecursivePlan(t *testing.T) {
	semantic := compilePatchPlan(t, false)
	semantic.Root.Relations = []*plan.RelationPlan{{Identity: "view:Items"}}
	_, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output",
		Records: rootRecordTypes(semantic, "[]*Event", "[]*CurrentEvent"),
	})
	if err == nil || !strings.Contains(err.Error(), "incomplete relation") {
		t.Fatalf("Lower() error = %v", err)
	}
}

func TestLowerPersistsThroughCanonicalGeneratorProduct(t *testing.T) {
	component := patchComponent(false)
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	bindings := patchViewBindings(t, component)
	targetPackage := "example.com/generated/events"
	generatedPlan, err := gen.New(gen.Input{
		Component: component, TargetPackage: targetPackage, ViewBindings: bindings,
	}).Plan()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	asset, err := Lower(semantic, Config{
		Package: "events", Factory: "NewEventsHandler",
		InputType: generatedPlan.Input.Type, OutputType: generatedPlan.Output.Type, Imports: generatedPlan.Imports,
		Records: rootRecordTypes(semantic,
			generatedInputFieldType(t, generatedPlan, semantic.Root.InputPath),
			generatedInputFieldType(t, generatedPlan, semantic.Root.Current.InputPath)),
	})
	if err != nil {
		t.Fatalf("Lower() error = %v", err)
	}
	hooks, err := ScaffoldHooks(Config{
		Package: "events", InputType: generatedPlan.Input.Type, OutputType: generatedPlan.Output.Type,
	})
	if err != nil {
		t.Fatalf("ScaffoldHooks() error = %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generationInput := gen.Input{
		Component: component, TargetPackage: targetPackage, ViewBindings: bindings,
		ContractHandler: &gen.ContractHandlerAsset{
			Factory: asset.Factory, File: asset.File,
		},
		HookScaffold: &gen.HookScaffoldAsset{File: hooks},
	}
	packageDir := filepath.Join(root, "events")
	result, err := gen.New(generationInput).Generate(packageDir)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if result.Plan.ContractHandler == nil || result.Plan.Handler != "NewEventsHandler" {
		t.Fatalf("generated handler plan = %+v, identity = %q", result.Plan.ContractHandler, result.Plan.Handler)
	}
	hookPath := filepath.Join(packageDir, result.Plan.HookScaffold.Destination)
	hookSource, err := os.ReadFile(hookPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(hookSource), "func (input *EventsInput) Init") ||
		!strings.Contains(string(hookSource), "func (output *EventsOutput) Finalize") {
		t.Fatalf("hook scaffold:\n%s", hookSource)
	}
	userSource := append(append([]byte(nil), hookSource...), []byte("\nconst UserHookPreserved = true\n")...)
	if err = os.WriteFile(hookPath, userSource, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err = gen.New(generationInput).Generate(packageDir); err != nil {
		t.Fatalf("regenerate with user hook: %v", err)
	}
	preserved, err := os.ReadFile(hookPath)
	if err != nil || string(preserved) != string(userSource) {
		t.Fatalf("user hook was rewritten: err=%v\n%s", err, preserved)
	}
	generationInput.HookScaffold = nil
	if _, err = gen.New(generationInput).Generate(packageDir); err != nil {
		t.Fatalf("regenerate without hook option: %v", err)
	}
	preserved, err = os.ReadFile(hookPath)
	if err != nil || string(preserved) != string(userSource) {
		t.Fatalf("disabled hook scaffold removed or rewrote user file: err=%v\n%s", err, preserved)
	}
	manifest, err := os.ReadFile(filepath.Join(packageDir, ".datly-gen.json"))
	if err != nil || strings.Contains(string(manifest), "events_hooks.go") {
		t.Fatalf("hook scaffold entered generated manifest: err=%v\n%s", err, manifest)
	}
	for _, emitted := range result.Files {
		if filepath.Base(emitted.Path) == "events_hooks.go" {
			t.Fatalf("user hook reported as generated artifact: %+v", emitted)
		}
	}
	runtimeTest := generatedGoHandlerRuntimeTest(generatedPlan.Input.Type, generatedPlan.Output.Type)
	if err = os.WriteFile(filepath.Join(root, "events", "handler_runtime_test.go"), []byte(runtimeTest), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated canonical handler package failed: %v\n%s", runErr, output)
	}
}

func generatedInputFieldType(t *testing.T, generatedPlan *gen.Plan, path plan.FieldPath) string {
	t.Helper()
	if generatedPlan == nil || len(path) != 2 || path[0] != "Input" {
		t.Fatalf("invalid generated input field path: %v", path)
	}
	for _, field := range generatedPlan.Input.Fields {
		if field.Name == path[1] {
			return field.Type
		}
	}
	t.Fatalf("generated input field %q was not found", path[1])
	return ""
}

func rootRecordTypes(semantic *plan.Plan, value, current string) []RecordType {
	return []RecordType{{
		Identity: semantic.Root.Identity, Path: append(plan.FieldPath(nil), semantic.Root.InputPath...),
		Value: value, Current: current,
	}}
}

func generatedGoHandlerRuntimeTest(inputType, outputType string) string {
	source := `package events

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestGeneratedGoHandlerUsesUnifiedEngine(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err = db.ExecContext(ctx, "CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, "INSERT INTO EVENTS(ID, NAME) VALUES (1, 'before')"); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{
		Routes: []*spec.Route{{Method: "PATCH", Path: "/events"}},
		Parameters: []*spec.Parameter{
			{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}},
			{Name: "CurrentEvents", Source: spec.BindSource{Kind: "view", Name: "CurrentEvents"}, Cardinality: string(spec.CardinalityMany)},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, EmitOutput: true},
		},
		Views: []*spec.View{{
			Name: "CurrentEvents", TypeName: "CurrentEvent", Cardinality: spec.CardinalityMany,
			Source: &spec.ViewSource{SQL: "SELECT ID, NAME FROM EVENTS"},
			Columns: []*spec.Column{
				{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}, PrimaryKey: true},
				{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
			},
		}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf({{INPUT}}{}), OutputType: reflect.TypeOf({{OUTPUT}}{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	routeInput, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/events"})
	if !ok {
		t.Fatal("compiled route input contract was not found")
	}
	viewProvider, err := viewprovider.New(viewprovider.Config{
		Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: &dsql.SQLComponent{DB: db},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("PATCH", "/events", strings.NewReader(` + "`" + `{"Data":[{"id":1,"name":"after"},{"name":"inserted"}]}` + "`" + `))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := handlerengine.New().Execute(ctx, handlerengine.Request{
		Input: routeInput,
		Scope: scope, Providers: []locator.Provider{viewProvider}, DataSource: sqldml.Source{DB: db},
		Handler: customhandler.New[{{INPUT}}, {{OUTPUT}}](NewEventsHandler()),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := actual.(*{{OUTPUT}})
	if len(result.Data) != 2 || result.Data[0].Id == nil || *result.Data[0].Id != 1 ||
		result.Data[1].Id == nil || *result.Data[1].Id != 2 {
		t.Fatalf("generated output = %+v", result.Data)
	}
	var names string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NAME, ',') FROM (SELECT NAME FROM EVENTS ORDER BY ID)").Scan(&names); err != nil || names != "after,inserted" {
		t.Fatalf("patched rows = %q err=%v", names, err)
	}
}
`
	return strings.NewReplacer("{{INPUT}}", inputType, "{{OUTPUT}}", outputType).Replace(source)
}

func compilePatchPlan(t *testing.T, compound bool) *plan.Plan {
	t.Helper()
	return rootSemanticPlan(plan.OperationPatch, compound)
}

func patchComponent(compound bool) *spec.Component {
	pointerInt := spec.TypeRef{Name: "int64", Pointer: true}
	columns := []*spec.Column{
		{Name: "ID", Source: "ID", Type: pointerInt, PrimaryKey: true},
		{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
	}
	currentColumns := []*spec.Column{columns[0].Clone(), columns[1].Clone()}
	if compound {
		tenant := &spec.Column{Name: "TENANT_ID", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}, PrimaryKey: true}
		columns = append(columns, tenant)
		currentColumns = append(currentColumns, tenant.Clone())
	}
	component := &spec.Component{
		Name:   "Events",
		Routes: []*spec.Route{{Method: "PATCH", Path: "/events"}},
		Parameters: []*spec.Parameter{
			{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}, TypeExpr: "[]*Event", Cardinality: string(spec.CardinalityMany)},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, TypeExpr: "[]*Event"},
			{Name: "CurrentEvents", Source: spec.BindSource{Kind: "view", Name: "CurrentEvents"}, TypeExpr: "[]*CurrentEvent", Cardinality: string(spec.CardinalityMany)},
		},
		RootView: &spec.View{Name: "Events", TypeName: "Event", Source: &spec.ViewSource{Table: "EVENTS"}, Columns: columns},
		Views:    []*spec.View{{Name: "CurrentEvents", TypeName: "CurrentEvent", Columns: currentColumns}},
	}
	return component
}

func patchViewBindings(t *testing.T, component *spec.Component) gen.ViewBindings {
	t.Helper()
	currentIdentity, err := component.Views[0].Identity()
	if err != nil {
		t.Fatal(err)
	}
	return gen.ViewBindings{component.Parameters[2].Identity(): currentIdentity}
}

func compileGeneratedHandler(t *testing.T, handlerSource []byte, contractSource string) {
	t.Helper()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if err := os.WriteFile(filepath.Join(root, "handler_gen.go"), handlerSource, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "contracts.go"), []byte(contractSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated Go handler did not compile: %v\n%s\n%s", err, output, handlerSource)
	}
}
