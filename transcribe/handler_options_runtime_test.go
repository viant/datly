package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	tcolumn "github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestCompilerGenerateRunsCanonicalWritesThroughUnifiedEngine(t *testing.T) {
	tests := []struct {
		name        string
		operation   WriteOperation
		typeExpr    string
		cardinality string
		template    string
	}{
		{
			name: "post many", operation: WritePost, typeExpr: "[]*EventsView", cardinality: ".Cardinality('Many')",
			template: `$sequencer.Allocate("EVENTS", $Input.Events, "Id")
#foreach($RecEvents in $Input.Events)
  #set($datlyWriteIndex0 = $foreach.Index)
  #if($writeHooks.Present("Events", $datlyWriteIndex0))
    $writeHooks.Entity("Events", $datlyWriteIndex0)
    #set($RecEvents = $Input.Events[$datlyWriteIndex0])
    $dml.Insert("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
  #end
#end
#set($Output.Data = $Input.Events)`,
		},
		{
			name: "put one", operation: WritePut, typeExpr: "*EventsView",
			template: `#if($writeHooks.Present("Events"))
  $writeHooks.Entity("Events")
  #set($RecEvents = $Input.Events)
  $dml.Update("EVENTS", $writeHooks.Value("Events"));
#end
#set($Output.Data = $Input.Events)`,
		},
		{
			name: "patch many", operation: WritePatch, typeExpr: "[]*EventsView", cardinality: ".Cardinality('Many')",
			template: `$sequencer.Allocate("EVENTS", $Input.Events, "Id")
#set($CurrentEventsById = $Input.CurrentEvents.IndexBy("Id"))
#foreach($RecEvents in $Input.Events)
  #set($datlyWriteIndex0 = $foreach.Index)
  #if($writeHooks.Present("Events", $datlyWriteIndex0))
    $writeHooks.Entity("Events", $datlyWriteIndex0)
    #set($RecEvents = $Input.Events[$datlyWriteIndex0])
    #if($CurrentEventsById.HasKey($RecEvents.Id) == true)
      $dml.Update("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
    #else
      $dml.Insert("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
    #end
  #end
#end
#set($Output.Data = $Input.Events)`,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			verifyGeneratedWrite(t, testCase.operation, testCase.typeExpr, testCase.cardinality, testCase.template)
		})
	}
}

func TestTranscribeRunsGeneratedGoPatchThroughUnifiedEngine(t *testing.T) {
	t.Parallel()
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE EVENTS (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		NAME TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create EVENTS: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dql := `#setting($_ = $route('/events', 'PATCH'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())
#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many') /*
SELECT ID, NAME FROM EVENTS
WHERE ID IN (#foreach($event in $Events)$event.Id#if($foreach.HasNext),#end#end)
*/)
#define($_ = $Status<int>(output/status).Output())
#define($_ = $Data<[]*EventsView>(output/body))
SELECT ID, NAME FROM EVENTS`
	generated, err := NewCompiler().Transcribe(ctx, Request{
		Source: &Source{
			Scope: "example.com/generated/events", Name: "Events", Connector: "main", Text: dql,
			Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		},
		Destination: root,
		Options: Options{Handler: HandlerOptions{
			Target: HandlerGo, Operation: WritePatch, Current: "CurrentEvents",
			Go: GoHandlerOptions{
				Factory: "NewEventsPatchHandler", Handler: "eventsPatchContract", Destination: "events_patch_gen.go",
			},
			Hooks: HookOptions{Scaffold: true, Destination: "events_patch_hooks.go"},
		}},
	})
	if err != nil {
		t.Fatalf("Transcribe() error = %v", err)
	}
	if generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.ContractHandler.Factory != "NewEventsPatchHandler" ||
		generated.Result.Plan.ContractHandler.Destination != "events_patch_gen.go" || generated.Result.Plan.VeltyHandler != nil ||
		generated.Result.Plan.HookScaffold == nil || generated.Result.Plan.HookScaffold.Destination != "events_patch_hooks.go" {
		t.Fatalf("generated Go handler products = contract:%+v velty:%+v hooks:%+v",
			generated.Result.Plan.ContractHandler, generated.Result.Plan.VeltyHandler, generated.Result.Plan.HookScaffold)
	}
	runtimeSource := strings.Replace(generatedGoWriteRuntimeSource(WritePatch), "NewEventsHandler()", "NewEventsPatchHandler()", 1)
	if err = os.WriteFile(filepath.Join(root, "generated", "generated_write_test.go"), []byte(runtimeSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated Go PATCH module failed: %v\n%s", runErr, output)
	}
}

func TestTranscribedPatchPreservesOmittedGeneratedViewColumns(t *testing.T) {
	t.Parallel()
	for _, target := range []HandlerTarget{HandlerGo, HandlerVelty} {
		t.Run(string(target), func(t *testing.T) {
			harness := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := harness.ExecStatements(ctx, `CREATE TABLE EVENTS (
				ID INTEGER PRIMARY KEY AUTOINCREMENT,
				NAME TEXT NOT NULL,
				NOTE TEXT
			)`); err != nil {
				t.Fatalf("create EVENTS: %v", err)
			}
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			dql := `#setting($_ = $route('/events', 'PATCH'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())
#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many') /*
SELECT ID, NAME, NOTE FROM EVENTS
WHERE ID IN (#foreach($event in $Events)$event.Id#if($foreach.HasNext),#end#end)
*/)
#define($_ = $Status<int>(output/status).Output())
#define($_ = $Data<[]*EventsView>(output/body))
SELECT ID, NAME, NOTE FROM EVENTS`
			generated, err := NewCompiler().Transcribe(ctx, Request{
				Source: &Source{
					Scope: "example.com/generated/events", Name: "Events", Connector: "main", Text: dql,
					Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
				},
				Destination: root,
				Options: Options{Handler: HandlerOptions{
					Target: target, Operation: WritePatch, Current: "CurrentEvents",
				}},
			})
			if err != nil {
				t.Fatalf("Transcribe() error = %v", err)
			}
			var markerFields []string
			for _, view := range generated.Result.Plan.Views {
				if view.Type == generated.Result.Plan.RootViewType {
					markerFields = view.SetMarkerFields
					break
				}
			}
			if !reflect.DeepEqual(markerFields, []string{"Id", "Name", "Note"}) {
				t.Fatalf("set marker fields = %v", markerFields)
			}
			testSource := generatedPatchPresenceRuntimeSource(t, target)
			if err = os.WriteFile(filepath.Join(root, "generated", "generated_presence_test.go"), []byte(testSource), 0o644); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("go", "test", "-mod=mod", "./...")
			command.Dir = root
			if output, runErr := command.CombinedOutput(); runErr != nil {
				t.Fatalf("generated %s presence module failed: %v\n%s", target, runErr, output)
			}
		})
	}
}

func TestTranscribeRunsGeneratedGoPostAndPutThroughUnifiedEngine(t *testing.T) {
	tests := []struct {
		name        string
		operation   WriteOperation
		typeExpr    string
		cardinality string
	}{
		{name: "post many", operation: WritePost, typeExpr: "[]*EventsView", cardinality: ".Cardinality('Many')"},
		{name: "put one", operation: WritePut, typeExpr: "*EventsView"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			verifyGeneratedGoWrite(t, testCase.operation, testCase.typeExpr, testCase.cardinality)
		})
	}
}

func TestGenerateRunsPatchWithUnsignedPointerKey(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	component := writeGenerationComponent()
	component.Key = spec.Key{Kind: spec.KindComponent, Scope: "example.com/generated/events", Name: "Events"}
	component.RootView.Columns[0].Type.Name = "uint64"
	component.RootView.Columns[0].Type.Pointer = true
	component.Views[0].Columns[0].Type.Name = "uint64"
	component.Views[0].Columns[0].Type.Pointer = true
	component.Parameters[3].TypeExpr = ""
	component.Views[0].TypeName = "CurrentEventsView"
	component.Routes = []*spec.Route{{Method: "PATCH", Path: "/events"}}
	asset, err := generateVeltyTarget(component, generatedViewBindings(t, component, generatedViewBinding{param: 3, view: 0}), &HandlerOptions{Operation: WritePatch, Current: "CurrentEvents"})
	if err != nil {
		t.Fatalf("generateVeltyTarget() error = %v", err)
	}
	generated, err := gen.New(gen.Input{
		Component: component, TargetPackage: "example.com/generated/generated", VeltyHandler: asset,
	}).Generate(filepath.Join(root, "generated"))
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if generated.Plan.VeltyHandler == nil || !strings.Contains(generated.Plan.VeltyHandler.Template, `HasKey($RecEvents.Id)`) {
		t.Fatalf("generated Velty template = %+v", generated.Plan.VeltyHandler)
	}
	testSource := generatedWriteRuntimeSourceForKey(WritePatch, "uint64")
	if err = os.WriteFile(filepath.Join(root, "generated", "generated_write_test.go"), []byte(testSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated unsigned patch module failed: %v\n%s", runErr, output)
	}
}

func TestGenerateRunsCompoundVeltyPatchThroughUnifiedEngine(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	component := writeGenerationComponent()
	component.Key = spec.Key{Kind: spec.KindComponent, Scope: "example.com/generated/events", Name: "Events"}
	component.Routes = []*spec.Route{{Method: "PATCH", Path: "/events"}}
	component.Parameters[3].TypeExpr = ""
	component.Views[0].TypeName = "CurrentEventsView"
	tenant := &spec.Column{Name: "TENANT_ID", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}, PrimaryKey: true}
	component.RootView.Columns = append(component.RootView.Columns, tenant)
	component.Views[0].Columns = append(component.Views[0].Columns, tenant.Clone())
	asset, err := generateVeltyTarget(component, generatedViewBindings(t, component, generatedViewBinding{param: 3, view: 0}), &HandlerOptions{Operation: WritePatch, Current: "CurrentEvents"})
	if err != nil {
		t.Fatalf("generateVeltyTarget() error = %v", err)
	}
	rootIdentity, err := component.RootView.Identity()
	if err != nil {
		t.Fatal(err)
	}
	generated, err := gen.New(gen.Input{
		Component: component, TargetPackage: "example.com/generated/generated", VeltyHandler: asset,
		SetMarkerViews: map[string]bool{rootIdentity: true},
	}).Generate(filepath.Join(root, "generated"))
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if generated.Plan.VeltyHandler == nil ||
		!strings.Contains(generated.Plan.VeltyHandler.Template, `$index.Build("CurrentEventsByIdAndTenantId"`) ||
		!strings.Contains(generated.Plan.VeltyHandler.Template, `$index.Has("CurrentEventsByIdAndTenantId"`) {
		t.Fatalf("generated compound Velty template = %+v", generated.Plan.VeltyHandler)
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "generated_compound_write_test.go"), []byte(generatedCompoundPatchRuntimeSource(t)), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated compound Velty PATCH module failed: %v\n%s", runErr, output)
	}
}

func TestGenerateRunsRecursivePatchThroughUnifiedEngine(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	component, child, detail := recursiveWriteGenerationComponent()
	component.Key = spec.Key{Kind: spec.KindComponent, Scope: "example.com/generated/orders", Name: "Orders"}
	component.Routes = []*spec.Route{{Method: "PATCH", Path: "/orders"}}
	component.Views[0].Source = &spec.ViewSource{SQL: "SELECT ID, NAME FROM ORDERS WHERE ID = 1"}
	component.Views[1].Source = &spec.ViewSource{SQL: "SELECT ID, ORDER_ID, NAME FROM ITEMS WHERE ID = 1"}
	component.Views[2].Source = &spec.ViewSource{SQL: "SELECT ID, ITEM_ID, NOTE FROM DETAILS WHERE ID = 1"}
	childIdentity, err := child.Identity()
	if err != nil {
		t.Fatal(err)
	}
	detailIdentity, err := detail.Identity()
	if err != nil {
		t.Fatal(err)
	}
	rootIdentity, err := component.RootView.Identity()
	if err != nil {
		t.Fatal(err)
	}
	asset, err := generateVeltyTarget(component, generatedViewBindings(t, component,
		generatedViewBinding{param: 2, view: 0},
		generatedViewBinding{param: 3, view: 1},
		generatedViewBinding{param: 4, view: 2}), &HandlerOptions{
		Operation: WritePatch, Current: "CurrentOrders",
		Currents: []CurrentBinding{
			{ViewIdentity: childIdentity, Param: "CurrentItems"},
			{ViewIdentity: detailIdentity, Param: "CurrentDetails"},
		},
	})
	if err != nil {
		t.Fatalf("generateVeltyTarget() error = %v", err)
	}
	generated, err := gen.New(gen.Input{
		Component: component, TargetPackage: "example.com/generated/generated", VeltyHandler: asset,
		SetMarkerViews: map[string]bool{rootIdentity: true, childIdentity: true, detailIdentity: true},
	}).Generate(filepath.Join(root, "generated"))
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if generated.Plan.VeltyHandler == nil || !strings.Contains(generated.Plan.VeltyHandler.Template, `Items/Id`) {
		t.Fatalf("generated recursive Velty template = %+v", generated.Plan.VeltyHandler)
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "generated_recursive_write_test.go"), []byte(generatedRecursiveWriteRuntimeSource()), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated recursive patch module failed: %v\n%s", runErr, output)
	}
}

func verifyGeneratedWrite(t *testing.T, operation WriteOperation, typeExpr, cardinality, wantTemplate string) {
	t.Helper()
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE EVENTS (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		NAME TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create EVENTS: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	compiler := NewCompiler()
	dql := `#setting($_ = $route('/events', 'GET'))
#define($_ = $Events<{{TYPE}}>(body/Data){{CARDINALITY}}.Required())
{{CURRENT}}
#define($_ = $Status<int>(output/status).Output())
#define($_ = $Data<{{TYPE}}>(output/body))
SELECT ID, NAME FROM EVENTS`
	current := ""
	intent := HandlerOptions{Target: HandlerVelty, Operation: operation}
	if operation == WritePatch {
		current = `#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many') /*
SELECT ID, NAME FROM EVENTS
WHERE ID IN (#foreach($event in $Events)$event.Id#if($foreach.HasNext),#end#end)
*/)`
		intent.Current = "CurrentEvents"
	}
	dql = strings.NewReplacer("{{TYPE}}", typeExpr, "{{CARDINALITY}}", cardinality, "{{CURRENT}}", current).Replace(dql)
	compiled, err := compiler.Compile(ctx, &Source{
		Scope: "example.com/generated/events", Name: "Events", Connector: "main",
		Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		Text: dql,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	rootView := compiled.Component.RootView
	if rootView == nil || len(rootView.Columns) != 2 || !rootView.Columns[0].PrimaryKey {
		t.Fatalf("compiled root view did not retain database constraints: %+v", rootView)
	}
	generated, err := compiler.Transcribe(ctx, Request{
		Source: &Source{
			Scope: "example.com/generated/events", Name: "Events", Connector: "main",
			Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
			Text: dql,
		},
		Destination: root,
		Options:     Options{Handler: intent},
	})
	if err != nil {
		t.Fatalf("generateCompiled() error = %v", err)
	}
	if generated.Result.Plan.VeltyHandler == nil || generated.Result.Plan.VeltyHandler.Template != wantTemplate {
		t.Fatalf("generated Velty template:\n%s\nwant:\n%s", generated.Result.Plan.VeltyHandler.Template, wantTemplate)
	}
	testSource := generatedWriteRuntimeSource(operation)
	if err = os.WriteFile(filepath.Join(root, "generated", "generated_write_test.go"), []byte(testSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated %s module failed: %v\n%s", operation, runErr, output)
	}
}

func verifyGeneratedGoWrite(t *testing.T, operation WriteOperation, typeExpr, cardinality string) {
	t.Helper()
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE EVENTS (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		NAME TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create EVENTS: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dql := strings.NewReplacer("{{TYPE}}", typeExpr, "{{CARDINALITY}}", cardinality).Replace(`#setting($_ = $route('/events', 'GET'))
#define($_ = $Events<{{TYPE}}>(body/Data){{CARDINALITY}}.Required())
#define($_ = $Status<int>(output/status).Output())
#define($_ = $Data<{{TYPE}}>(output/body))
SELECT ID, NAME FROM EVENTS`)
	generated, err := NewCompiler().Transcribe(ctx, Request{
		Source: &Source{
			Scope: "example.com/generated/events", Name: "Events", Connector: "main", Text: dql,
			Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		},
		Destination: root,
		Options: Options{Handler: HandlerOptions{
			Target: HandlerGo, Operation: operation,
		}},
	})
	if err != nil {
		t.Fatalf("Transcribe() error = %v", err)
	}
	if generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.VeltyHandler != nil {
		t.Fatalf("generated Go products = contract:%+v velty:%+v", generated.Result.Plan.ContractHandler, generated.Result.Plan.VeltyHandler)
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "generated_write_test.go"), []byte(generatedContractWriteRuntimeSource(operation)), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated Go %s module failed: %v\n%s", operation, runErr, output)
	}
}

func generatedWriteRuntimeSource(operation WriteOperation) string {
	return generatedWriteRuntimeSourceForKey(operation, "int64")
}

func generatedContractWriteRuntimeSource(operation WriteOperation) string {
	source := generatedWriteRuntimeSourceForKey(operation, "int64")
	source = strings.Replace(source,
		`handlerengine "github.com/viant/datly/runtime/handler/engine"`,
		`customhandler "github.com/viant/datly/runtime/handler/custom"
	handlerengine "github.com/viant/datly/runtime/handler/engine"`, 1)
	source = strings.Replace(source,
		`handler, err := NewEventsHandler()
	if err != nil {
		t.Fatal(err)
	}`,
		`handler := customhandler.New[EventsInput, EventsOutput](NewEventsHandler())`, 1)
	return source
}

func generatedGoWriteRuntimeSource(operation WriteOperation, withCurrent ...bool) string {
	source := generatedWriteRuntimeSourceForKey(operation, "int64", withCurrent...)
	source = strings.Replace(source,
		`handlerengine "github.com/viant/datly/runtime/handler/engine"`,
		`writerhandler "github.com/viant/datly/runtime/handler/writer"
	handlerengine "github.com/viant/datly/runtime/handler/engine"`, 1)
	source = strings.Replace(source,
		`handler, err := NewEventsHandler()
	if err != nil {
		t.Fatal(err)
	}`,
		`handler, err := writerhandler.New(artifact.Component, reflect.TypeOf(EventsInput{}), reflect.TypeOf(EventsOutput{}), "`+string(operation)+`")
	if err != nil {
		t.Fatal(err)
	}`, 1)
	source = strings.Replace(source,
		`component := &spec.Component{Routes:`,
		`component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeOf(EventsInput{}).PkgPath(), Name: "Events"}, Name: "Events", Settings: &spec.Settings{Mutation: "`+string(operation)+`"}, RootView: &spec.View{Name: "Events", Source: &spec.ViewSource{Table: "EVENTS"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int64"}, PrimaryKey: true, AutoIncrement: true}, {Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}}}}, Routes:`, 1)
	return source
}

func generatedPatchPresenceRuntimeSource(t *testing.T, target HandlerTarget) string {
	t.Helper()
	source := generatedWriteRuntimeSource(WritePatch)
	if target == HandlerGo {
		source = generatedGoWriteRuntimeSource(WritePatch)
	}
	replace := func(old, replacement string) {
		t.Helper()
		if count := strings.Count(source, old); count != 1 {
			t.Fatalf("generated PATCH presence fixture expected one occurrence of %q, got %d", old, count)
		}
		source = strings.Replace(source, old, replacement, 1)
	}
	replace(
		"CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)",
		"CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL, NOTE TEXT)")
	replace(
		"INSERT INTO EVENTS(ID, NAME) VALUES (1, 'before')",
		"INSERT INTO EVENTS(ID, NAME, NOTE) VALUES (1, 'before', 'keep')")
	replace(
		`{"Data":[{"id":1,"name":"after"},{"name":"inserted"}]}`,
		`{"Data":[{"id":1,"name":"after"},{"name":"inserted","note":"new"}]}`)
	replace(
		"SELECT ID, NAME FROM EVENTS WHERE ID IN",
		"SELECT ID, NAME, NOTE FROM EVENTS WHERE ID IN")
	replace(
		`{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}}},`,
		`{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}}, {Name: "NOTE", Source: "NOTE", Type: spec.TypeRef{Name: "string"}}},`)
	replace(
		`if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NAME, ',') FROM (SELECT NAME FROM EVENTS ORDER BY ID)").Scan(&names); err != nil || names != "after,inserted" {
		t.Fatalf("patched rows = %q err=%v", names, err)
	}`,
		`if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NAME, ',') FROM (SELECT NAME FROM EVENTS ORDER BY ID)").Scan(&names); err != nil || names != "after,inserted" {
		t.Fatalf("patched rows = %q err=%v", names, err)
	}
	var notes string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NOTE, ',') FROM (SELECT NOTE FROM EVENTS ORDER BY ID)").Scan(&notes); err != nil || notes != "keep,new" {
		t.Fatalf("patch notes = %q err=%v", notes, err)
	}`)
	return source
}

func generatedCompoundPatchRuntimeSource(t *testing.T) string {
	t.Helper()
	source := generatedWriteRuntimeSource(WritePatch)
	replace := func(old, replacement string) {
		t.Helper()
		if count := strings.Count(source, old); count != 1 {
			t.Fatalf("compound PATCH fixture expected one occurrence of %q, got %d", old, count)
		}
		source = strings.Replace(source, old, replacement, 1)
	}
	replace(
		"CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)",
		"CREATE TABLE EVENTS (ID INTEGER NOT NULL, TENANT_ID INTEGER NOT NULL, NAME TEXT NOT NULL, PRIMARY KEY (ID, TENANT_ID))")
	replace(
		"INSERT INTO EVENTS(ID, NAME) VALUES (1, 'before')",
		"INSERT INTO EVENTS(ID, TENANT_ID, NAME) VALUES (1, 1, 'other-tenant'), (1, 2, 'before')")
	replace(
		`{"Data":[{"id":1,"name":"after"},{"name":"inserted"}]}`,
		`{"Data":[{"id":1,"tenantId":2,"name":"after"},{"id":1,"tenantId":3,"name":"inserted"}]}`)
	replace(
		"SELECT ID, NAME FROM EVENTS WHERE ID IN",
		"SELECT ID, NAME, TENANT_ID FROM EVENTS WHERE ID IN")
	replace(
		`{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}}},`,
		`{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}}, {Name: "TENANT_ID", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}, PrimaryKey: true}},`)
	replace(
		`if len(result.Data) != 2 || result.Data[0].Id == nil || *result.Data[0].Id != 1 || result.Data[1].Id == nil || *result.Data[1].Id != 2 {
		t.Fatalf("unexpected generated output: %+v", result.Data)
	}
	var names string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NAME, ',') FROM (SELECT NAME FROM EVENTS ORDER BY ID)").Scan(&names); err != nil || names != "after,inserted" {
		t.Fatalf("patched rows = %q err=%v", names, err)
	}`,
		`if len(result.Data) != 2 || result.Data[0].Id != 1 || result.Data[0].TenantId != 2 ||
		result.Data[1].Id != 1 || result.Data[1].TenantId != 3 {
		t.Fatalf("unexpected generated output: %+v", result.Data)
	}
	var names string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NAME, ',') FROM (SELECT NAME FROM EVENTS ORDER BY TENANT_ID)").Scan(&names); err != nil || names != "other-tenant,after,inserted" {
		t.Fatalf("compound patched rows = %q err=%v", names, err)
	}`)
	return source
}

func generatedWriteRuntimeSourceForKey(operation WriteOperation, keyType string, withCurrent ...bool) string {
	setup := ""
	body := `{"Data":[{"name":"one"},{"name":"two"}]}`
	assertion := `
	if len(result.Data) != 2 || result.Data[0].Id == nil || *result.Data[0].Id != 1 || result.Data[1].Id == nil || *result.Data[1].Id != 2 {
		t.Fatalf("unexpected generated output: %+v", result.Data)
	}
	var count int
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM EVENTS WHERE ID IN (1, 2)").Scan(&count); err != nil || count != 2 {
		t.Fatalf("sequenced rows were not flushed: count=%d err=%v", count, err)
	}`
	if operation == WritePut {
		setup = `
	if _, err = db.ExecContext(ctx, "INSERT INTO EVENTS(ID, NAME) VALUES (1, 'before')"); err != nil {
		t.Fatal(err)
	}`
		body = `{"Data":{"id":1,"name":"after"}}`
		assertion = `
	if result.Data == nil || result.Data.Id == nil || *result.Data.Id != 1 {
		t.Fatalf("unexpected generated output: %+v", result.Data)
	}
	var name string
	if err = db.QueryRowContext(ctx, "SELECT NAME FROM EVENTS WHERE ID = 1").Scan(&name); err != nil || name != "after" {
		t.Fatalf("updated row = %q err=%v", name, err)
	}`
	}
	if operation == WritePatch {
		setup = `
	if _, err = db.ExecContext(ctx, "INSERT INTO EVENTS(ID, NAME) VALUES (1, 'before')"); err != nil {
		t.Fatal(err)
	}`
		body = `{"Data":[{"id":1,"name":"after"},{"name":"inserted"}]}`
		assertion = `
	if len(result.Data) != 2 || result.Data[0].Id == nil || *result.Data[0].Id != 1 || result.Data[1].Id == nil || *result.Data[1].Id != 2 {
		t.Fatalf("unexpected generated output: %+v", result.Data)
	}
	var names string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NAME, ',') FROM (SELECT NAME FROM EVENTS ORDER BY ID)").Scan(&names); err != nil || names != "after,inserted" {
		t.Fatalf("patched rows = %q err=%v", names, err)
	}`
	}
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
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestGeneratedWriteHandler(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err = db.ExecContext(ctx, "CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}{{SETUP}}
	component := &spec.Component{Routes: []*spec.Route{{Method: "{{METHOD}}", Path: "/events"}}, Parameters: []*spec.Parameter{
		{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}},
		{{CURRENT_PARAM}}
		{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}},
		{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, EmitOutput: true},
	}, {{CURRENT_VIEW}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(EventsInput{}), OutputType: reflect.TypeOf(EventsOutput{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	routeInput, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "{{METHOD}}", Path: "/events"})
	if !ok {
		t.Fatal("compiled route input contract was not found")
	}
	handler, err := NewEventsHandler()
	if err != nil {
		t.Fatal(err)
	}
	var providers []locator.Provider
	if len(artifact.ViewDependencies) > 0 {
		viewProvider, providerErr := viewprovider.New(viewprovider.Config{Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: &dsql.SQLComponent{DB: db}})
		if providerErr != nil {
			t.Fatal(providerErr)
		}
		providers = append(providers, viewProvider)
	}
	request := httptest.NewRequest("{{METHOD}}", "/events", strings.NewReader(` + "`" + `{{BODY}}` + "`" + `))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := handlerengine.New().Execute(ctx, handlerengine.Request{
		Input: routeInput,
		Scope: scope, Providers: providers, DataSource: sqldml.Source{DB: db}, Handler: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := actual.(*EventsOutput){{ASSERTION}}
}
`
	currentParam := ""
	currentView := ""
	includeCurrent := operation == WritePatch || operation == WritePut
	if len(withCurrent) > 0 {
		includeCurrent = withCurrent[0]
	}
	if includeCurrent {
		currentParam = `{Name: "CurrentEvents", Source: spec.BindSource{Kind: "view", Name: "CurrentEvents"}, Cardinality: string(spec.CardinalityMany)},`
		currentView = `Views: []*spec.View{{
			Key: spec.Key{Kind: spec.KindView, Scope: "example.com/generated/events", Name: "CurrentEvents"}, Name: "CurrentEvents",
			Source: &spec.ViewSource{SQL: "SELECT ID, NAME FROM EVENTS WHERE ID IN (#foreach($event in $Events)$event.Id#if($foreach.HasNext),#end#end)"},
			Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "{{KEY_TYPE}}"}, PrimaryKey: true}, {Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}}},
		}},`
		if operation == WritePut {
			currentView = strings.Replace(currentView, "ID IN (#foreach($event in $Events)$event.Id#if($foreach.HasNext),#end#end)", "ID = $Events.Id", 1)
		}
	}
	return strings.NewReplacer("{{METHOD}}", strings.ToUpper(string(operation)), "{{SETUP}}", setup, "{{BODY}}", body, "{{ASSERTION}}", assertion,
		"{{CURRENT_PARAM}}", currentParam, "{{CURRENT_VIEW}}", currentView, "{{KEY_TYPE}}", keyType).Replace(source)
}

func generatedRecursiveWriteRuntimeSource() string {
	return `package orders

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
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestGeneratedRecursiveWriteHandler(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "orders.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE TABLE ORDERS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)",
		"CREATE TABLE ITEMS (ID INTEGER PRIMARY KEY AUTOINCREMENT, ORDER_ID INTEGER NOT NULL, NAME TEXT NOT NULL)",
		"CREATE TABLE DETAILS (ID INTEGER PRIMARY KEY AUTOINCREMENT, ITEM_ID INTEGER NOT NULL, NOTE TEXT NOT NULL)",
		"INSERT INTO ORDERS(ID, NAME) VALUES (1, 'order-before')",
		"INSERT INTO ITEMS(ID, ORDER_ID, NAME) VALUES (1, 1, 'item-before')",
		"INSERT INTO DETAILS(ID, ITEM_ID, NOTE) VALUES (1, 1, 'detail-before')",
	} {
		if _, err = db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	component := &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/orders"}}, Parameters: []*spec.Parameter{
		{Name: "Orders", Source: spec.BindSource{Kind: "body", Name: "Data"}},
		{Name: "CurrentOrders", Source: spec.BindSource{Kind: "view", Name: "CurrentOrders"}, Cardinality: string(spec.CardinalityMany)},
		{Name: "CurrentItems", Source: spec.BindSource{Kind: "view", Name: "CurrentItems"}, Cardinality: string(spec.CardinalityMany)},
		{Name: "CurrentDetails", Source: spec.BindSource{Kind: "view", Name: "CurrentDetails"}, Cardinality: string(spec.CardinalityMany)},
		{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, EmitOutput: true},
	}, Views: []*spec.View{
		{Name: "CurrentOrders", TypeName: "CurrentOrdersView", Source: &spec.ViewSource{SQL: "SELECT ID, NAME FROM ORDERS WHERE ID = 1"}, Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}, PrimaryKey: true},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		}},
		{Name: "CurrentItems", TypeName: "CurrentItemsView", Source: &spec.ViewSource{SQL: "SELECT ID, ORDER_ID, NAME FROM ITEMS WHERE ID = 1"}, Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}, PrimaryKey: true},
			{Name: "ORDER_ID", Source: "ORDER_ID", Type: spec.TypeRef{Name: "int64"}},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		}},
		{Name: "CurrentDetails", TypeName: "CurrentDetailsView", Source: &spec.ViewSource{SQL: "SELECT ID, ITEM_ID, NOTE FROM DETAILS WHERE ID = 1"}, Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}, PrimaryKey: true},
			{Name: "ITEM_ID", Source: "ITEM_ID", Type: spec.TypeRef{Name: "int64"}},
			{Name: "NOTE", Source: "NOTE", Type: spec.TypeRef{Name: "string"}},
		}},
	}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(OrdersInput{}), OutputType: reflect.TypeOf(OrdersOutput{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	routeInput, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/orders"})
	if !ok {
		t.Fatal("compiled route input contract was not found")
	}
	handler, err := NewOrdersHandler()
	if err != nil {
		t.Fatal(err)
	}
	viewProvider, err := viewprovider.New(viewprovider.Config{
		Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: &dsql.SQLComponent{DB: db},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("PATCH", "/orders", strings.NewReader(` + "`" + `{"Data":[{"id":1,"name":"order-after","items":[{"id":1,"name":"item-after","details":[{"id":1,"note":"detail-after"},{"note":"detail-new"}]},{"name":"item-new","details":[{"note":"new-item-detail"}]}]},{"name":"order-new","items":[{"name":"new-order-item","details":[{"note":"new-order-detail"}]}]}]}` + "`" + `))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := handlerengine.New().Execute(ctx, handlerengine.Request{
		Input: routeInput,
		Scope: scope, Providers: []locator.Provider{viewProvider}, DataSource: sqldml.Source{DB: db}, Handler: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := actual.(*OrdersOutput)
	if len(result.Data) != 2 || result.Data[0].Id == nil || *result.Data[0].Id != 1 || result.Data[1].Id == nil || *result.Data[1].Id != 2 {
		t.Fatalf("unexpected recursive output: %+v", result.Data)
	}
	if len(result.Data[0].Items) != 2 || len(result.Data[1].Items) != 1 {
		t.Fatalf("unexpected recursive children: %+v", result.Data)
	}
	if result.Data[0].Items[0].Id == nil || *result.Data[0].Items[0].Id != 1 || result.Data[0].Items[0].OrderId != 1 {
		t.Fatalf("existing child was not updated/linked: %+v", result.Data[0].Items[0])
	}
	if result.Data[0].Items[1].Id == nil || *result.Data[0].Items[1].Id != 2 || result.Data[0].Items[1].OrderId != 1 {
		t.Fatalf("new child was not sequenced/linked: %+v", result.Data[0].Items[1])
	}
	if result.Data[1].Items[0].Id == nil || *result.Data[1].Items[0].Id != 3 || result.Data[1].Items[0].OrderId != 2 {
		t.Fatalf("new parent child was not sequenced/linked: %+v", result.Data[1].Items[0])
	}
	if len(result.Data[0].Items[0].Details) != 2 || len(result.Data[0].Items[1].Details) != 1 || len(result.Data[1].Items[0].Details) != 1 {
		t.Fatalf("unexpected third-level output: %+v", result.Data)
	}
	if result.Data[0].Items[0].Details[0].Id == nil || *result.Data[0].Items[0].Details[0].Id != 1 || result.Data[0].Items[0].Details[0].ItemId != 1 {
		t.Fatalf("existing detail was not updated/linked: %+v", result.Data[0].Items[0].Details[0])
	}
	if result.Data[0].Items[0].Details[1].Id == nil || *result.Data[0].Items[0].Details[1].Id != 2 || result.Data[0].Items[0].Details[1].ItemId != 1 {
		t.Fatalf("new detail was not sequenced/linked: %+v", result.Data[0].Items[0].Details[1])
	}
	if result.Data[0].Items[1].Details[0].Id == nil || *result.Data[0].Items[1].Details[0].Id != 3 || result.Data[0].Items[1].Details[0].ItemId != 2 {
		t.Fatalf("new item detail was not sequenced/linked: %+v", result.Data[0].Items[1].Details[0])
	}
	if result.Data[1].Items[0].Details[0].Id == nil || *result.Data[1].Items[0].Details[0].Id != 4 || result.Data[1].Items[0].Details[0].ItemId != 3 {
		t.Fatalf("new parent detail was not sequenced/linked: %+v", result.Data[1].Items[0].Details[0])
	}
	var orders, items, details int
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ORDERS").Scan(&orders); err != nil || orders != 2 {
		t.Fatalf("orders count=%d err=%v", orders, err)
	}
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ITEMS").Scan(&items); err != nil || items != 3 {
		t.Fatalf("items count=%d err=%v", items, err)
	}
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM DETAILS").Scan(&details); err != nil || details != 4 {
		t.Fatalf("details count=%d err=%v", details, err)
	}
	var names string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NAME, ',') FROM (SELECT NAME FROM ITEMS ORDER BY ID)").Scan(&names); err != nil || names != "item-after,item-new,new-order-item" {
		t.Fatalf("item names=%q err=%v", names, err)
	}
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NOTE, ',') FROM (SELECT NOTE FROM DETAILS ORDER BY ID)").Scan(&names); err != nil || names != "detail-after,detail-new,new-item-detail,new-order-detail" {
		t.Fatalf("detail notes=%q err=%v", names, err)
	}
}
`
}
