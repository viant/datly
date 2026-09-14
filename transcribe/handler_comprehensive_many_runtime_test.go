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
	tcolumn "github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	comprehensive "github.com/viant/datly/transcribe/testdata/comprehensive"
	"github.com/viant/datly/typecatalog"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/x"
)

func TestTranscribedComprehensiveManyRunsThroughUnifiedEngine(t *testing.T) {
	tests := []struct {
		name   string
		target HandlerTarget
	}{
		{name: "generated Go", target: HandlerGo},
		{name: "generated Velty", target: HandlerVelty},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			verifyTranscribedComprehensiveMany(t, testCase.target)
		})
	}
}

func verifyTranscribedComprehensiveMany(t *testing.T, target HandlerTarget) {
	t.Helper()
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx,
		`CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)`,
		`CREATE TABLE EXISTING_EVENTS (ID INTEGER PRIMARY KEY, NAME TEXT NOT NULL)`,
		`INSERT INTO EXISTING_EVENTS(ID, NAME) VALUES (10, 'current-one'), (11, 'current-two')`,
	); err != nil {
		t.Fatalf("prepare comprehensive-many schema: %v", err)
	}

	const modelPackage = "github.com/viant/datly/transcribe/testdata/comprehensive"
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(
		reflect.TypeOf(comprehensive.Event{}),
		x.WithName("Event"), x.WithPkgPath(modelPackage),
	)); err != nil {
		t.Fatalf("register linked event: %v", err)
	}

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dql := `#package('example.com/generated/comprehensive')
#import('model','github.com/viant/datly/transcribe/testdata/comprehensive')
#setting($_ = $route('/v1/api/dev/comprehensive/events-many', 'POST'))
#define($_ = $Events<[]*model.Event>(body/Data).Cardinality('Many').Required())
#define($_ = $CurEventsId<?>(param/Events) /*
? SELECT ARRAY_AGG(Id) AS Values FROM ` + "`/`" + ` LIMIT 1
*/)
#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many') /*
SELECT ID, NAME FROM EXISTING_EVENTS
WHERE ID IN (#foreach($id in $CurEventsId.Values)$id#if($foreach.HasNext),#end#end)
*/)
#define($_ = $Status<int>(output/status).Output())
#define($_ = $Data<[]*model.Event>(output/body))
SELECT ID, NAME FROM EVENTS`
	source := &Source{
		Scope: "example.com/generated/comprehensive", Name: "ComprehensiveMany",
		Connector: "main", Text: dql, Types: catalog,
		ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
	}
	generated, err := NewCompiler().Transcribe(ctx, Request{
		Source: source, Destination: root,
		Options: Options{Handler: HandlerOptions{Target: target, Operation: WritePost}},
	})
	if err != nil {
		t.Fatalf("Transcribe() error = %v", err)
	}
	if generated.Result.Plan.Input.Ownership != gen.ContractGenerated ||
		generated.Result.Plan.Output.Ownership != gen.ContractGenerated {
		t.Fatalf("comprehensive contracts = input:%+v output:%+v", generated.Result.Plan.Input, generated.Result.Plan.Output)
	}
	assertComprehensiveManyPlan(t, generated, modelPackage, target)

	testSource := generatedComprehensiveManyRuntimeSource(target)
	packageDir := filepath.Join(root, "generated")
	if err = os.WriteFile(filepath.Join(packageDir, "comprehensive_many_test.go"), []byte(testSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated %s comprehensive-many module failed: %v\n%s", target, runErr, output)
	}
}

func assertComprehensiveManyPlan(t *testing.T, generated *GeneratedPackage, modelPackage string, target HandlerTarget) {
	t.Helper()
	if generated == nil || generated.Result == nil || generated.Result.Plan == nil {
		t.Fatal("comprehensive-many generation plan is required")
	}
	plan := generated.Result.Plan
	var eventsType, helperType, currentType, outputType string
	for _, field := range plan.Input.Fields {
		switch field.Name {
		case "Events":
			eventsType = field.Type
		case "CurEventsId":
			helperType = field.Type
		case "CurrentEvents":
			currentType = field.Type
		}
	}
	for _, field := range plan.Output.Fields {
		if field.Name == "Data" {
			outputType = field.Type
		}
	}
	if eventsType != "[]*model.Event" || currentType != "[]*CurrentEventsView" ||
		outputType != "[]*model.Event" || helperType != "CurEventsIdHelper" {
		t.Fatalf("comprehensive field types: events=%q helper=%q current=%q output=%q", eventsType, helperType, currentType, outputType)
	}
	var imported bool
	for _, item := range plan.Imports {
		if item.Alias == "model" && item.Package == modelPackage {
			imported = true
			break
		}
	}
	if !imported {
		t.Fatalf("comprehensive imports = %+v", plan.Imports)
	}
	if len(plan.Routes) != 1 || plan.Routes[0].Method != "POST" ||
		plan.Routes[0].Path != "/v1/api/dev/comprehensive/events-many" {
		t.Fatalf("comprehensive routes = %+v", plan.Routes)
	}
	switch target {
	case HandlerGo:
		if plan.ContractHandler == nil || plan.VeltyHandler != nil {
			t.Fatalf("generated Go products = contract:%+v velty:%+v", plan.ContractHandler, plan.VeltyHandler)
		}
	case HandlerVelty:
		if plan.ContractHandler != nil || plan.VeltyHandler == nil {
			t.Fatalf("generated Velty products = contract:%+v velty:%+v", plan.ContractHandler, plan.VeltyHandler)
		}
		for _, fragment := range []string{
			`$sequencer.Allocate("EVENTS", $Input.Events, "Id")`,
			`#foreach($RecEvents in $Input.Events)`,
			`$dml.Insert("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));`,
			`#set($Output.Data = $Input.Events)`,
		} {
			if !strings.Contains(plan.VeltyHandler.Template, fragment) {
				t.Fatalf("comprehensive Velty template missing %q:\n%s", fragment, plan.VeltyHandler.Template)
			}
		}
	default:
		t.Fatalf("unsupported comprehensive target %q", target)
	}
}

func generatedComprehensiveManyRuntimeSource(target HandlerTarget) string {
	customImport := ""
	handlerFactory := `handler, err := NewComprehensiveManyHandler()
	if err != nil {
		t.Fatal(err)
	}`
	if target == HandlerGo {
		customImport = `customhandler "github.com/viant/datly/runtime/handler/custom"`
		handlerFactory = `handler := customhandler.New[ComprehensiveManyInput, ComprehensiveManyOutput](NewComprehensiveManyHandler())`
	}
	source := `package comprehensive_many

import (
	"context"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/resource"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	{{CUSTOM_IMPORT}}
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	dtag "github.com/viant/datly/tag"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func (i *ComprehensiveManyInput) Init(context.Context) error {
	if len(i.Events) != 2 || len(i.CurEventsId.Values) != 2 || len(i.CurrentEvents) != 2 {
		return fmt.Errorf("comprehensive input: events=%d helper=%#v current=%d", len(i.Events), i.CurEventsId.Values, len(i.CurrentEvents))
	}
	return nil
}

func TestComprehensiveMany(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "comprehensive.db"))
	if err != nil { t.Fatal(err) }
	defer db.Close()
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)",
		"CREATE TABLE EXISTING_EVENTS (ID INTEGER PRIMARY KEY, NAME TEXT NOT NULL)",
		"INSERT INTO EXISTING_EVENTS(ID, NAME) VALUES (10, 'current-one'), (11, 'current-two')",
	} {
		if _, err = db.ExecContext(ctx, statement); err != nil { t.Fatal(err) }
	}
	holderType := reflect.TypeOf(Component{})
	holderField, ok := holderType.FieldByName("Contract")
	if !ok { t.Fatal("generated component holder is missing Contract") }
	holderTag, present, err := dtag.ParseComponent(holderField.Tag)
	if err != nil || !present { t.Fatalf("parse generated component holder: present=%v err=%v tag=%s", present, err, holderField.Tag) }
	routeSource := &bootstrap.RouteSource{
		HolderType: "Component", FieldName: holderField.Name, PackageName: "comprehensive_many",
		PackagePath: holderType.PkgPath(), Tag: holderTag,
		InputType: "ComprehensiveManyInput", OutputType: "ComprehensiveManyOutput",
	}
	inputType := reflect.TypeOf(ComprehensiveManyInput{})
	outputType := reflect.TypeOf(ComprehensiveManyOutput{})
	if err = routeSource.ValidateContractTypes(inputType, outputType); err != nil { t.Fatal(err) }
	component, err := routeSource.Resolve(inputType, outputType)
	if err != nil { t.Fatal(err) }
	var helperCodec string
	for _, param := range component.Parameters {
		if param != nil && param.Name == "CurEventsId" && param.Codec != nil { helperCodec = param.Codec.Body }
	}
	if helperCodec != "structql" || len(component.Views) != 1 || component.Views[0].Name != "CurrentEvents" {
		t.Fatalf("generated holder metadata: codec=%q views=%+v", helperCodec, component.Views)
	}
	resources:=resource.New();if err:=resources.Register(DatlyResourceNamespace,DatlyResources);err!=nil{t.Fatal(err)}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: inputType, OutputType: outputType, Resources:resources,
	})
	if err != nil { t.Fatal(err) }
	viewProvider, err := viewprovider.New(viewprovider.Config{Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: &dsql.SQLComponent{DB: db}})
	if err != nil { t.Fatal(err) }
	{{HANDLER_FACTORY}}
	registered := &druntime.RegisteredComponent{
		Component: artifact.Component, Input: artifact.Input, OutputType: outputType,
		Handler: handler, DataSource: sqldml.Source{DB: db}, Providers: []locator.Provider{viewProvider},
	}
	runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered},druntime.WithResources(resources))
	if err != nil { t.Fatal(err) }
	request := httptest.NewRequest("POST", "/v1/api/dev/comprehensive/events-many", strings.NewReader("{\"Data\":[{\"id\":10,\"name\":\"one\"},{\"id\":11,\"name\":\"two\"}]}"))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil { t.Fatal(err) }
	actual, err := runtime.ExecuteRoute(ctx, "POST", "/v1/api/dev/comprehensive/events-many", scope)
	if err != nil { t.Fatal(err) }
	result := actual.(*ComprehensiveManyOutput)
	if len(result.Data) != 2 || result.Data[0].Id == nil || *result.Data[0].Id != 10 || result.Data[1].Id == nil || *result.Data[1].Id != 11 {
		t.Fatalf("comprehensive output = %+v", result.Data)
	}
	var names string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NAME, ',') FROM (SELECT NAME FROM EVENTS ORDER BY ID)").Scan(&names); err != nil || names != "one,two" {
		t.Fatalf("persisted names = %q err=%v", names, err)
	}
}
`
	return strings.NewReplacer(
		"{{CUSTOM_IMPORT}}", customImport,
		"{{HANDLER_FACTORY}}", handlerFactory,
	).Replace(source)
}
