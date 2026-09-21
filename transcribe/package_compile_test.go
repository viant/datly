package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	tcolumn "github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	linkedcontract "github.com/viant/datly/transcribe/testdata/linkedcontract"
	"github.com/viant/datly/typecatalog"
)

type PackageCompileInput struct {
	TenantID int `parameter:"TenantID,kind=path,in=tenantID,required"`
}

type PackageCompileOutput struct {
	Status string `parameter:"Status,kind=output,in=status"`
}

type PackageCompileView struct {
	ID int `sqlx:"id"`
}

type PackageCompileReaderOutput struct {
	Status string                `parameter:"Status,kind=output,in=status"`
	Data   []*PackageCompileView `parameter:"Data,kind=output,in=view" view:"Users,table=users" sql:"SELECT id FROM users"`
}

type PackageCompileMultipleViewOutput struct {
	Primary   []*PackageCompileView `parameter:"Primary,kind=output,in=view" view:"Users,table=users"`
	Secondary []*PackageCompileView `parameter:"Secondary,kind=output,in=view" view:"OtherUsers,table=other_users"`
}

type PackageCompileScalarViewOutput struct {
	Data int `parameter:"Data,kind=output,in=view"`
}

type OtherPackageCompileInput struct{}

type PackageCompileOutputs []*PackageCompileOutput

type EmptyPackageCompileOutput struct{}

type PackageCompileIndependentInput struct {
	Existing []*PackageCompileView `parameter:"Existing,kind=view,in=Existing" view:"Existing,table=users" sql:"SELECT id FROM users"`
}

type PackageCompileExternalIndependentInput struct {
	Existing []*linkedcontract.Event `parameter:"Existing,kind=view,in=Existing" view:"Existing,table=events" sql:"SELECT ID, NAME FROM events"`
}

func TestPackageCompilationJoinsLinkedTypesAndDQLAuthority(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	source := &Source{
		Scope: packagePath,
		Name:  "Users",
		Text: `#setting($_ = $route('/v2/users/{tenantID}', 'GET'))
#define($_ = $TenantID<int>(path/tenantID).Required())
SELECT id FROM users WHERE tenant_id = :TenantID`,
	}
	route := &bootstrap.RouteSource{
		FieldName: "Users", PackagePath: packagePath,
		InputType: "PackageCompileInput", OutputType: "PackageCompileOutput",
		Tag: dtag.Component{Method: "GET", Path: "/v1/users/{tenantID}", Connector: "db", Handler: "HandleUsers"},
	}
	catalog := typecatalog.NewCatalog()
	result, err := (&PackageCompilation{
		Source: source, Component: route,
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
		Types: catalog,
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Component.Routes) != 1 || result.Component.Routes[0].Path != "/v2/users/{tenantID}" || result.Component.Routes[0].Handler != "HandleUsers" {
		t.Fatalf("DQL route did not override package route: %+v", result.Component.Routes)
	}
	if result.Component.Settings == nil || result.Component.Settings.InputType != "PackageCompileInput" ||
		result.Component.Settings.OutputType != "" || result.Component.Settings.DefaultConnector != "db" {
		t.Fatalf("package contract settings = %+v", result.Component.Settings)
	}
	input, err := result.TypeResolver.Descriptor("PackageCompileInput")
	if err != nil || input == nil || input.Type != reflect.TypeOf(PackageCompileInput{}) {
		t.Fatalf("linked input descriptor = %+v, err=%v", input, err)
	}
	if source.PackageComponent != nil || source.Types != nil {
		t.Fatal("package compilation mutated its source")
	}
}

func TestPackageCompilationPreservesMarshallerThroughGeneratedHolder(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: `#setting($_ = $route('/v2/users', 'GET'))
SELECT id FROM users`},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "OtherPackageCompileInput", OutputType: "PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/v1/users", Marshaller: "tabular"},
		},
		InputType: reflect.TypeOf(OtherPackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Transcribe(context.Background(), root)
	if err != nil {
		t.Fatalf("Transcribe() error = %v", err)
	}
	if len(generated.Result.Plan.Routes) != 1 || generated.Result.Plan.Routes[0].Marshaller != "tabular" {
		t.Fatalf("generated routes = %+v", generated.Result.Plan.Routes)
	}
	sources, err := bootstrap.DiscoverComponentsFromPackages(context.Background(), root, []string{"example.com/generated/generated"}, nil)
	if err != nil || len(sources) != 1 {
		t.Fatalf("discover generated holder = %+v, %v", sources, err)
	}
	if sources[0].Tag.Name != "Users" || sources[0].Tag.Marshaller != "tabular" {
		t.Fatalf("generated component tag = %+v", sources[0].Tag)
	}
	component, err := sources[0].Resolve(nil, nil)
	if err != nil {
		t.Fatalf("resolve generated holder: %v", err)
	}
	if len(component.Routes) != 1 || component.Routes[0].Path != "/v2/users" || component.Routes[0].Marshaller != "tabular" {
		t.Fatalf("generated holder component = %+v", component)
	}
}

func TestPackageCompilationTranscribesGeneratedGoHandlerAfterDQLRefinement(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE EVENTS (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		NAME TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create EVENTS: %v", err)
	}
	const packagePath = "github.com/viant/datly/transcribe"
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dql := `#setting($_ = $route('/events', 'PATCH'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())
#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many') /*
SELECT ID, NAME FROM EVENTS
WHERE ID IN (#foreach($event in $Events)$event.Id#if($foreach.HasNext),#end#end)
*/)
#define($_ = $Data<[]*EventsView>(output/body))
SELECT ID, NAME FROM EVENTS`
	generated, err := (&PackageCompilation{
		Source: &Source{
			Scope: packagePath, Name: "Events", Text: dql, Connector: "main",
			ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		},
		Component: &bootstrap.RouteSource{
			FieldName: "Events", PackagePath: packagePath,
			InputType: "OtherPackageCompileInput", OutputType: "PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/package-events", Connector: "main"},
		},
		InputType: reflect.TypeOf(OtherPackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
		Options: Options{
			Contracts: ContractsGenerated,
			Handler:   HandlerOptions{Target: HandlerGo, Operation: WritePatch, Current: "CurrentEvents"},
		},
	}).Transcribe(ctx, root)
	if err != nil {
		t.Fatalf("Transcribe() error = %v", err)
	}
	if generated.Result.Plan.Input.Ownership != gen.ContractGenerated || generated.Result.Plan.Output.Ownership != gen.ContractGenerated ||
		generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.VeltyHandler != nil {
		t.Fatalf("package+DQL generated plan = %+v", generated.Result.Plan)
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "generated_write_test.go"), []byte(generatedGoWriteRuntimeSource(WritePatch)), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("package+DQL generated Go module failed: %v\n%s", runErr, output)
	}
}

func TestPackageCompilationRunsGeneratedHandlersWithLinkedContracts(t *testing.T) {
	tests := []struct {
		name   string
		target HandlerTarget
	}{
		{name: "generated Go", target: HandlerGo},
		{name: "generated Velty", target: HandlerVelty},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			verifyPackageGeneratedHandlerWithLinkedContracts(t, testCase.target)
		})
	}
}

func verifyPackageGeneratedHandlerWithLinkedContracts(t *testing.T, target HandlerTarget) {
	t.Helper()
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE EVENTS (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		NAME TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create EVENTS: %v", err)
	}
	const packagePath = "github.com/viant/datly/transcribe/testdata/linkedcontract"
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	compilation := &PackageCompilation{
		Source: &Source{
			Scope: packagePath, Name: "Events", Connector: "main",
			Text: `#setting($_ = $route('/events', 'POST'))
SELECT ID, NAME FROM EVENTS`,
		},
		Component: &bootstrap.RouteSource{
			FieldName: "Events", PackagePath: packagePath,
			InputType: "Input", OutputType: "Output",
			Tag: dtag.Component{Method: "POST", Path: "/events", Connector: "main", View: "Events"},
		},
		InputType: reflect.TypeOf(linkedcontract.Input{}), OutputType: reflect.TypeOf(linkedcontract.Output{}),
		Options: Options{
			Contracts: ContractsLinked,
			Handler:   HandlerOptions{Target: target, Operation: WritePost},
		},
	}
	compiled, err := compilation.Compile(ctx)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if compiled.Contracts.Input == nil || compiled.Contracts.Output == nil {
		t.Fatalf("package contract authority = %+v", compiled.Contracts)
	}
	generated, err := compilation.Transcribe(ctx, root)
	if err != nil {
		t.Fatalf("Transcribe() error = %v", err)
	}
	if generated.Result.Plan.Input.Ownership != gen.ContractLinked || generated.Result.Plan.Output.Ownership != gen.ContractLinked {
		t.Fatalf("linked contract ownership = input:%+v output:%+v", generated.Result.Plan.Input, generated.Result.Plan.Output)
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "linked_contract_handler_test.go"), []byte(packageLinkedHandlerRuntimeSource(target)), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("package+DQL linked-contract %s module failed: %v\n%s", target, runErr, output)
	}
}

func packageLinkedHandlerRuntimeSource(target HandlerTarget) string {
	customImport := ""
	handlerFactory := `handler, err := NewEventsHandler()
	if err != nil {
		t.Fatal(err)
	}`
	if target == HandlerGo {
		customImport = `customhandler "github.com/viant/datly/runtime/handler/custom"`
		handlerFactory = `handler := customhandler.New[contracts.Input, contracts.Output](NewEventsHandler())`
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
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	contracts "github.com/viant/datly/transcribe/testdata/linkedcontract"
	{{CUSTOM_IMPORT}}
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestLinkedContractHandler(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err = db.ExecContext(ctx, "CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/events"}}, Parameters: []*spec.Parameter{
		{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}, Cardinality: string(spec.CardinalityMany)},
		{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, EmitOutput: true},
	}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component,
		InputType: reflect.TypeOf(contracts.Input{}),
		OutputType: reflect.TypeOf(contracts.Output{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	routeInput, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/events"})
	if !ok {
		t.Fatal("compiled route input contract was not found")
	}
	{{HANDLER_FACTORY}}
	request := httptest.NewRequest("POST", "/events", strings.NewReader(` + "`" + `{"Data":[{"name":"one"},{"name":"two"}]}` + "`" + `))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := handlerengine.New().Execute(ctx, handlerengine.Request{
		Input: routeInput,
		Scope: scope,
		DataSource: sqldml.Source{DB: db},
		Handler: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, ok := actual.(*contracts.Output)
	if !ok || len(result.Data) != 2 || result.Data[0].ID == nil || *result.Data[0].ID != 1 || result.Data[1].ID == nil || *result.Data[1].ID != 2 {
		t.Fatalf("linked output = %#v (%T)", actual, actual)
	}
	var names string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NAME, ',') FROM (SELECT NAME FROM EVENTS ORDER BY ID)").Scan(&names); err != nil || names != "one,two" {
		t.Fatalf("persisted names = %q, err=%v", names, err)
	}
}
`
	return strings.NewReplacer("{{CUSTOM_IMPORT}}", customImport, "{{HANDLER_FACTORY}}", handlerFactory).Replace(source)
}

func TestPackageCompilationDiscoversTemplateSQLWithLinkedInput(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, "CREATE TABLE users (id INTEGER, tenant_id INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	const packagePath = "github.com/viant/datly/transcribe"
	result, err := (&PackageCompilation{
		Source: &Source{
			Scope: packagePath, Name: "Users",
			ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
			Text: `#setting($_ = $route('/users/{tenantID}', 'GET'))
#define($_ = $TenantID<int>(path/tenantID).Required())
SELECT id
#if($TenantID < 0)
, tenant_id
#end
FROM users WHERE tenant_id = $TenantID`,
		},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileInput", OutputType: "PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users/{tenantID}", Connector: "main"},
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Compile(ctx)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(result.Component.RootView.Columns) != 1 || result.Component.RootView.Columns[0].Name != "id" {
		t.Fatalf("columns = %+v", result.Component.RootView.Columns)
	}
}

func TestPackageCompilationSelectsLinkedContractsOnlyWhenCanonicalRoleIsUnchanged(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	route := &bootstrap.RouteSource{
		FieldName: "Users", PackagePath: packagePath,
		InputType: "PackageCompileInput", OutputType: "PackageCompileOutput",
		Tag: dtag.Component{Method: "GET", Path: "/users"},
	}
	unchanged, err := (&PackageCompilation{
		Source:    &Source{Scope: packagePath, Name: "Users", Text: "#setting($_ = $route('/users', 'GET'))\nSELECT id FROM users"},
		Component: route, InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Contracts.Input == nil || unchanged.Contracts.Output != nil {
		t.Fatalf("unchanged contracts = %+v", unchanged.Contracts)
	}

	changed, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Search<string>(query/search).Optional())
SELECT id FROM users`},
		Component: route, InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if changed.Contracts.Input != nil || changed.Contracts.Output != nil {
		t.Fatalf("changed contracts = %+v", changed.Contracts)
	}
	var tenantType string
	for _, param := range changed.Component.Parameters {
		if param != nil && param.Name == "TenantID" {
			tenantType = param.TypeExpr
		}
	}
	if len(changed.Component.Parameters) != 3 || tenantType != "int" {
		t.Fatalf("changed canonical params = %+v", changed.Component.Parameters)
	}
}

func TestPackageCompilationKeepsMatchingReaderOutputLinked(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	result, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: "#setting($_ = $route('/users', 'GET'))\nSELECT id FROM users"},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileInput", OutputType: "PackageCompileReaderOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users", View: "Users"},
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileReaderOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Contracts.Input == nil || result.Contracts.Output == nil {
		t.Fatalf("contracts = %+v", result.Contracts)
	}
	view := result.Views[gen.RootViewPath]
	if view == nil || view.DescriptorKey != packagePath+".PackageCompileView" {
		t.Fatalf("view references = %+v", result.Views)
	}
	descriptor, err := result.TypeResolver.Descriptor(view.DescriptorKey)
	if err != nil || descriptor == nil || descriptor.Type != reflect.TypeOf(PackageCompileView{}) {
		t.Fatalf("root view descriptor = %+v, err=%v", descriptor, err)
	}
	plan, err := gen.New(gen.Input{
		Component: result.Component, TypeResolver: result.TypeResolver,
		TargetPackage: "example.com/generated/users", Contracts: result.Contracts, Views: result.Views,
	}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Views) != 1 || plan.Views[0].Ownership != gen.ViewLinked ||
		plan.Views[0].Type != "transcribe.PackageCompileView" {
		t.Fatalf("linked root view plan = %+v", plan.Views)
	}
}

func TestPackageCompilationLinksIndependentViewWhenInputContractDiverges(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	result, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Search<string>(query/search).Optional())
SELECT id FROM users`},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileIndependentInput", OutputType: "PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf(PackageCompileIndependentInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Contracts.Input != nil {
		t.Fatalf("diverged input contract remained linked: %+v", result.Contracts.Input)
	}
	if len(result.Component.Views) != 1 {
		t.Fatalf("independent views = %+v", result.Component.Views)
	}
	identity, err := result.Component.Views[0].Identity()
	if err != nil {
		t.Fatal(err)
	}
	reference := result.Views[identity]
	if reference == nil || reference.DescriptorKey != packagePath+".PackageCompileView" {
		t.Fatalf("independent view references = %+v", result.Views)
	}
	plan, err := gen.New(gen.Input{
		Component: result.Component, TypeResolver: result.TypeResolver,
		TargetPackage: "example.com/generated/users", Contracts: result.Contracts, Views: result.Views,
	}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	var existingType string
	linkedViews := 0
	for _, field := range plan.Input.Fields {
		if field.Name == "Existing" {
			existingType = field.Type
		}
	}
	for _, view := range plan.Views {
		if view.Ownership == gen.ViewLinked && view.DescriptorKey == packagePath+".PackageCompileView" {
			linkedViews++
		}
	}
	if len(plan.Views) != 2 || linkedViews != 1 ||
		len(plan.Input.Fields) != 2 || existingType != "[]*transcribe.PackageCompileView" {
		t.Fatalf("package independent plan = views:%+v input:%+v", plan.Views, plan.Input.Fields)
	}
}

func TestPackageCompilationLinksExternalIndependentViewInComponentScope(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	result, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Search<string>(query/search).Optional())
SELECT 1`},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileExternalIndependentInput", OutputType: "PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf(PackageCompileExternalIndependentInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Component.Views) != 1 || result.Component.Views[0].Key.Scope != packagePath {
		t.Fatalf("canonical external view = %+v", result.Component.Views)
	}
	identity, err := result.Component.Views[0].Identity()
	if err != nil {
		t.Fatal(err)
	}
	if reference := result.Views[identity]; reference == nil || reference.DescriptorKey != "github.com/viant/datly/transcribe/testdata/linkedcontract.Event" {
		t.Fatalf("external independent view reference = %+v", result.Views)
	}
}

func TestPackageCompilationGeneratesDQLDivergedIndependentView(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Existing<[]*PackageCompileView>(view/Existing) /* SELECT 1 AS id */)
SELECT id FROM users`},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileIndependentInput", OutputType: "PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf(PackageCompileIndependentInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Transcribe(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	var generatedView *gen.ViewPlan
	for index := range generated.Result.Plan.Views {
		candidate := &generated.Result.Plan.Views[index]
		if candidate.Name == "PackageCompileView" {
			generatedView = candidate
			break
		}
	}
	if generatedView == nil || generatedView.Ownership != gen.ViewGenerated {
		t.Fatalf("diverged independent view = %+v", generated.Result.Plan.Views)
	}
	var existingType string
	for _, field := range generated.Result.Plan.Input.Fields {
		if field.Name == "Existing" {
			existingType = field.Type
		}
	}
	if existingType != "[]*PackageCompileView" {
		t.Fatalf("diverged independent input type = %q", existingType)
	}
	inputSource, err := os.ReadFile(filepath.Join(root, "generated", generated.Result.Plan.Input.Destination))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(inputSource), "github.com/viant/datly/transcribe") {
		t.Fatalf("diverged independent input retained package import:\n%s", inputSource)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("DQL-diverged independent-view module did not compile: %v\n%s", runErr, output)
	}
}

func TestPackageCompilationRootViewMetadataDivergenceGeneratesOutput(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	result, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: `#setting($_ = $route('/users', 'GET'))
#setting($_ = $dest('generated_users.go'))
SELECT id FROM users`},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileInput", OutputType: "PackageCompileReaderOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users", View: "Users"},
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileReaderOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Contracts.Input == nil || result.Contracts.Output != nil || result.Component.Settings.OutputType != "" ||
		result.Component.Settings.Generation == nil || result.Component.Settings.Generation.ViewFile != "generated_users.go" || result.Component.RootView == nil || result.Component.RootView.Dest != "" {
		t.Fatalf("compiled ownership = contracts:%+v settings:%+v view:%+v", result.Contracts, result.Component.Settings, result.Component.RootView)
	}
	if len(result.Views) != 0 {
		t.Fatalf("diverged output retained linked view authority: %+v", result.Views)
	}
}

func TestPackageCompilationGeneratesDefaultReaderOutputWhenLinkedOutputHasNoViewSlot(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	result, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: "#setting($_ = $route('/users', 'GET'))\nSELECT id FROM users"},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileInput", OutputType: "EmptyPackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(EmptyPackageCompileOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Contracts.Input == nil || result.Contracts.Output != nil {
		t.Fatalf("contracts = %+v", result.Contracts)
	}
}

func TestPackageCompilationDQLMetadataOverrideRetainsReflectedFieldType(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/generated\n\ngo 1.25\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	generated, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $TenantID(path/tenantID).Optional())
SELECT id FROM users`},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileInput", OutputType: "PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Transcribe(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.Input.Ownership != gen.ContractGenerated || len(generated.Result.Plan.Input.Fields) != 1 ||
		generated.Result.Plan.Input.Fields[0].Type != "int" {
		t.Fatalf("generated input = %+v", generated.Result.Plan.Input)
	}
}

func TestPackageCompilationDivergedImportedContractUsesGeneratedLocalName(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	result, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Search<string>(query/search).Optional())
SELECT id FROM users`},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "*clock.Time", OutputType: "PackageCompileOutput",
			Imports: []spec.ImportSpec{{Alias: "clock", Package: "time"}},
			Tag:     dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf((*time.Time)(nil)), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Contracts.Input != nil || result.Contracts.Output != nil {
		t.Fatalf("contracts = %+v", result.Contracts)
	}
	if result.Component.Settings == nil || result.Component.Settings.InputType != "" {
		t.Fatalf("diverged imported input type was retained: %+v", result.Component.Settings)
	}
	plan, err := gen.New(gen.Input{
		Component: result.Component, TypeResolver: result.TypeResolver,
		TargetPackage: "example.com/generated/users", Contracts: result.Contracts,
	}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.Input.Type != "UsersInput" || plan.Input.Ownership != gen.ContractGenerated {
		t.Fatalf("generated input = %+v", plan.Input)
	}
}

func TestPackageCompilationPreservesExplicitDQLContractTypeWithPackageSpelling(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	result, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: `#setting($_ = $route('/users', 'GET'))
#setting($_ = $input_type('PackageCompileInput'))
#define($_ = $Search<string>(query/search).Optional())
SELECT id FROM users`},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileInput", OutputType: "PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Contracts.Input != nil || result.ContractTypeOverrides.Input != "PackageCompileInput" {
		t.Fatalf("contract decision = linked:%+v authored:%+v", result.Contracts.Input, result.ContractTypeOverrides)
	}
	plan, err := gen.New(gen.Input{Component: result.Component}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.Input.Type != "PackageCompileInput" || plan.Input.Ownership != gen.ContractGenerated {
		t.Fatalf("generated input = %+v", plan.Input)
	}
}

func TestPackageCompilationGenerateOwnsReaderOutputMissingFromPackageContract(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/generated\n\ngo 1.25\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	generated, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: "#setting($_ = $route('/users', 'GET'))\nSELECT id FROM users"},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileInput", OutputType: "PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Transcribe(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.Input.Ownership != gen.ContractLinked || generated.Result.Plan.Output.Ownership != gen.ContractGenerated {
		t.Fatalf("contract plan = %+v", generated.Result.Plan)
	}
	if generated.Package.HasType("PackageCompileInput") || !generated.Package.HasType("UsersOutput") {
		t.Fatalf("contract ownership was not preserved: %+v", generated.Package.Types)
	}
	if _, err = os.Stat(filepath.Join(root, "generated", "input.go")); !os.IsNotExist(err) {
		t.Fatalf("linked input file was emitted: %v", err)
	}
	output, err := os.ReadFile(filepath.Join(root, "generated", "output.go"))
	if err != nil || !strings.Contains(strings.Join(strings.Fields(string(output)), " "), "Data []*UsersView") {
		t.Fatalf("generated reader output = %v\n%s", err, output)
	}
}

func TestPackageCompilationRejectsMismatchedPackage(t *testing.T) {
	_, err := (&PackageCompilation{
		Source:    &Source{Scope: "example.com/one"},
		Component: &bootstrap.RouteSource{PackagePath: "example.com/two"},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Compile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected package mismatch, got %v", err)
	}
}

func TestPackageCompilationRejectsMismatchedLinkedContract(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	_, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath},
		Component: &bootstrap.RouteSource{
			PackagePath: packagePath, InputType: "PackageCompileInput", OutputType: "PackageCompileOutput",
		},
		InputType: reflect.TypeOf(OtherPackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Compile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not match holder contract") {
		t.Fatalf("expected linked contract mismatch, got %v", err)
	}
}

func TestPackageCompilationSupportsContainerContracts(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	result, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: "#setting($_ = $route('/users', 'GET'))\nSELECT id FROM users"},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "*PackageCompileInput", OutputType: "[]*PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf((*PackageCompileInput)(nil)), OutputType: reflect.TypeOf([]*PackageCompileOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Component.Settings == nil || result.Component.Settings.InputType != "*PackageCompileInput" ||
		result.Component.Settings.OutputType != "" {
		t.Fatalf("container contract = %+v", result.Component.Settings)
	}
}

func TestPackageCompilationRegistersNamedContainerContract(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	result, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: "#setting($_ = $route('/users', 'GET'))\nSELECT id FROM users"},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileInput", OutputType: "PackageCompileOutputs",
			Tag: dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutputs{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := result.TypeResolver.Descriptor("PackageCompileOutputs")
	if err != nil || descriptor == nil || descriptor.Type != reflect.TypeOf(PackageCompileOutputs{}) {
		t.Fatalf("named container descriptor = %+v, err=%v", descriptor, err)
	}
}

func TestPackageCompilationDoesNotMutateCatalogOnCompileFailure(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	catalog := typecatalog.NewCatalog()
	_, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Text: "not valid DQL"},
		Component: &bootstrap.RouteSource{
			PackagePath: packagePath, InputType: "PackageCompileInput", OutputType: "PackageCompileOutput",
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}), Types: catalog,
	}).Compile(context.Background())
	if err == nil {
		t.Fatal("expected compile failure")
	}
	if _, ok, resolveErr := catalog.Resolve(typecatalog.PackageAuthority, packagePath+".PackageCompileInput"); resolveErr != nil || ok {
		t.Fatalf("failed compile mutated catalog: ok=%v err=%v", ok, resolveErr)
	}
}

func TestPackageCompilationRejectsMultipleViewBindingsWithoutMutatingCatalog(t *testing.T) {
	const packagePath = "github.com/viant/datly/transcribe"
	catalog := typecatalog.NewCatalog()
	_, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Text: "#setting($_ = $route('/users', 'GET'))\nSELECT id FROM users"},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileInput", OutputType: "PackageCompileMultipleViewOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users"},
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileMultipleViewOutput{}), Types: catalog,
	}).Compile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "more than one output/view field") {
		t.Fatalf("Compile() error = %v", err)
	}
	if _, ok, resolveErr := catalog.Resolve(typecatalog.PackageAuthority, packagePath+".PackageCompileInput"); resolveErr != nil || ok {
		t.Fatalf("failed compile mutated catalog: ok=%v err=%v", ok, resolveErr)
	}
}

func TestLinkedRootViewDescriptorRejectsNonStructBinding(t *testing.T) {
	_, err := newPackageViewResolver(&spec.Component{}, nil).rootDescriptor(reflect.TypeOf(PackageCompileScalarViewOutput{}))
	if err == nil || !strings.Contains(err.Error(), "named package struct") {
		t.Fatalf("linkedRootViewDescriptor() error = %v", err)
	}
}
