package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestTranscribedPatchUsesStructQLDerivedIDs(t *testing.T) {
	tests := []struct {
		name   string
		target HandlerTarget
	}{
		{name: "generated Go", target: HandlerGo},
		{name: "generated Velty", target: HandlerVelty},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			verifyTranscribedStructQLPatch(t, testCase.target)
		})
	}
}

func verifyTranscribedStructQLPatch(t *testing.T, target HandlerTarget) {
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
	dql := "#setting($_ = $route('/events', 'PATCH'))\n" +
		"#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())\n" +
		"#define($_ = $CurEventsId<?>(param/Events) /*\n" +
		"? SELECT ARRAY_AGG(Id) AS IDs FROM `/` LIMIT 1\n" +
		"*/)\n" +
		`#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many') /*
SELECT ID, NAME FROM EVENTS
WHERE ID IN (#foreach($id in $CurEventsId.IDs)$id#if($foreach.HasNext),#end#end)
*/)
#define($_ = $Data<[]*EventsView>(output/body))
SELECT ID, NAME FROM EVENTS`
	source := &Source{
		Scope: "example.com/generated/events", Name: "Events", Connector: "main", Text: dql,
		Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
	}
	compiled, err := NewCompiler().Compile(ctx, source)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if compiled.Component.RootView == nil || len(compiled.Component.RootView.Columns) == 0 {
		t.Fatalf("compiled root columns = %+v", compiled.Component.RootView)
	}
	generated, err := NewCompiler().Transcribe(ctx, Request{
		Source:      source,
		Destination: root,
		Options: Options{Handler: HandlerOptions{
			Target: target, Operation: WritePatch, Current: "CurrentEvents",
		}},
	})
	if err != nil {
		t.Fatalf("Transcribe() error = %v", err)
	}
	var helperFound bool
	for _, field := range generated.Result.Plan.Input.Fields {
		if field.Name == "CurEventsId" && field.Type == "CurEventsIdHelper" {
			helperFound = true
			break
		}
	}
	if !helperFound {
		t.Fatalf("generated StructQL helper field = %+v", generated.Result.Plan.Input.Fields)
	}
	var currentTag string
	for _, field := range generated.Result.Plan.Input.Fields {
		if field.Name == "CurrentEvents" {
			currentTag = field.Tag
		}
	}
	if !strings.Contains(currentTag, `sql:"uri=`) {
		t.Fatalf("generated current-view field tag = %q", currentTag)
	}
	currentSQL := false
	for _, file := range generated.Result.Plan.Resources.Files {
		if strings.Contains(file.Content, `$CurEventsId.IDs`) {
			currentSQL = true
		}
	}
	if !currentSQL {
		t.Fatal("generated SQL resource lost StructQL input")
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "structql_patch_test.go"), []byte(generatedStructQLPatchRuntimeSource(target)), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated %s StructQL PATCH module failed: %v\n%s", target, runErr, output)
	}
}

func generatedStructQLPatchRuntimeSource(target HandlerTarget) string {
	customImport := ""
	handlerFactory := `handler, err := NewEventsHandler()
	if err != nil {
		t.Fatal(err)
	}`
	if target == HandlerGo {
		customImport = `customhandler "github.com/viant/datly/runtime/handler/custom"`
		handlerFactory = `handler := customhandler.New[EventsInput, EventsOutput](NewEventsHandler())`
	}
	source := `package events

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
	dtag "github.com/viant/datly/tag"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func (i *EventsInput) Init(context.Context) error {
	if len(i.CurEventsId.IDs) != 1 || len(i.CurrentEvents) != 1 {
		return fmt.Errorf("bound StructQL helper=%#v current=%#v", i.CurEventsId.IDs, i.CurrentEvents)
	}
	return nil
}

func TestStructQLPatch(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)",
		"INSERT INTO EVENTS(ID, NAME) VALUES (1, 'before')",
		"INSERT INTO EVENTS(ID, NAME) VALUES (2, 'untouched')",
	} {
		if _, err = db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	holderType := reflect.TypeOf(Component{})
	holderField, ok := holderType.FieldByName("Contract")
	if !ok { t.Fatal("generated component holder is missing Contract") }
	holderTag, present, err := dtag.ParseComponent(holderField.Tag)
	if err != nil || !present { t.Fatalf("parse generated component holder: present=%v err=%v tag=%s", present, err, holderField.Tag) }
	routeSource := &bootstrap.RouteSource{
		HolderType: "Component", FieldName: holderField.Name, PackageName: "events", PackagePath: holderType.PkgPath(),
		Tag: holderTag, InputType: "EventsInput", OutputType: "EventsOutput",
	}
	inputType := reflect.TypeOf(EventsInput{})
	outputType := reflect.TypeOf(EventsOutput{})
	if err = routeSource.ValidateContractTypes(inputType, outputType); err != nil { t.Fatal(err) }
	component, err := routeSource.Resolve(inputType, outputType)
	if err != nil { t.Fatal(err) }
	var helperCodec string
	for _, param := range component.Parameters {
		if param != nil && param.Name == "CurEventsId" && param.Codec != nil && len(param.Codec.Args) == 1 {
			helperCodec = param.Codec.Args[0]
		}
	}
	resources:=resource.New();if err:=resources.Register(DatlyResourceNamespace,DatlyResources);err!=nil{t.Fatal(err)}
	if strings.HasPrefix(helperCodec,"uri="){data,readErr:=resources.ReadFile(strings.TrimPrefix(helperCodec,"uri="));if readErr!=nil{t.Fatal(readErr)};helperCodec=string(data)}
	viewSQL := ""
	if len(component.Views) == 1 && component.Views[0] != nil && component.Views[0].Source != nil {
		data,err:=resources.ReadFile(component.Views[0].Source.URI);if err!=nil{t.Fatal(err)};viewSQL=string(data)
	}
	if !strings.Contains(helperCodec, "ARRAY_AGG(Id) AS IDs") || len(component.Views) != 1 ||
		!strings.Contains(viewSQL, "$CurEventsId.IDs") {
		t.Fatalf("generated holder metadata: codec=%q views=%d viewSQL=%q", helperCodec, len(component.Views), viewSQL)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: inputType, OutputType: outputType, Resources:resources,
	})
	if err != nil {
		t.Fatal(err)
	}
	viewProvider, err := viewprovider.New(viewprovider.Config{
		Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: &dsql.SQLComponent{DB: db},
	})
	if err != nil {
		t.Fatal(err)
	}
	{{HANDLER_FACTORY}}
	registered := &druntime.RegisteredComponent{
		Component: artifact.Component, Input: artifact.Input, OutputType: outputType,
		Handler: handler, DataSource: sqldml.Source{DB: db},
		Providers: []locator.Provider{viewProvider},
	}
	runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("PATCH", "/events", strings.NewReader("{\"Data\":[{\"id\":1,\"name\":\"after\"}]}"))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := runtime.ExecuteRoute(ctx, "PATCH", "/events", scope)
	if err != nil {
		t.Fatal(err)
	}
	result := actual.(*EventsOutput)
	if len(result.Data) != 1 || result.Data[0].Id == nil || *result.Data[0].Id != 1 {
		t.Fatalf("output = %+v", result.Data)
	}
	var names string
	if err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(NAME, ',') FROM (SELECT NAME FROM EVENTS ORDER BY ID)").Scan(&names); err != nil || names != "after,untouched" {
		t.Fatalf("persisted names = %q, err=%v", names, err)
	}
}
`
	return strings.NewReplacer(
		"{{CUSTOM_IMPORT}}", customImport,
		"{{HANDLER_FACTORY}}", handlerFactory,
	).Replace(source)
}
