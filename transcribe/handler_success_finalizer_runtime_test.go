package transcribe

import "strings"

func generatedSuccessFinalizerRuntimeSource(target HandlerTarget) string {
	customImport := ""
	handlerFactory := `handler, err := NewEventsHandler()
	if err != nil {
		t.Fatal(err)
	}
	return handler`
	if target == HandlerGo {
		customImport = `customhandler "github.com/viant/datly/runtime/handler/custom"`
		handlerFactory = `return customhandler.New[EventsInput, EventsOutput](NewEventsHandler())`
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
	rhandler "github.com/viant/datly/runtime/handler"
	{{CUSTOM_IMPORT}}
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

var (
	initializerCalls int
	finalizerCalls   int
	lifecycleOrder   []string
)

func (i *EventsInput) Init(context.Context) error {
	initializerCalls++
	lifecycleOrder = append(lifecycleOrder, "init")
	return nil
}

func (o *EventsOutput) Finalize(context.Context) error {
	finalizerCalls++
	lifecycleOrder = append(lifecycleOrder, "finalize")
	return nil
}

func resetLifecycle() {
	initializerCalls = 0
	finalizerCalls = 0
	lifecycleOrder = nil
}

func openLifecycleDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err = db.Exec("CREATE TABLE EVENTS (ID TEXT PRIMARY KEY, NAME TEXT NOT NULL CHECK (NAME <> ''))"); err != nil {
		t.Fatal(err)
	}
	return db
}

func generatedArtifact(t *testing.T) *bootstrap.Artifact {
	t.Helper()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: generatedComponent(),
		InputType: reflect.TypeOf(EventsInput{}), OutputType: reflect.TypeOf(EventsOutput{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func generatedComponent() *spec.Component {
	return &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/generated/events", Name: "Events"},
		Name: "Events", Routes: []*spec.Route{{Method: "POST", Path: "/events"}},
		Parameters: []*spec.Parameter{
			{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}, Cardinality: string(spec.CardinalityMany)},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, EmitOutput: true},
		},
	}
}

func newGeneratedHandler(t *testing.T) rhandler.Handler {
	t.Helper()
	{{HANDLER_FACTORY}}
}

func executeGenerated(t *testing.T, source sqldml.Source, body string) error {
	t.Helper()
	request := httptest.NewRequest("POST", "/events", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	component := generatedComponent()
	artifact := generatedArtifact(t)
	runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(EventsOutput{}),
		DataSource: source, Handler: newGeneratedHandler(t),
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.ExecuteRoute(context.Background(), "POST", "/events", scope)
	return err
}

func TestGeneratedSuccessFinalizerOrdering(t *testing.T) {
	t.Run("runs after engine owned commit", func(t *testing.T) {
		resetLifecycle()
		db := openLifecycleDB(t)
		source := sqldml.Source{DB: db, OnCommit: func(context.Context) {
			lifecycleOrder = append(lifecycleOrder, "commit")
		}}
		if err := executeGenerated(t, source, ` + "`" + `{"Data":[{"id":"one","name":"first"}]}` + "`" + `); err != nil {
			t.Fatal(err)
		}
		if initializerCalls != 1 || finalizerCalls != 1 {
			t.Fatalf("lifecycle init=%d finalize=%d", initializerCalls, finalizerCalls)
		}
		if got := strings.Join(lifecycleOrder, ","); got != "init,commit,finalize" {
			t.Fatalf("lifecycle order = %q", got)
		}
	})

	t.Run("is skipped after flush failure", func(t *testing.T) {
		resetLifecycle()
		db := openLifecycleDB(t)
		source := sqldml.Source{DB: db, OnCommit: func(context.Context) {
			lifecycleOrder = append(lifecycleOrder, "commit")
		}}
		if err := executeGenerated(t, source, ` + "`" + `{"Data":[{"id":"bad","name":""}]}` + "`" + `); err == nil {
			t.Fatal("expected flush failure")
		}
		if initializerCalls != 1 || finalizerCalls != 0 {
			t.Fatalf("lifecycle init=%d finalize=%d", initializerCalls, finalizerCalls)
		}
		if got := strings.Join(lifecycleOrder, ","); got != "init" {
			t.Fatalf("lifecycle order = %q", got)
		}
	})
}

var (
	_ xhandler.Initializer = (*EventsInput)(nil)
	_ xhandler.Finalizer   = (*EventsOutput)(nil)
)
`
	return strings.NewReplacer(
		"{{CUSTOM_IMPORT}}", customImport,
		"{{HANDLER_FACTORY}}", handlerFactory,
	).Replace(source)
}
