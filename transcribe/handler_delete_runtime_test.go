package transcribe

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	druntime "github.com/viant/datly/runtime"
	rhandler "github.com/viant/datly/runtime/handler"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	veltyhandler "github.com/viant/datly/runtime/handler/velty"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type explicitDeleteEvent struct {
	ID int64 `json:"id" sqlx:"ID,primaryKey=true"`
}

type explicitDeleteInput struct {
	Event  *explicitDeleteEvent `parameter:"Event,kind=body,in=Data"`
	Delete bool                 `parameter:"Delete,kind=body,in=Delete"`
}

type explicitDeleteOutput struct{}

func TestExplicitDeleteRunsThroughUnifiedEngine(t *testing.T) {
	required := true
	compiled, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/generated/events", Name: "Events",
		Text: `#setting($_ = $route('/events', 'DELETE'))
#define($_ = $Event<*>(body/Data).Required())
#define($_ = $Delete<bool>(body/Delete))
#if($Input.Delete)
$dml.Delete("EVENTS", $Input.Event)
#end`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if compiled.VeltyHandler == nil || !strings.Contains(compiled.VeltyHandler.Template, `#if($Input.Delete)`) ||
		!strings.Contains(compiled.VeltyHandler.Template, `$dml.Delete("EVENTS", $Input.Event)`) {
		t.Fatalf("transcribed Velty handler = %#v", compiled.VeltyHandler)
	}
	handler, err := veltyhandler.New[explicitDeleteInput, explicitDeleteOutput](veltyhandler.Config{
		Template: compiled.VeltyHandler.Template,
	})
	if err != nil {
		t.Fatalf("New handler error = %v", err)
	}
	custom := customhandler.New[explicitDeleteInput, explicitDeleteOutput](
		xhandler.ContractFunc[explicitDeleteInput, explicitDeleteOutput](func(ctx context.Context, session xhandler.Session, input *explicitDeleteInput, _ *explicitDeleteOutput) error {
			value, ok, lookupErr := session.Binder().Lookup(ctx, xhandler.DMLKey)
			if lookupErr != nil {
				return lookupErr
			}
			dml, ok := value.(xhandler.DML)
			if !ok {
				return fmt.Errorf("DML capability has type %T", value)
			}
			if input.Delete {
				return dml.Delete("EVENTS", input.Event)
			}
			return nil
		}),
	)
	for _, testCase := range []struct {
		name    string
		handler rhandler.Handler
	}{
		{name: "authored Velty", handler: handler},
		{name: "custom Go", handler: custom},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			for _, marker := range []struct {
				name      string
				value     bool
				remaining string
			}{
				{name: "marker true", value: true, remaining: "2"},
				{name: "marker false", value: false, remaining: "1,2"},
			} {
				t.Run(marker.name, func(t *testing.T) {
					runExplicitDelete(t, testCase.handler, &required, marker.value, marker.remaining)
				})
			}
		})
	}
}

func runExplicitDelete(t *testing.T, handler rhandler.Handler, required *bool, marker bool, wantIDs string) {
	t.Helper()

	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx,
		"CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY, NAME TEXT NOT NULL)",
		"INSERT INTO EVENTS(ID, NAME) VALUES (1, 'delete-me')",
		"INSERT INTO EVENTS(ID, NAME) VALUES (2, 'keep-me')",
	); err != nil {
		t.Fatalf("setup EVENTS: %v", err)
	}
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/generated/events", Name: "Events"},
		Name:   "Events",
		Routes: []*spec.Route{{Method: http.MethodDelete, Path: "/events"}},
		Parameters: []*spec.Parameter{
			{Name: "Event", Source: spec.BindSource{Kind: "body", Name: "Data"}, Required: required},
			{Name: "Delete", Source: spec.BindSource{Kind: "body", Name: "Delete"}},
		},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(explicitDeleteInput{}), OutputType: reflect.TypeOf(explicitDeleteOutput{}),
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(explicitDeleteOutput{}),
		Handler: handler, DataSource: sqldml.Source{DB: harness.DB},
	}})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	body := fmt.Sprintf(`{"Data":{"id":1},"Delete":%t}`, marker)
	request := httptest.NewRequest(http.MethodDelete, "/events", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatalf("request provider error = %v", err)
	}
	if _, err = runtime.ExecuteRoute(ctx, http.MethodDelete, "/events", scope); err != nil {
		t.Fatalf("ExecuteRoute() error = %v", err)
	}
	var ids string
	if err = harness.DB.QueryRowContext(ctx, "SELECT GROUP_CONCAT(ID, ',') FROM (SELECT ID FROM EVENTS ORDER BY ID)").Scan(&ids); err != nil {
		t.Fatalf("query remaining IDs: %v", err)
	}
	if ids != wantIDs {
		t.Fatalf("remaining IDs = %q, want %s", ids, wantIDs)
	}
}
