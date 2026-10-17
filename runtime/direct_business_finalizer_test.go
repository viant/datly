package runtime_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	legacy "github.com/viant/xdatly/handler/response"
)

type directBusinessInput struct{}
type directBusinessOutput struct {
	legacy.Status
	Data []*legacyStatusRow `json:"data"`
}

func (*directBusinessOutput) Finalize(context.Context) error {
	return &legacy.Error{Code: 403, Message: "record not found or access denied"}
}

type directBusinessLogger struct{ cause error }

func (*directBusinessLogger) Debug(string, ...any) {}
func (*directBusinessLogger) Info(string, ...any)  {}
func (*directBusinessLogger) Warn(string, ...any)  {}
func (l *directBusinessLogger) Error(msg string, args ...any) {
	if msg == "HTTP route execution failed" && len(args) > 0 {
		l.cause, _ = args[0].(error)
	}
}
func TestDirectBusinessFinalizerLegacyErrors(t *testing.T) {
	h := sqlite.New(t)
	if e := h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER)"); e != nil {
		t.Fatal(e)
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "records"}, Settings: &spec.Settings{CaseFormat: "lc"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}, Parameters: []*spec.Parameter{{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
	a, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[directBusinessInput](), OutputType: reflect.TypeFor[directBusinessOutput]()})
	if e != nil {
		t.Fatal(e)
	}
	read, e := reader.NewExecution(reader.Config{Component: a.Component, InputType: reflect.TypeFor[directBusinessInput](), OutputType: reflect.TypeFor[directBusinessOutput](), Plan: a.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
	if e != nil {
		t.Fatal(e)
	}
	rt, e := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[directBusinessOutput](), Reader: read}})
	if e != nil {
		t.Fatal(e)
	}
	defer rt.Shutdown(t.Context())
	log := &directBusinessLogger{}
	w := httptest.NewRecorder()
	gateway.NewHandler(rt, log, "diagnostic").ServeHTTP(w, httptest.NewRequest("GET", "/records", nil))
	var body map[string]interface{}
	if json.Unmarshal(w.Body.Bytes(), &body) != nil {
		t.Fatal("invalidJSON")
	}
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	if _, ok := log.cause.(*legacy.Error); !ok {
		t.Fatalf("unexpected concrete chain root %T", log.cause)
	}
	if !reflect.DeepEqual(body["errors"], []any{"record not found or access denied"}) || body["message"] != "record not found or access denied" {
		t.Fatalf("unexpected public envelope: %s", w.Body.String())
	}

}
