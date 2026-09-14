package runtime

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	veltyhandler "github.com/viant/datly/runtime/handler/velty"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
)

type veltyRouteInput struct {
	Name        string
	Initialized bool
}

func (i *veltyRouteInput) Init(context.Context) error {
	i.Initialized = true
	return nil
}

type veltyRouteOutput struct {
	Name      string
	Finalized bool
}

func (o *veltyRouteOutput) Finalize(context.Context) error {
	o.Finalized = true
	return nil
}

func TestServiceExecutesRegisteredVeltyHandlerThroughUnifiedEngine(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/velty", Name: "Events"},
		Name:   "Events",
		Routes: []*spec.Route{{Method: http.MethodPost, Path: "/v1/api/events"}},
		Parameters: []*spec.Parameter{{Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component:  component,
		InputType:  reflect.TypeOf(veltyRouteInput{}),
		OutputType: reflect.TypeOf(veltyRouteOutput{}),
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	handler, err := veltyhandler.New[veltyRouteInput, veltyRouteOutput](veltyhandler.Config{Template: `
#if($Input.Initialized)
$dml.Execute("INSERT INTO events(name) VALUES (?)", $Name)
#set($Output.Name = $Name)
#end`})
	if err != nil {
		t.Fatalf("velty handler build failed: %v", err)
	}
	bundle, err := route.NewBundle([]*spec.Component{component})
	if err != nil {
		t.Fatalf("bundle build failed: %v", err)
	}
	service := newTestService(t, bundle, map[string]*registry.RegisteredComponent{
		component.Key.String(): {
			Component:  component,
			Input:      artifact.Input,
			OutputType: reflect.TypeOf(veltyRouteOutput{}),
			DataSource: sqldml.Source{DB: h.DB},
			Handler:    handler,
		},
	})

	actual, err := executeTestRoute(t, service, ctx, testharness.NewRequest(http.MethodPost, "/v1/api/events").WithQuery(url.Values{"name": []string{"Ada"}}))
	if err != nil {
		t.Fatalf("velty route execution failed: %v", err)
	}
	output, ok := actual.(*veltyRouteOutput)
	if !ok || output.Name != "Ada" || !output.Finalized {
		t.Fatalf("unexpected velty output: %#v", actual)
	}
	var name string
	if err := h.DB.QueryRowContext(ctx, `SELECT name FROM events`).Scan(&name); err != nil || name != "Ada" {
		t.Fatalf("expected engine-flushed velty insert, name=%q err=%v", name, err)
	}
}
