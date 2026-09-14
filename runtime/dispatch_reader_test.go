package runtime

import (
	"context"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/assertly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
)

type readEngineInput struct {
	ID          int
	Initialized bool
}

func (i *readEngineInput) Init(context.Context) error {
	i.Initialized = true
	return nil
}

func TestService_ExecuteRouteWithRequest_BindsInputExecutesSQLAndShapesOutput(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (7, 'john')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/users", Name: "Users"},
		Name: "Users",
		Routes: []*spec.Route{
			{Method: "GET", Path: "/v1/api/users/{id}"},
		},
		RootView: &spec.View{
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users WHERE id = :ID AND :Initialized",
			},
		},
		Parameters: []*spec.Parameter{
			{
				Name:   "ID",
				Source: spec.BindSource{Kind: "path", Name: "id"},
			},
			{Name: "Initialized"},
			{
				Name:   "Data",
				Source: spec.BindSource{Kind: "output", Name: "view"},
			},
		},
	}

	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
	}

	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component:       component,
		InputType:       reflect.TypeOf(readEngineInput{}),
		OutputType:      reflect.TypeOf(output{}),
		DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{
		Component:  component,
		InputType:  reflect.TypeOf(readEngineInput{}),
		OutputType: reflect.TypeOf(output{}),
		Plan:       artifact.Reader,
		SQL:        &rsql.SQLComponent{DB: h.DB},
	})
	if err != nil {
		t.Fatalf("reader registration failed: %v", err)
	}

	bundle, err := route.NewBundle([]*spec.Component{component})
	if err != nil {
		t.Fatalf("bundle build failed: %v", err)
	}

	engine := newTestService(t, bundle, map[string]*registry.RegisteredComponent{
		component.Key.String(): {
			Component:  component,
			Input:      artifact.Input,
			OutputType: reflect.TypeOf(output{}),
			Reader:     reader,
		},
	})

	actual, err := executeTestRoute(t, engine, context.Background(), testharness.NewRequest("GET", "/v1/api/users/7").WithQuery(url.Values{}))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	assertly.AssertValues(t, &output{
		Data: []*row{{ID: 7, Name: "john"}},
	}, actual)
}

func TestRuntimeConstantDrivesReaderSQLTemplate(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE vendors (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO vendors(id, name) VALUES (1, 'acme')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct {
		Vendor string
	}
	type row struct {
		ID   int
		Name string
	}
	type output struct {
		Data []*row
	}
	value := "vendors"
	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Vendors"},
		Name:     "Vendors",
		Routes:   []*spec.Route{{Method: "GET", Path: "/vendors"}},
		Settings: &spec.Settings{Const: map[string]string{"Vendor": value}},
		RootView: &spec.View{Name: "Vendors", Source: &spec.ViewSource{SQL: "SELECT id, name FROM $Unsafe.Vendor"}},
		Parameters: []*spec.Parameter{
			{Name: "Vendor", Source: spec.BindSource{Kind: "const", Name: "Vendor"}, TypeExpr: "string", Value: &value},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{
		Component: artifact.Component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}),
		Plan: artifact.Reader, SQL: &rsql.SQLComponent{DB: h.DB},
	})
	if err != nil {
		t.Fatalf("NewExecution() error = %v", err)
	}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Reader: reader,
	}})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	actual, err := executeTestRoute(t, runtime, context.Background(), testharness.NewRequest("GET", "/vendors"))
	if err != nil {
		t.Fatalf("ExecuteRoute() error = %v", err)
	}
	result := actual.(*output)
	if len(result.Data) != 1 || result.Data[0].ID != 1 || result.Data[0].Name != "acme" {
		t.Fatalf("output = %+v", result)
	}
}
