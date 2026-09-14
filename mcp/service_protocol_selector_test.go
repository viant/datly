package mcp

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	mcpserver "github.com/viant/datly/mcp/server"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
)

type protocolSelectorInput struct {
	Fields []string
	Limit  int
}

type protocolSelectorRow struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type protocolSelectorOutput struct {
	Data []*protocolSelectorRow `json:"data"`
}

func TestProtocolResourceAppliesAuthoredReaderSelectorBindings(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	if err := harness.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO users(id, name) VALUES (1, 'Ada'), (2, 'Grace')`,
	); err != nil {
		t.Fatal(err)
	}
	component := protocolSelectorComponent()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(protocolSelectorInput{}),
		OutputType: reflect.TypeOf(protocolSelectorOutput{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{
		Component: component, InputType: reflect.TypeOf(protocolSelectorInput{}),
		OutputType: reflect.TypeOf(protocolSelectorOutput{}), Plan: artifact.Reader,
		SQL: &rsql.SQLComponent{DB: harness.DB},
	})
	if err != nil {
		t.Fatal(err)
	}
	registered := &registry.RegisteredComponent{
		Component: component, Input: artifact.Input,
		OutputType: reflect.TypeOf(protocolSelectorOutput{}), Reader: reader,
	}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Components: []*registry.RegisteredComponent{registered}, Invoker: runtime})
	if err != nil {
		t.Fatal(err)
	}
	factory, err := mcpserver.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := factory(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	content := readProtocolResource(t, handler, "datly://localhost/selector-users?fields=name&limit=1")
	actual := &protocolSelectorOutput{}
	if err := json.Unmarshal([]byte(content.Text), actual); err != nil {
		t.Fatal(err)
	}
	if len(actual.Data) != 1 || actual.Data[0].ID != 0 || actual.Data[0].Name != "Ada" {
		t.Fatalf("selector output = %+v", actual)
	}
}

func protocolSelectorComponent() *spec.Component {
	return &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "SelectorUsers"}, Name: "SelectorUsers",
		Routes: []*spec.Route{{Method: "GET", Path: "/selector-users", MCP: []*spec.MCPExposure{{
			Kind: spec.MCPExposureResourceTemplate, Name: "selector-users", MIMEType: "application/json",
		}}}},
		RootView: &spec.View{Name: "users", Source: &spec.ViewSource{SQL: "SELECT id, name FROM users ORDER BY id"}, Selector: &spec.Selector{
			AllowFields: true, AllowLimit: true,
		}},
		Parameters: []*spec.Parameter{
			{Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "users", Property: spec.SelectorPropertyFields}},
			{Name: "Limit", Source: spec.BindSource{Kind: "query", Name: "limit"}, QuerySelector: &spec.QuerySelectorBinding{View: "users", Property: spec.SelectorPropertyLimit}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}
}
