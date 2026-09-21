package mcp

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
)

type readerInput struct {
	ID int `json:"id"`
}

type readerRow struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type readerOutput struct {
	Data []*readerRow `json:"data"`
}

func TestToolExecutesReaderHandlerThroughUnifiedRuntime(t *testing.T) {
	required := true
	harness := testharness.NewSQLiteHarness(t)
	if err := harness.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO users(id, name) VALUES (7, 'Ada')`,
	); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Reader"}, Name: "Reader",
		Routes:   []*spec.Route{{Method: "GET", Path: "/users", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "reader.run"}}}},
		RootView: &spec.View{Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE id = :ID"}},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}, Required: &required},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(readerInput{}), OutputType: reflect.TypeOf(readerOutput{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{
		Component: component, InputType: reflect.TypeOf(readerInput{}), OutputType: reflect.TypeOf(readerOutput{}),
		Plan: artifact.Reader, SQL: &rsql.SQLComponent{DB: harness.DB},
	})
	if err != nil {
		t.Fatal(err)
	}
	registered := &registry.RegisteredComponent{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(readerOutput{}), Reader: reader,
	}
	entry, present := runtimeToolService(t, registered).Registry().ToolRegistry.Get("reader.run")
	if !present || entry.Metadata.OutputSchema == nil || entry.Metadata.OutputSchema.Type != "object" || entry.Metadata.OutputSchema.Properties["data"] == nil {
		t.Fatalf("reader output schema = %+v", entry.Metadata.OutputSchema)
	}
	result := executeRuntimeTool(t, registered, "reader.run", map[string]interface{}{"id": float64(7)})
	data, ok := testharness.StructuredObject(t, result.StructuredContent)["data"].([]interface{})
	if !ok || len(data) != 1 || data[0].(map[string]interface{})["name"] != "Ada" {
		t.Fatalf("reader result = %+v", result.StructuredContent)
	}
}
