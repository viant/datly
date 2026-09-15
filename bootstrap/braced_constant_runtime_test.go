package bootstrap_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness/sqlite"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
)

func TestUnquotedBracedConstantDQLDiscoveryAndRead(t *testing.T) {
	type input struct {
		Project string `parameter:"project,kind=const,in=project"`
	}
	const sql = "SELECT id,name FROM ${project}.records WHERE id=1"
	const dql = "#setting($_ = $route('/records','GET'))\n#setting($_ = $const('project','unused_default'))\n" + sql
	for _, extension := range []string{"yaml", "json"} {
		t.Run(extension, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'resolved')"); err != nil {
				t.Fatal(err)
			}
			text := "project: main\n"
			if extension == "json" {
				text = `{"project":"main"}`
			}
			file := filepath.Join(t.TempDir(), "values."+extension)
			if err := os.WriteFile(file, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			values, err := (constant.Loader{}).Load(ctx, file)
			if err != nil {
				t.Fatal(err)
			}
			source := &transcribe.Source{Name: "Records", Scope: "example.com/probe", Text: dql, Const: values, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": h.DB})}
			compiled, err := transcribe.NewCompiler().Compile(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if source.Text != dql || !strings.Contains(compiled.Component.RootView.Source.SQL, "${project}.records") {
				t.Fatalf("authored SQL changed: %q", compiled.Component.RootView.Source.SQL)
			}
			before, _ := json.Marshal(compiled.Component)
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, Const: values, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[probeOutput](), DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			sqlComponent := &dsql.SQLComponent{DB: h.DB}
			if err = sqlComponent.RegisterConnector("main", h.DB); err != nil {
				t.Fatal(err)
			}
			execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: sqlComponent})
			if err != nil {
				t.Fatal(err)
			}
			result, err := execution.Read(ctx, &input{Project: "request-value-must-not-override"}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			rows := result.(*probeOutput).Rows
			if len(rows) != 1 || rows[0].Name != "resolved" {
				t.Fatalf("rows=%+v", rows)
			}
			after, _ := json.Marshal(compiled.Component)
			if string(before) != string(after) {
				t.Fatal("runtime rewrote compiled source")
			}
		})
	}
}
