package bootstrap_test

import (
	"context"
	"encoding/json"
	"github.com/viant/bindly/resource"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/testutil/sqlfault"
)

type probeInput struct{ ID int }
type probeRow struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}
type probeOutput struct{ Rows []probeRow }

// The placeholder-named decoy makes missing expansion an observable wrong-data
// read instead of allowing a missing-table error to conceal the DB boundary.
func TestSQLiteDiscoveryAndExecutionInstanceFiles(t *testing.T) {
	const authored = "SELECT id, name FROM `$project.ds.records` WHERE id=:ID"
	shared := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Name: "Records"},
		Routes:   []*spec.Route{{Method: "GET", Path: "/records"}},
		Settings: &spec.Settings{Const: map[string]string{"project": "authored"}},
		RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: authored}},
	}
	before, _ := json.Marshal(shared)
	for _, environment := range []string{"e2e", "prod"} {
		t.Run(environment, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			h := sqlite.New(t)
			err := h.ExecStatements(ctx,
				"CREATE TABLE `"+environment+".ds.records` (id INTEGER,name TEXT)",
				"INSERT INTO `"+environment+".ds.records` VALUES (1,'"+environment+"')",
				"CREATE TABLE `$project.ds.records` (id INTEGER,name TEXT)",
				"INSERT INTO `$project.ds.records` VALUES (1,'unexpanded-decoy')")
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var calls []sqlfault.Call
			db := h.FaultDB(t, func(_ context.Context, c sqlfault.Call) error {
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, c)
				return nil
			})
			component := shared.Clone()
			extension, body := ".yaml", "project: "+environment+"\n"
			if environment == "prod" {
				extension, body = ".json", `{"project":"prod"}`
			}
			file := filepath.Join(t.TempDir(), "constants"+extension)
			if err := os.WriteFile(file, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			instance, err := (constant.Loader{}).Load(ctx, file)
			if err != nil {
				t.Fatal(err)
			}
			component.Settings.DefaultConnector = "main"
			resolver := sqlx.ParameterResolver(func(name string) (any, bool, error) { return 1, name == "ID", nil })
			err = column.New(column.Connections{"main": db}).Refine(ctx, component, nil, &column.TemplateInput{Const: instance, Value: reflect.ValueOf(probeInput{ID: 1}), ParameterResolver: resolver})
			if err != nil {
				t.Fatal(err)
			}
			if component.RootView.Source.SQL != authored {
				t.Fatal("discovery rewrote source")
			}
			// The real bootstrap/reader path, with a fresh compilation standing for reload.
			for revision := 1; revision <= 2; revision++ {
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Const: instance, Component: component, InputType: reflect.TypeFor[probeInput](), OutputType: reflect.TypeFor[probeOutput](), DirectViewField: "Rows"})
				if err != nil {
					t.Fatal(err)
				}
				sqlComponent := &dsql.SQLComponent{DB: db}
				if err = sqlComponent.RegisterConnector("main", db); err != nil {
					t.Fatal(err)
				}
				execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: sqlComponent})
				if err != nil {
					t.Fatal(err)
				}
				result, err := execution.Read(ctx, &probeInput{ID: 1}, nil, resolver)
				if err != nil {
					t.Fatal(err)
				}
				rows := result.(*probeOutput).Rows
				if len(rows) != 1 || rows[0].Name != environment {
					t.Fatalf("instance data mismatch: %+v", rows)
				}
				t.Logf("instance=%s revision=%d got=%q required=%q", environment, revision, rows[0].Name, environment)
			}
			mu.Lock()
			defer mu.Unlock()
			seenDiscovery, seenRead := false, false
			expectedDiscovery := "SELECT id, name FROM `" + environment + ".ds.records` WHERE 1 = 0 AND id = ?"
			expectedRead := "SELECT COALESCE(id, 0) AS id, COALESCE(name, '') AS name FROM `" + environment + ".ds.records` WHERE id=?"
			for _, call := range calls {
				if call.Phase != "query" {
					continue
				}
				if strings.Contains(call.SQL, "$project") {
					t.Fatalf("driver received an unresolved identifier: %q", call.SQL)
				}
				if call.SQL == expectedDiscovery {
					seenDiscovery = true
				}
				if call.SQL == expectedRead {
					seenRead = true
				}
				t.Logf("DB RECEIVED phase=%s SQL=%q", call.Phase, call.SQL)
			}
			if !seenDiscovery || !seenRead {
				t.Fatalf("missing exact DB SQL: discovery=%v read=%v", seenDiscovery, seenRead)
			}
			after, _ := json.Marshal(shared)
			if string(before) != string(after) {
				t.Fatal("shared source/map mutated")
			}
		})
	}
}

func TestInstanceResourcePathsKeepNamedAuthority(t *testing.T) {
	ctx := context.Background()
	store := resource.New()
	const SQL = "SELECT id, name FROM `$project.ds.records` WHERE id=:ID"
	files := fstest.MapFS{"e2e/read.sql": &fstest.MapFile{Data: []byte(SQL)}, "prod/read.sql": &fstest.MapFile{Data: []byte(SQL)}}
	if err := store.Register("app", files); err != nil {
		t.Fatal(err)
	}
	authored := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, Settings: &spec.Settings{Const: map[string]string{"project": "default", "Stage": "default"}, DefaultConnector: "main"}, RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{URI: "app:${Stage}/read.sql"}}}
	snapshot, _ := json.Marshal(authored)
	for _, stage := range []string{"e2e", "prod"} {
		t.Run(stage, func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE `"+stage+".ds.records` (id INTEGER, name TEXT)", "INSERT INTO `"+stage+".ds.records` VALUES(1,'"+stage+"')"); err != nil {
				t.Fatal(err)
			}
			values, _ := constant.New(map[string]string{"Stage": stage, "project": stage})
			component := authored.Clone()
			resolver := sqlx.ParameterResolver(func(name string) (any, bool, error) { return 1, name == "ID", nil })
			if err := column.New(column.Connections{"main": h.DB}).Refine(ctx, component, store, &column.TemplateInput{Const: values, Value: reflect.ValueOf(probeInput{ID: 1}), ParameterResolver: resolver}); err != nil {
				t.Fatal(err)
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, Resources: store, Const: values, InputType: reflect.TypeFor[probeInput](), OutputType: reflect.TypeFor[probeOutput](), DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := execution.Read(ctx, &probeInput{ID: 1}, nil, resolver)
			if err != nil {
				t.Fatal(err)
			}
			if rows := result.(*probeOutput).Rows; len(rows) != 1 || rows[0].Name != stage {
				t.Fatalf("wrong instance resource: %+v", rows)
			}
			if component.RootView.Source.SQL != "" || component.RootView.Source.URI != "app:${Stage}/read.sql" {
				t.Fatal("compiled resource ownership changed")
			}
		})
	}
	after, _ := json.Marshal(authored)
	if string(after) != string(snapshot) {
		t.Fatal("authored component changed")
	}
	for _, file := range files {
		if string(file.Data) != SQL {
			t.Fatal("resource contents changed")
		}
	}
	values, _ := constant.New(map[string]string{"Stage": "../outside"})
	if _, err := fs.ReadFile(values.Resources(store), "app:${Stage}/read.sql"); err == nil {
		t.Fatal("path escaped named filesystem")
	}
	if _, err := fs.ReadFile(store, "e2e/read.sql"); err == nil {
		t.Fatal("named store acquired an implicit default")
	}
}

func TestInstanceNativeCacheSeparatesConnectorTargets(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	values, _ := constant.New(map[string]string{"Cache": root})
	shared := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, Settings: &spec.Settings{DefaultConnector: "main", Cache: &spec.CacheSettings{Enabled: true, TTL: "1m", Location: "${Cache}"}}, RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT id, name FROM records WHERE id=:ID"}}}
	var scopes []string
	for _, stage := range []string{"e2e", "prod"} {
		t.Run(stage, func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'"+stage+"')"); err != nil {
				t.Fatal(err)
			}
			connections, err := connector.Open(ctx, []connector.Config{{Name: "main", Driver: "sqlite3", DSN: filepath.Join(h.TempDir, "test.db")}}, "main")
			if err != nil {
				t.Fatal(err)
			}
			defer connections.Close()
			scopes = append(scopes, connections.CacheIdentity())
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Const: values, Component: shared, InputType: reflect.TypeFor[probeInput](), OutputType: reflect.TypeFor[probeOutput](), DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: connections.SQL, CacheIdentity: connections.CacheIdentity()})
			if err != nil {
				t.Fatal(err)
			}
			resolver := sqlx.ParameterResolver(func(name string) (any, bool, error) { return 1, name == "ID", nil })
			for pass := 0; pass < 2; pass++ {
				actual, err := reader.Read(ctx, &probeInput{ID: 1}, nil, resolver)
				if err != nil {
					t.Fatal(err)
				}
				rows := actual.(*probeOutput).Rows
				if len(rows) != 1 || rows[0].Name != stage {
					t.Fatalf("cache crossed instances: %+v", rows)
				}
				if pass == 0 {
					if err := h.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
	if scopes[0] == scopes[1] {
		t.Fatal("different resolved connectors share cache scope")
	}
	if shared.Settings.Cache.Location != "${Cache}" {
		t.Fatal("cache access rewrote authored config")
	}
}
