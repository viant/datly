package transcribe

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

type resourceWorkspace struct {
	root      string
	discovery Discovery
}

func (w *resourceWorkspace) init(t *testing.T) {
	t.Helper()
	w.root = t.TempDir()
	writeSourceFile(t, w.root, "app/go.mod", "module corp.example/app\ngo 1.25\nreplace corp.example/private => ../private\n")
	writeSourceFile(t, w.root, "private/go.mod", "module corp.example/private\ngo 1.25\n")
	writeSourceFile(t, w.root, "private/model/model.go", "package model\ntype Row struct {ID int `sqlx:\"id\"`}\n")
	writeSourceFile(t, w.root, "private/model/.datly-gen.json", `{"resources":{"namespace":"private_sql","files":["datly_sql/query.sql"]}}`)
	writeSourceFile(t, w.root, "private/model/datly_sql/query.sql", "SELECT id FROM records WHERE id=1")
	writeSourceFile(t, w.root, "app/api/holder.go", "package api\nimport (model \"corp.example/private/model\"; xdatly \"github.com/viant/xdatly\")\ntype Input struct{}\ntype Output struct {Rows []*model.Row `parameter:\"Rows,kind=output,in=view\" view:\"records,table=records\" sql:\"uri=private_sql:datly_sql/query.sql\"`}\ntype Holder struct {Contract xdatly.Component[Input,Output] `component:\"Records,path=/records,method=GET,view=records\"`}\n")
	writeSourceFile(t, w.root, "app/api/Records.sql", `#setting($_ = $route('/records','GET'))
SELECT * FROM (${embed:private_sql:datly_sql/query.sql}) records`)
	writeSourceFile(t, w.root, "app/api/local.sql", "SELECT 10 AS id")
	writeSourceFile(t, w.root, "app/api/.datly-gen.json", `{"resources":{"namespace":"app_sql","files":["datly_sql/not_a_component.sql"]}}`)
	writeSourceFile(t, w.root, "app/api/datly_sql/not_a_component.sql", `#setting($_ = $route('/not-a-route','GET')) SELECT 999 AS id`)
	w.discovery = Discovery{BaseDir: filepath.Join(w.root, "app"), Include: []string{"corp.example/app/api/..."}, Exclude: []string{"corp.example/private/..."}}
}

func TestDiscoveryResourcesTwoModulesAndReloadSQLite(t *testing.T) {
	ctx := context.Background()
	workspace := &resourceWorkspace{}
	workspace.init(t)
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2)"); err != nil {
		t.Fatal(err)
	}
	type row struct {
		ID int `sqlx:"id" json:"id"`
	}
	type output struct {
		Rows []row `json:"rows"`
	}
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var projects []*ProjectGeneration
	compile := func(ctx context.Context, _ *typecatalog.Catalog) (*application.Build, error) {
		project, err := workspace.discovery.Compile(ctx)
		if err != nil {
			return nil, err
		}
		if len(project.Components) != 1 {
			return nil, fmt.Errorf("manifest asset became a component: %d", len(project.Components))
		}
		compiled := project.Components[0]
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, Types: compiled.Source.Types, Resources: compiled.Source.Resources, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
		if err != nil {
			return nil, err
		}
		reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
		if err != nil {
			return nil, err
		}
		projects = append(projects, project)
		return &application.Build{Types: compiled.Source.Types, Resources: project.Resources, Components: []*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeOf(output{}), Reader: reader}}}, nil
	}
	if err := manager.Reload(ctx, application.Request{Revision: 1, Compile: compile}); err != nil {
		t.Fatal(err)
	}
	pinned, _, err := manager.Pin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, workspace.root, "private/model/datly_sql/query.sql", "SELECT id FROM records WHERE id=2")
	writeSourceFile(t, workspace.root, "app/api/local.sql", "SELECT 20 AS id")
	writeSourceFile(t, workspace.root, "private/model/model.go", "package model\ntype Row struct {ID int `sqlx:\"id\"`}\ntype Added struct {Name string}\n")
	if err := manager.Reload(ctx, application.Request{Revision: 2, Compile: compile}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		ctx  context.Context
		want int
	}{{"pinned old", pinned, 1}, {"current", ctx, 2}} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			manager.ServeHTTP(response, httptest.NewRequest("GET", "/records", nil).WithContext(test.ctx))
			want := fmt.Sprintf(`{"rows":[{"id":%d}]}`, test.want)
			if response.Code != 200 || strings.TrimSpace(response.Body.String()) != want {
				t.Fatalf("response=%d %s want=%s", response.Code, response.Body.String(), want)
			}
		})
		catalog, err := manager.Types(test.ctx)
		if err != nil {
			t.Fatal(err)
		}
		typ, found, err := catalog.Resolve(typecatalog.PackageAuthority, "corp.example/private/model.Added")
		if err != nil || found != (test.want == 2) {
			t.Fatalf("generation %d added type=%v found=%v err=%v", test.want, typ, found, err)
		}
	}
	for index, project := range projects {
		for _, test := range []struct {
			store           *resource.Store
			reference, want string
		}{{project.Resources, "private_sql:datly_sql/query.sql", fmt.Sprintf("SELECT id FROM records WHERE id=%d", index+1)}, {project.Components[0].Source.Resources, "local.sql", fmt.Sprintf("SELECT %d AS id", (index+1)*10)}} {
			data, err := test.store.ReadFile(test.reference)
			if err != nil || string(data) != test.want {
				t.Fatalf("snapshot %s=%q want=%q err=%v", test.reference, data, test.want, err)
			}
		}
	}
	writeSourceFile(t, workspace.root, "private/model/.datly-gen.json", `{"resources":{"namespace":"private_sql","files":["datly_sql/missing.sql"]}}`)
	if err := manager.Reload(ctx, application.Request{Revision: 3, Compile: compile}); err == nil {
		t.Fatal("missing resource reload accepted")
	}
	if manager.Revision() != 2 {
		t.Fatal("failed asset stage replaced active generation")
	}
	activeTypes, err := manager.Types(ctx)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, found, err := activeTypes.Resolve(typecatalog.PackageAuthority, "corp.example/private/model.Added")
	if err != nil || !found {
		t.Fatalf("active types lost: %v %v", found, err)
	}
	fields, err := xshape.New(descriptor, nil).Fields()
	if err != nil || len(fields) != 1 || fields[0].Name != "Name" {
		t.Fatalf("active shape changed: %+v %v", fields, err)
	}
	db.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id FROM records ORDER BY id"}, []row{{1}, {2}})
}

func TestDiscoveryResourceFailuresAreExplicit(t *testing.T) {
	for _, test := range []struct{ name, manifest, want string }{
		{"namespace collision", `{"resources":{"namespace":"app_sql","files":["datly_sql/query.sql"]}}`, "declared by both"},
		{"missing asset", `{"resources":{"namespace":"private_sql","files":["datly_sql/missing.sql"]}}`, "missing.sql"},
		{"malformed manifest", `{"resources":`, "read .datly-gen.json"},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := &resourceWorkspace{}
			w.init(t)
			writeSourceFile(t, w.root, "private/model/.datly-gen.json", test.manifest)
			if _, err := w.discovery.Compile(context.Background()); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want=%s", err, test.want)
			}
		})
	}
}
