package transcribe

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
	xmodule "github.com/viant/x/module"
	xshape "github.com/viant/x/shape"
)

func TestCrossProjectPrivateClosureSQLite(t *testing.T) {
	root := t.TempDir()
	writeSourceFile(t, root, "app/go.mod", "module corp.example/app\ngo 1.25\nreplace corp.example/private => ../private\n")
	writeSourceFile(t, root, "private/go.mod", "module corp.example/private\ngo 1.25\n")
	writeSourceFile(t, root, "private/model/model.go", "package model\ntype Row struct { ID int `sqlx:\"id\"` }\n")
	writeSourceFile(t, root, "private/unused/broken.go", "this is intentionally invalid Go")
	for _, project := range []string{"app", "private"} {
		body := "package api\nimport ( model \"corp.example/private/model\"; xdatly \"github.com/viant/xdatly\" )\ntype Input struct{}\ntype Output struct{ Rows []*model.Row `parameter:\"Rows,kind=output,in=view\" view:\"records,table=records\" sql:\"SELECT id FROM records\"` }\ntype Holder struct{ Contract xdatly.Component[Input,Output] `component:\"Records,path=/records,method=GET,view=records\"` }\n"
		writeSourceFile(t, root, project+"/api/holder.go", body)
		writeSourceFile(t, root, project+"/api/Records.sql", "#setting($_ = $route('/records', 'GET'))\nSELECT id FROM records")
	}
	ctx := context.Background()
	workspace, err := (xmodule.LocalWorkspace{BaseDir: root, ModuleDirs: []string{"app", "private"}}).Resolve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{"app", "private"} {
		t.Run(project, func(t *testing.T) {
			selected := "corp.example/" + project + "/api"
			routes, err := (bootstrap.PackageDiscovery{Workspace: workspace, Include: []string{selected}, Exclude: []string{"corp.example/private/model"}}).Discover(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(routes) != 1 || routes[0].PackagePath != selected {
				t.Fatalf("routes=%+v", routes)
			}
			packages, err := (loaderast.LocalPackageLoader{Workspace: workspace}).Load(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			if len(packages.Roots) != 1 || len(packages.Packages) != 2 || packages.Packages["corp.example/private/model"] == nil {
				t.Fatalf("package closure=%+v", packages)
			}
			catalog := typecatalog.NewCatalog()
			for _, pkg := range packages.Packages {
				if err := catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
					t.Fatal(err)
				}
			}
			descriptor, found, err := catalog.Resolve(typecatalog.PackageAuthority, "corp.example/private/model.Row")
			if err != nil || !found {
				t.Fatalf("private type=%v,%v", found, err)
			}
			fields, err := xshape.New(descriptor, nil).Fields()
			if err != nil {
				t.Fatal(err)
			}
			var runtimeFields []xshape.RuntimeField
			for _, field := range fields {
				runtimeFields = append(runtimeFields, xshape.RuntimeField{Name: field.Name, TypeExpr: field.TypeExpr, Tag: field.Tag})
			}
			rowType, err := (xshape.Runtime{}).Struct(runtimeFields)
			if err != nil {
				t.Fatal(err)
			}
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7)"); err != nil {
				t.Fatal(err)
			}
			want := reflect.MakeSlice(reflect.SliceOf(rowType), 1, 1)
			want.Index(0).FieldByName("ID").SetInt(7)
			db.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id FROM records"}, want.Interface())
			active := typecatalog.NewCatalog()
			discovery := &Discovery{BaseDir: root, ModuleDirs: []string{"app", "private"}, Include: []string{selected}, Exclude: []string{"corp.example/private/model"}, Types: active}
			compiled, err := discovery.Compile(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(compiled.Components) != 1 || compiled.Components[0].Source.Scope != selected {
				t.Fatalf("compiled project=%+v", compiled)
			}
			if _, found, err := compiled.Components[0].Source.Types.Resolve(typecatalog.PackageAuthority, "corp.example/private/model.Row"); err != nil || !found {
				t.Fatalf("staged private type=%v,%v", found, err)
			}
			for _, valid := range []bool{true, false} {
				if !valid {
					writeSourceFile(t, root, project+"/api/Broken.sql", "#setting($_ = $route('', 'GET'))\nSELECT id FROM records")
					if _, err := discovery.Compile(ctx); err == nil {
						t.Fatal("invalid staged component was accepted")
					}
				}
				if _, found, err := active.Resolve(typecatalog.PackageAuthority, "corp.example/private/model.Row"); err != nil || found {
					t.Fatalf("discovery mutated active catalog: found=%v err=%v", found, err)
				}
			}
		})
	}
}
