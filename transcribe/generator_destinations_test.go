package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

func TestGeneratorDestinationAuthority(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	for directory, code := range map[string]string{"hooks/root": genpatch.RootHooks, "hooks/child": genpatch.ChildHooks} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, directory, "hooks.go"), []byte(code), 0644); err != nil {
			t.Fatal(err)
		}
	}
	catalog := typecatalog.NewCatalog()
	for _, directory := range []string{"hooks/root", "hooks/child"} {
		pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(root), directory)
		if err != nil {
			t.Fatal(err)
		}
		if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
			t.Fatal(err)
		}
	}
	text := "#setting($_ = $file_prefix('orders_'))\n#setting($_ = $support_dest('entity_methods','methods.go'))\n#setting($_ = $support_dest('types','contracts.go'))\n" + genpatch.DestinationDQL("github.com/viant/datly/genfixture")
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "source", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: text, Types: catalog}}
	got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Package.PkgPath != "github.com/viant/datly/genfixture/api/orders" {
		t.Fatal(got.Package.PkgPath)
	}
	for _, directory := range []string{"requests", "responses", "entities", "items", "api/orders"} {
		if _, err := os.Stat(filepath.Join(root, directory)); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []string{"api/orders/contracts.go", "entities/methods.go", "items/methods.go"} {
		if _, err := os.Stat(filepath.Join(root, file)); err != nil {
			t.Fatal("exact support override", err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "generated")); !os.IsNotExist(err) {
		t.Fatal("forced generated directory", err)
	}
	for directory, code := range map[string]string{"hooks/root": genpatch.RootHooks, "hooks/child": genpatch.ChildHooks} {
		edited := strings.Replace(code, "const Version=1", "const Version=2", 1) + "\n// authored edit\n"
		if err := os.WriteFile(filepath.Join(root, directory, "hooks.go"), []byte(edited), 0644); err != nil {
			t.Fatal(err)
		}
	}
	catalog = typecatalog.NewCatalog()
	for _, directory := range []string{"hooks/root", "hooks/child"} {
		pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(root), directory)
		if err != nil {
			t.Fatal(err)
		}
		if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
			t.Fatal(err)
		}
	}
	request.Source.Types = catalog
	if _, err = (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatalf("regenerate destination hooks: %v", err)
	}
	for _, directory := range []string{"hooks/root", "hooks/child"} {
		data, err := os.ReadFile(filepath.Join(root, directory, "hooks.go"))
		if err != nil || !strings.Contains(string(data), "const Version=2") {
			t.Fatal("authored hook edit lost", err)
		}
	}
	runtime := genpatch.RuntimeSource
	start := strings.Index(runtime, "var lookupRead bool")
	end := strings.Index(runtime, "func TestGeneratedPatchRuntime")
	runtime = runtime[:start] + runtime[end:]
	runtime = strings.Replace(runtime, " \"fmt\"", "", 1)
	runtime = strings.Replace(runtime, " \"context\"", " \"context\";rows \"github.com/viant/datly/genfixture/entities\";rh \"github.com/viant/datly/genfixture/hooks/root\";ch \"github.com/viant/datly/genfixture/hooks/child\";\"github.com/viant/datly/spec\"", 1)
	runtime = strings.NewReplacer("OrdersViewHas", "rows.OrderHas", "OrdersView", "rows.Order", "hookObservedOriginal", "rh.HookObservedOriginal", "lookupRead", "rh.LookupRead").Replace(runtime)
	runtime = strings.Replace(runtime, " if !rh.LookupRead", " if rh.Version!=2||ch.Version!=2||rh.Calls==0||ch.Calls==0||rh.Completions==0{t.Fatal(\"authored hooks were not linked\")}\n if !rh.LookupRead", 1)
	runtime = strings.Replace(runtime, "artifact,err:=", `component.TypeContext=&spec.TypeContext{Imports:[]spec.ImportSpec{{Alias:"rh",Package:"github.com/viant/datly/genfixture/hooks/root"},{Alias:"ch",Package:"github.com/viant/datly/genfixture/hooks/child"}}};component.RootView.EntityHooks="rh.Hooks"
 artifact,err:=`, 1)
	genpatch.Run(t, root, filepath.Join(root, "api/orders"), runtime)

}
