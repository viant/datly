package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratorPatchDerivesState(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: genpatch.DQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Result.Plan.MutationHandler == nil || got.Result.Plan.VeltyHandler != nil {
		t.Fatal("Go mutation generation missing")
	}
	pkgDir := filepath.Join(root, strings.TrimPrefix(got.Package.PkgPath, "github.com/viant/datly/genfixture/"))
	hooks := filepath.Join(pkgDir, "lifecycle.go")
	edited := genpatch.ObserveHooks(t, pkgDir)
	if _, err = (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatalf("regenerate: %v", err)
	}
	after, err := os.ReadFile(hooks)
	if err != nil || string(after) != edited {
		t.Fatal("edited hook changed", err)
	}
	genpatch.Run(t, root, pkgDir, genpatch.RuntimeSource)
}

func TestGeneratorReaderWriterRemainSeparate(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	var packages []string
	for _, operation := range []string{"get", "patch", "post", "put"} {
		source := &Source{Name: "Orders" + strings.Title(operation), Scope: "example.com/generated/orders", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: strings.NewReplacer(genpatch.PackageDirective, "#package('api/orders/"+operation+"')", "'PATCH'", "'"+strings.ToUpper(operation)+"'").Replace(genpatch.DQL)}
		got, err := (Generator{Operation: operation}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
		if err == nil {
			compiled, compileErr := NewCompiler().Compile(ctx, source)
			if compileErr != nil {
				t.Fatal(compileErr)
			}
			fromCompiled, compileErr := (Generator{Operation: operation}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
			if compileErr != nil {
				t.Fatal(operation, compileErr)
			}
			if fromCompiled.Package.PkgPath != got.Package.PkgPath {
				t.Fatal("Source and Compiled destinations differ")
			}
		}
		if err != nil {
			t.Fatal(operation, err)
		}
		for _, pkg := range packages {
			if pkg == got.Package.PkgPath {
				t.Fatal("reader/writer package collision")
			}
		}
		packages = append(packages, got.Package.PkgPath)
		plan := got.Result.Plan
		if operation == "get" {
			if plan.MutationHandler != nil || plan.EntitySupport != nil || plan.HookScaffold != nil {
				t.Fatal("reader acquired mutation scaffolding")
			}
			for _, field := range plan.Input.Fields {
				if field.Source == "body" || field.Source == "param" {
					t.Fatalf("reader acquired writer state %+v", field)
				}
			}
		} else if plan.MutationHandler == nil {
			t.Fatal("missing writer")
		}
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("separate generated products: %v\n%s", err, out)
	}
}

func TestGeneratorPreservesDQLAuthority(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, overlay, wantError string }{
		{"authored", `#setting($_ = $input_type('PatchRequest'))
#setting($_ = $output_type('PatchResponse'))
#setting($_ = $dest('entities.go'))
#setting($_ = $support_dest('entities','entity_support.go'))
#setting($_ = $input_dest('request.go'))
#setting($_ = $output_dest('response.go'))
#define($_ = $Payload<[]*OrdersView>(body/changes).Cardinality('Many'))
#define($_ = $RequestID<string>(header/X-Request-ID).Required())
#define($_ = $Result<[]*OrdersView>(output/body))`, ""},
		{"output type conflict", `#define($_ = $Data<string>(output/body))`, "conflicts"},
		{"binding conflict", `#define($_ = $OrdersKeys<string>(header/Keys))`, "conflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			source := &Source{Name: "Orders", Scope: "example.com/generated/orders", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: tc.overlay + "\n" + genpatch.DQL}
			got, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("conflict=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Result.Plan.Input.Type != "PatchRequest" || got.Result.Plan.Output.Type != "PatchResponse" {
				t.Fatal("authored contract names lost")
			}
			if got.Result.Plan.ViewDest != "entities.go" || got.Result.Plan.Input.Destination != "request.go" || got.Result.Plan.Output.Destination != "response.go" {
				t.Fatal("authored artifact destinations lost")
			}
			field, ok := got.Result.Plan.Input.Field("Payload")
			if !ok || !strings.Contains(field.Tag, "in=changes") {
				t.Fatal("authored body binding lost")
			}
			if _, ok := got.Result.Plan.Input.Field("RequestID"); !ok {
				t.Fatal("header lost")
			}
		})
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	_, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Text: genpatch.DQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}})
	if err == nil || !strings.Contains(err.Error(), "conflicts with authored route") {
		t.Fatalf("route conflict=%v", err)
	}
}
