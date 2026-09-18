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
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: genpatch.LifecycleDQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Result.Plan.MutationHandler != nil || got.Result.Plan.VeltyHandler != nil || got.Result.Plan.Settings.Mutation != "patch" {
		t.Fatal("universal writer metadata missing")
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

func TestGeneratorPatchAcceptsAuthoredSQLXIdentity(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := &Source{Name: "Records", Scope: "example.com/source", Text: `#package('api/records/writer')
#setting($_ = $input_type('PatchInput'))
#setting($_ = $output_type('PatchOutput'))
#setting($_ = $route('/records','PATCH'))
#define($_ = $Data<[]*Record>(output/body))
SELECT r.ID, r.OWNER_ID, type(r,'Record'),
       CAST(r.ID AS int), CAST(r.OWNER_ID AS int),
       tag(r.ID,'sqlx:"id,primaryKey"'),
       tag(r.OWNER_ID,'sqlx:"owner_id,refTable=owners,refColumn=id"')
FROM records r`}
	generated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.Settings.Mutation != "patch" {
		t.Fatal("authored SQLX primary key did not select universal writer metadata")
	}
}

func TestGeneratorEphemeralOwnershipLeavesNoPackageManifest(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: genpatch.DQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	generator := Generator{Operation: "patch", EphemeralOwnership: true}
	for attempt := 0; attempt < 2; attempt++ {
		generated, err := generator.Generate(ctx, request)
		if err != nil {
			t.Fatalf("generation %d: %v", attempt+1, err)
		}
		directory := filepath.Join(root, strings.TrimPrefix(generated.Package.PkgPath, "github.com/viant/datly/genfixture/"))
		if _, err = os.Stat(filepath.Join(directory, ".datly-gen.json")); !os.IsNotExist(err) {
			t.Fatalf("generation %d retained package manifest: %v", attempt+1, err)
		}
	}
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
		} else if plan.MutationHandler != nil || plan.Settings.Mutation != operation || plan.HookScaffold != nil {
			t.Fatalf("writer did not select universal mutation metadata: %+v", plan)
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

func TestGeneratorExplicitLifecycleDeclarations(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	const module = "github.com/viant/datly/lifecyclefixture"
	for _, tc := range []struct{ name, header, control, wantError string }{
		{"local", "", "lifecycle_type(o,'OrderRules')", ""},
		{"local alias", "#import('hooks','" + module + "/api/orders')\n", "lifecycle_type(o,'hooks.OrderRules')", ""},
		{"foreign package", "#import('hooks','example.com/shop/hooks')\n", "lifecycle_type(o,'hooks.OrderLifecycle')", "cannot scaffold package"},
		{"unknown alias", "", "lifecycle_type(o,'missing.OrderRules')", "cannot scaffold package"},
		{"wrapped", "", "lifecycle_type(o,'*OrderRules')", "concrete unwrapped"},
		{"generic", "", "lifecycle_type(o,'OrderRules[int]')", "concrete unwrapped"},
		{"auxiliary", "", "lifecycle_type(Kinds,'KindRules')", "auxiliary view"},
		{"conflicting role type", "", "lifecycle_type(o,'SharedRules'),lifecycle_type(Items,'SharedRules')", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: module}).Write(t, root)
			text := tc.header + `#setting($_ = $input_type('OrdersInput'))
#setting($_ = $output_type('OrdersOutput'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<[]*Order>(output/body))
` + strings.Replace(genpatch.DQL, "SELECT o.*, Items.*, Kinds.*,", "SELECT o.*, Items.*, Kinds.*, type(o,'Order'), "+tc.control+",", 1)
			request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/source", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
			generated, err := (Generator{Operation: "patch"}).Generate(ctx, request)
			if tc.name == "conflicting role type" {
				if err == nil {
					t.Fatal("shared lifecycle scaffold accepted incompatible parent contracts")
				}
				return
			}
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("error=%v, want %s", err, tc.wantError)
				}
				if _, statErr := os.Stat(filepath.Join(root, "api/orders")); !os.IsNotExist(statErr) {
					t.Fatalf("invalid lifecycle persisted: %v", statErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			scaffold := generated.Result.Plan.HookScaffold
			if scaffold == nil || len(scaffold.EntityHooks) != 1 || scaffold.EntityHooks[0].Type != module+"/api/orders.OrderRules" {
				t.Fatalf("explicit binding=%+v", scaffold)
			}
			before, err := os.ReadFile(filepath.Join(root, "api/orders/lifecycle.go"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(filepath.Join(root, "api/orders/lifecycle.go"))
			if err != nil || string(before) != string(after) {
				t.Fatal("regeneration changed explicit scaffold", err)
			}
		})
	}
}
