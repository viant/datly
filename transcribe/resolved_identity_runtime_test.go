package transcribe

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratorResolvedIdentityIndexes(t *testing.T) {
	for _, deep := range []bool{false, true} {
		t.Run(fmt.Sprint("named_deep=", deep), func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			schema := append(append([]string(nil), genpatch.Schema...), genpatch.ResolvedIdentitySchema...)
			text := genpatch.LifecycleDQL
			if deep {
				schema = append(schema, genpatch.DeepIdentitySchema...)
				text = genpatch.NamedIdentityDQL
			}
			if err := db.ExecStatements(ctx, schema...); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
			request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
			got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(root, strings.TrimPrefix(got.Package.PkgPath, "github.com/viant/datly/genfixture/"))
			hooks := genpatch.ObserveResolvedHooks(t, directory)
			if _, err = (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(filepath.Join(directory, "lifecycle.go"))
			if err != nil || string(after) != hooks {
				t.Fatal("regeneration changed application lifecycle", err)
			}
			// Shared entity-sync acceptance owns race instrumentation. These fresh
			// generated modules validate identity semantics without rebuilding the
			// entire dependency graph under -race for every shape variant.
			genpatch.Run(t, root, directory, genpatch.ResolvedIdentityRuntime(deep), "-v")
		})
	}
}

func TestGeneratorResolvedCompositeZeroIdentity(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	schema := append(append([]string(nil), genpatch.CompositeSchema...), genpatch.ResolvedCompositeSchema...)
	if err := db.ExecStatements(ctx, schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: genpatch.ResolvedCompositeDQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, strings.TrimPrefix(got.Package.PkgPath, "github.com/viant/datly/genfixture/"))
	before, err := os.ReadFile(filepath.Join(directory, "lifecycle.go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(directory, "lifecycle.go"))
	if err != nil || string(after) != string(before) {
		t.Fatal("empty lifecycle placeholders changed", err)
	}
	genpatch.Run(t, root, directory, genpatch.ResolvedCompositeRuntime(), "-v")
}

func TestGeneratorNamedResolvedSplitPackages(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	schema := append(append(append([]string(nil), genpatch.Schema...), genpatch.ResolvedIdentitySchema...), genpatch.DeepIdentitySchema...)
	if err := db.ExecStatements(ctx, schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: genpatch.NamedSplitIdentityDQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, strings.TrimPrefix(got.Package.PkgPath, "github.com/viant/datly/genfixture/"))
	hooks := genpatch.ObserveResolvedHooks(t, directory)
	source := genpatch.SplitResolvedRuntime(t, root)
	if _, err = (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(directory, "lifecycle.go"))
	if err != nil || string(after) != hooks {
		t.Fatal("split lifecycle changed on regeneration", err)
	}
	genpatch.Run(t, root, directory, source, "-v")
}

func TestGeneratedIdentityPreparationCost(t *testing.T) {
	for _, wide := range []bool{false, true} {
		t.Run(fmt.Sprint("wide_high_cardinality=", wide), func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			schema := append(append(append([]string(nil), genpatch.Schema...), genpatch.ResolvedIdentitySchema...), genpatch.DeepIdentitySchema...)
			if err := db.ExecStatements(ctx, schema...); err != nil {
				t.Fatal(err)
			}
			if wide {
				if err := db.ExecStatements(ctx, genpatch.PreparationWideSchema()...); err != nil {
					t.Fatal(err)
				}
			}
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
			request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: genpatch.NamedIdentityDQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
			got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(root, strings.TrimPrefix(got.Package.PkgPath, "github.com/viant/datly/genfixture/"))
			// One bounded sample is sufficient to assert the generated preparation
			// benchmark remains executable; repeated statistical benchmarking belongs
			// in the dedicated performance job, not the default package suite.
			genpatch.Run(t, root, directory, genpatch.PreparationCostRuntime(wide), "-run", "^$", "-bench", "BenchmarkGeneratedPreparation", "-benchmem", "-benchtime=3x", "-count=1")

		})
	}
}

func TestGeneratorReadIndexFilenameControls(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, settings, want string }{
		{"default", "", "indexes.go"},
		{"prefix", `#setting($_ = $file_prefix('orders_'))`, "orders_indexes.go"},
		{"override", `#setting($_ = $file_prefix('orders_'))
#setting($_ = $support_dest('indexes','lookup.go'))`, "lookup.go"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
			request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: tc.settings + "\n" + genpatch.DQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
			got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if got.Result.Plan.ReadIndexes == nil || got.Result.Plan.ReadIndexes.Source.Destination != tc.want {
				t.Fatalf("index destination: %+v", got.Result.Plan.ReadIndexes)
			}
			path := filepath.Join(root, "api/orders", tc.want)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(before) {
				t.Fatal("index regeneration unstable", err)
			}
		})
	}
}
