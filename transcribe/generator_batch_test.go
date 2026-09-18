package transcribe

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratorBatchLookup(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genbatch"}).Write(t, root)
	source := &Source{Name: "Orders", Scope: "batch", Text: genpatch.DQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	generated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.VeltyHandler != nil || generated.Result.Plan.Settings.Mutation != "patch" {
		t.Fatal("GEN did not emit universal writer metadata")
	}
	directory := filepath.Join(root, strings.TrimPrefix(generated.Package.PkgPath, "github.com/viant/datly/genbatch/"))
	genpatch.Run(t, root, directory, genpatch.BatchRuntimeSource, "-v")
}

func TestGeneratorCompositeLookup(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.CompositeSchema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genbatch"}).Write(t, root)
	source := &Source{Name: "Orders", Scope: "batch", Text: genpatch.CompositeDQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	generated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, strings.TrimPrefix(generated.Package.PkgPath, "github.com/viant/datly/genbatch/"))
	genpatch.Run(t, root, directory, genpatch.CompositeRuntimeSource)
}
