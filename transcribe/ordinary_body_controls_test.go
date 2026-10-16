package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

func TestOrdinarySharedBodyNativeRejections(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		edit       func(string) string
	}{
		{"extra business column", "business column coverage", func(s string) string {
			return strings.Replace(s, "r.updated, r.should_delete,", "r.updated, r.extra, r.should_delete,", 1)
		}},
		{"explicit scalar mismatch", "CAST", func(s string) string {
			return strings.Replace(s, ".WithColumnType('id','int')", ".WithColumnType('id','string')", 1)
		}},

		{"joined body owner", "body must remain an independent leaf", func(s string) string {
			s = strings.Replace(s, "type(r,'Record'),", "child.*,type(child,'Child'),type(r,'Record'),", 1)
			return strings.Replace(s, "FROM (SELECT src.*, '' AS should_delete FROM records src) r", "FROM (SELECT src.*, '' AS should_delete FROM records src) r LEFT JOIN child_records child ON child.parent_id=r.id", 1)
		}},
		{"public rename", "field tags conflict", func(s string) string {
			return strings.Replace(s, "type(r,'Record'),", "type(r,'Record'),tag(r.id,'json:\"different\"'),", 1)
		}},
		{"read destination", "destination", func(s string) string {
			return strings.Replace(s, ".TypeName('Record')", ".TypeName('Record').Dest('current.go')", 1)
		}},
		{"unrelated physical table", "table compatibility", func(s string) string {
			return strings.Replace(s, "r.updated FROM records r */", "r.updated FROM other_records r */", 1)
		}},
		{"different connector", "connector compatibility", func(s string) string {
			return strings.Replace(s, ".TypeName('Record')", ".TypeName('Record').Connector('other')", 1)
		}},
		{"unclassified extra transient", "unclassified transient", func(s string) string {
			s = strings.Replace(s, "r.updated, r.should_delete,", "r.updated, r.extra, r.should_delete,", 1)
			return strings.Replace(s, "type(r,'Record'),", "type(r,'Record'),internal_transient(r.extra),", 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/bodycurrentfixture"}).Write(t, root)
			db := sqlite.New(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER,entity_id INTEGER,contact_id INTEGER,created INTEGER,updated INTEGER,extra TEXT)", "CREATE TABLE other_records(id INTEGER PRIMARY KEY,type_id INTEGER,entity_id INTEGER,contact_id INTEGER,created INTEGER,updated INTEGER)", "CREATE TABLE child_records(id INTEGER PRIMARY KEY,parent_id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			text := tc.edit(strings.Replace(ordinaryBodyCurrentDQL, "CURRENT_ROW_TYPE", "Record", 1))
			source := &Source{Name: "records", Scope: "github.com/viant/datly/bodycurrentfixture/source", Connector: "main", Text: text, ColumnRefiner: column.New(column.Connections{"main": db.DB, "other": db.DB})}
			_, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("intended %q guard, actual %v", tc.want, err)
			}
			if _, e := os.Stat(filepath.Join(root, "generated")); !os.IsNotExist(e) {
				t.Fatalf("rejected source emitted output: %v", e)
			}
			if source.Text != text {
				t.Fatal("source changed")
			}
			t.Logf("native intended rejection: %v", err)
		})
	}
}

func TestOrdinarySharedBodyFinalTargetRejections(t *testing.T) {
	for _, target := range []struct {
		operation string
		language  HandlerTarget
	}{{"get", HandlerGo}, {"patch", HandlerVelty}} {
		t.Run(target.operation+"/"+string(target.language), func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/bodycurrentfixture"}).Write(t, root)
			db := sqlite.New(t)
			if err := db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER,entity_id INTEGER,contact_id INTEGER,created INTEGER,updated INTEGER)"); err != nil {
				t.Fatal(err)
			}
			text := strings.Replace(ordinaryBodyCurrentDQL, "CURRENT_ROW_TYPE", "Record", 1)
			if target.operation == "get" {
				text = strings.Replace(text, "'/records','PATCH'", "'/records','GET'", 1)
			}
			source := &Source{Name: "records", Scope: "github.com/viant/datly/bodycurrentfixture/source", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}
			_, err := (Generator{Operation: target.operation, Language: target.language}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
			if err == nil || !strings.Contains(err.Error(), "mutation markers require") {
				t.Fatalf("non-native lifecycle accepted: %v", err)
			}
			if _, e := os.Stat(filepath.Join(root, "generated")); !os.IsNotExist(e) {
				t.Fatal("unsupported final target emitted")
			}
			t.Logf("final target rejection %v", err)
		})
	}
}

// Load a real package declaration into the ordinary catalog, then let native
// output/view linking select it. No constructed ViewReference grants ownership.
func TestOrdinarySharedBodyPreservesLinkedRoot(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/bodycurrentfixture"}).Write(t, root)
	raw := `package shared
import "reflect"
type Record struct {
 Id int ` + "`sqlx:\"id,primaryKey=true\"`" + `
 TypeId int ` + "`sqlx:\"type_id\"`" + `
 EntityId int ` + "`sqlx:\"entity_id\"`" + `
 ContactId int ` + "`sqlx:\"contact_id\"`" + `
 Created int ` + "`sqlx:\"created\"`" + `
 Updated int ` + "`sqlx:\"updated\"`" + `
 ShouldDelete bool ` + "`sqlx:\"-\" diff:\"-\" internal:\"true\" json:\"-\" writer:\"delete\"`" + `
}
var _anchorRecord=reflect.TypeFor[Record]()
`
	writeSourceFile(t, root, "shared/record.go", raw)
	pkg, err := loaderast.LoadPackageFS(t.Context(), os.DirFS(root), "shared")
	if err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatal(err)
	}
	db := sqlite.New(t)
	if err = db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER,entity_id INTEGER,contact_id INTEGER,created INTEGER,updated INTEGER)"); err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(ordinaryBodyCurrentDQL, "CURRENT_ROW_TYPE", "CurrentRow", 1)
	text = strings.Replace(text, "#setting($_ = $connector", "#import('shared','github.com/viant/datly/bodycurrentfixture/shared')\n#setting($_ = $connector", 1)
	text = strings.Replace(text, "$Data<[]*Record>(output/body)", "$Data<[]*shared.Record>(output/view)", 1)
	text = strings.Replace(text, "type(r,'Record')", "type(r,'shared.Record')", 1)
	compiled, err := NewCompiler().Compile(t.Context(), &Source{Name: "records", Scope: "github.com/viant/datly/bodycurrentfixture/source", Text: text, Connector: "main", Types: catalog, ColumnRefiner: column.New(column.Connections{"main": db.DB})})
	if err != nil {
		t.Fatal(err)
	}
	if compiled.Views[gen.RootViewPath] == nil || compiled.Views[gen.RootViewPath].DescriptorKey == "" {
		t.Fatal("genuine output row did not retain linked root authority")
	}
	input, _, err := generationInput(root, "", compiled)
	if err != nil {
		t.Fatal(err)
	}
	if err = input.ValidateLifecycleTarget(true); err != nil {
		t.Fatal(err)
	}
	plan, err := gen.New(input).Plan()
	if err != nil {
		t.Fatal(err)
	}
	linked := false
	for _, v := range plan.Views {
		if v.Ownership == gen.ViewSharedBody {
			t.Fatal("linked row converted into generated body owner")
		}
		linked = linked || v.Ownership == gen.ViewLinked
	}
	if !linked {
		t.Fatal("no linked row in native plan")
	}
	after, err := os.ReadFile(filepath.Join(root, "shared/record.go"))
	if err != nil || string(after) != raw {
		t.Fatal("authored row changed")
	}
	t.Log("genuine AST catalog -> native output/view linked root; no ordinary generated sharing, owner bytes preserved")
}

func TestOrdinarySharedBodyInferredReadKeepsOwnRow(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/bodycurrentfixture"}).Write(t, root)
	db := sqlite.New(t)
	if err := db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER,entity_id INTEGER,contact_id INTEGER,created INTEGER,updated INTEGER)"); err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(ordinaryBodyCurrentDQL, ".TypeName('CURRENT_ROW_TYPE')", "", 1)
	source := &Source{Name: "records", Scope: "github.com/viant/datly/bodycurrentfixture/source", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	result, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	generated := 0
	for _, v := range result.Result.Plan.Views {
		if v.Ownership == gen.ViewSharedBody {
			t.Fatal("inferred independent row was admitted as body-owned")
		}
		if v.Ownership == gen.ViewGenerated {
			generated++
		}
	}
	if generated != 2 {
		t.Fatalf("distinct inferred rows=%d", generated)
	}
}
