package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/transcribe/handler/compiler"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
)

type statementLinkedRow struct {
	Id       int64
	Next     *statementLinkedRow
	Optional *int
	Name     string
	Rows     []statementLinkedRow
	Dynamic  any
}

func TestNestedStatementAuthority(t *testing.T) {
	const model = "example.com/app/model"
	catalog := typecatalog.NewCatalog()
	dir := t.TempDir()
	files := map[string]string{
		"go.mod": "module example.com/app\n\ngo 1.24\n",
		"model/row.go": `package model
import (alias "example.com/app/foreign"; "time")
type Row struct { Id int64; Next *Row; Optional *int; Name string; Rows []Row; IDs []int; Dynamic any; Detail *alias.Detail; Clock time.Time; hidden int }
type Embed struct{ Id int }
type Promoted struct{ *Embed }
type Left struct{ Id int };type Right struct{ Id int };type Ambiguous struct {Left;Right}
`,
		"foreign/detail.go": "package foreign\ntype Detail struct { Id int64; Optional *string }\n",
	}
	for name, body := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	for _, pkg := range []string{"model", "foreign"} {
		loaded, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(dir), pkg)
		if err != nil {
			t.Fatal(err)
		}
		if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, loaded); err != nil {
			t.Fatal(err)
		}
	}
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[statementLinkedRow](), x.WithName("LinkedRow"), x.WithPkgPath(model))); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	contracts := &gen.Plan{Package: "example.com/app/generated", Imports: []spec.ImportSpec{{Alias: "shared", Package: model}}}
	t.Run("typed Update record leaf", func(t *testing.T) {
		authority := map[string]plan.StatementSelector{
			"Input.Table": {Path: plan.FieldPath{"Input", "Table"}, Type: spec.TypeRef{Name: "string"}},
			"Input.Row":   {Path: plan.FieldPath{"Input", "Row"}, Type: spec.TypeRef{Name: "*shared.LinkedRow"}},
		}
		source := `$dml.Update($Input.Table,$Input.Row.Next)`
		if err := extendStatementAuthority(source, authority, contracts, resolver); err != nil {
			t.Fatal(err)
		}
		field, ok := authority["Input.Row.Next"]
		if !ok || !compiler.StatementRecordType(field.Type.Name) || !reflect.DeepEqual(field.PointerParents, []plan.FieldPath{{"Input", "Row"}}) {
			t.Fatalf("field=%+v", field)
		}
		if _, err := (&compiler.Compiler{}).CompileStatement(source, authority); err != nil {
			t.Fatal(err)
		}
	})
	for _, tc := range []struct {
		typ, path, want string
		parents         []plan.FieldPath
	}{
		{"*shared.Row", "Output.Row.Id", "int64", []plan.FieldPath{{"Output", "Row"}}},
		{"*shared.LinkedRow", "Output.Row.Id", "int64", []plan.FieldPath{{"Output", "Row"}}},
		{"*shared.Row", "Output.Row.Detail.Id", "int64", []plan.FieldPath{{"Output", "Row"}, {"Output", "Row", "Detail"}}},
		{"*shared.Row", "Output.Row.Detail.Optional", "*string", []plan.FieldPath{{"Output", "Row"}, {"Output", "Row", "Detail"}}},
		{"*shared.Row", "Output.Row.Clock", "time.Time", []plan.FieldPath{{"Output", "Row"}}},
		{"*shared.Row", "Output.Row.Next.Next.Id", "int64", []plan.FieldPath{{"Output", "Row"}, {"Output", "Row", "Next"}, {"Output", "Row", "Next", "Next"}}},
		{"*shared.Promoted", "Output.Row.Id", "int", []plan.FieldPath{{"Output", "Row"}, {"Output", "Row", "Embed"}}},
	} {
		t.Run(tc.typ+tc.path, func(t *testing.T) {
			authority := map[string]plan.StatementSelector{"Output.Row": {Path: plan.FieldPath{"Output", "Row"}, Type: spec.TypeRef{Name: tc.typ}, Addressable: true}}
			source := `$dml.ExecuteWithResult("INSERT INTO records(id) VALUES (1)", $` + tc.path + `)`
			if err := extendStatementAuthority(source, authority, contracts, resolver); err != nil {
				t.Fatal(err)
			}
			field := authority[tc.path]
			if field.Type.Name != tc.want || !field.Addressable || !reflect.DeepEqual(field.PointerParents, tc.parents) {
				t.Fatalf("authority = %+v", field)
			}
		})
	}
	for _, tc := range []struct{ typ, path string }{
		{"*shared.Row", "Output.Row.hidden"}, {"*shared.Row", "Output.Row.Unknown"}, {"*shared.Row", "Output.Row.Name.Id"}, {"*shared.Row", "Output.Row.Rows.Id"}, {"*shared.Row", "Output.Row.Dynamic.Id"}, {"*shared.Row", "Output.Row.IDs"}, {"[]shared.Row", "Output.Row.Id"}, {"[2]shared.Row", "Output.Row.Id"}, {"map[string]shared.Row", "Output.Row.Id"}, {"**shared.Row", "Output.Row.Id"}, {"*shared.Ambiguous", "Output.Row.Id"},
	} {
		t.Run("reject "+tc.typ+tc.path, func(t *testing.T) {
			authority := map[string]plan.StatementSelector{"Output.Row": {Path: plan.FieldPath{"Output", "Row"}, Type: spec.TypeRef{Name: tc.typ}, Addressable: true}}
			if err := extendStatementAuthority(`$dml.ExecuteWithResult("INSERT INTO records(id) VALUES (1)", $`+tc.path+`)`, authority, contracts, resolver); err == nil {
				t.Fatal("unsupported traversal acquired authority")
			}
		})
	}
	for _, path := range []string{"Output.Row.Optional", "Output.Row.Name", "Output.Row.Rows", "Output.Row.Dynamic", "Output.Row.Next", "Output.Row.Unknown", "Output.Row[0].Id", "Output.Row.Next().Id"} {
		t.Run("result "+path, func(t *testing.T) {
			authority := map[string]plan.StatementSelector{"Output.Row": {Path: plan.FieldPath{"Output", "Row"}, Type: spec.TypeRef{Name: "*shared.Row"}, Addressable: true}}
			source := `$dml.ExecuteWithResult("INSERT INTO records(id) VALUES (1)", $` + path + `)`
			err := extendStatementAuthority(source, authority, contracts, resolver)
			if err == nil {
				_, err = (&compiler.Compiler{}).CompileStatement(source, authority)
			}
			if err == nil {
				t.Fatal("invalid result accepted")
			}
		})
	}
	// A referenced recursive path is finite; an unrelated recursive descriptor is
	// neither enumerated nor asked to supply authority.
	authority := map[string]plan.StatementSelector{}
	if err := extendStatementAuthority(`$dml.ExecuteWithResult("INSERT INTO records(id) VALUES (1)", $Output.Row.Id)`, authority, contracts, resolver); err != nil {
		t.Fatal(err)
	}
	if _, err := (&compiler.Compiler{}).CompileStatement(`$dml.ExecuteWithResult("INSERT INTO records(id) VALUES (1)", $Output.Row.Id)`, authority); err == nil || !strings.Contains(err.Error(), "unresolved canonical selector") {
		t.Fatalf("unadmitted root acquired authority: %v", err)
	}
}
