package generate

import (
	"github.com/viant/datly/spec"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"
)

func TestGeneratedDocumentationOrigins(t *testing.T) {
	c := &spec.Component{Name: "Users", RootView: &spec.View{Name: "Users", Source: &spec.ViewSource{SQL: "SELECT u.id AS user_id,COUNT(*) AS total FROM users u GROUP BY u.id"}, Columns: []*spec.Column{{Name: "UserID", Source: "user_id", Tag: `docTable:"wrong" docColumn:"wrong"`, Type: spec.TypeRef{Name: "int"}}, {Name: "Total", Source: "total", Type: spec.TypeRef{Name: "int"}}}}}
	plan, err := New(Input{Component: c}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range plan.Views {
		for _, f := range v.Fields {
			if reflect.StructTag(f.Tag).Get("docTable") != "" || reflect.StructTag(f.Tag).Get("docColumn") != "" {
				t.Fatalf("generated documentation transport: %s", f.Tag)
			}

		}
	}

}

func TestBorrowedDocumentationDoesNotChangeRowAuthority(t *testing.T) {
	source := "package p; type Row struct { ID int `sqlx:\"id\" docTable:\"users\" docColumn:\"id\"` }"
	file, err := parser.ParseFile(token.NewFileSet(), "row.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	decl := file.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec)
	want := []Field{{Name: "ID", Type: "int", Tag: `sqlx:"id"`}}
	if err := validateBorrowedStruct(decl, nil, "example.com/p", want); err != nil {
		t.Fatal(err)
	}
	want[0].Tag = `sqlx:"id" docColumn:"wrong" docTable:"wrong"`
	if err := validateBorrowedStruct(decl, nil, "example.com/p", want); err != nil {
		t.Fatal("obsolete expected tags became authority", err)
	}
	for _, supported := range []string{`sqlx:"id" json:"different"`, `sqlx:"id" codec:"changed"`, `sqlx:"id" setMarker:"true"`} {
		want[0].Tag = supported
		if err := validateBorrowedStruct(decl, nil, "example.com/p", want); err == nil {
			t.Fatalf("supported metadata drift accepted: %s", supported)
		}
	}
	want[0].Type = "string"
	want[0].Tag = `sqlx:"id"`
	if err := validateBorrowedStruct(decl, nil, "example.com/p", want); err == nil {
		t.Fatal("type drift accepted")
	}
	want[0].Type = "int"
	want[0].Tag = `sqlx:"other_id"`
	if err := validateBorrowedStruct(decl, nil, "example.com/p", want); err == nil {
		t.Fatal("physical mapping drift accepted")
	}
}

func TestGeneratedPhysicalProjectionAliasControls(t *testing.T) {
	component := &spec.Component{Name: "Users", RootView: &spec.View{Name: "Users", Source: &spec.ViewSource{SQL: "SELECT id AS user_id FROM users"}, Columns: []*spec.Column{{Name: "user_id", Source: "id", Output: "user_id", Selector: "user_selector", Type: spec.TypeRef{Name: "int"}}}}}
	plan, err := New(Input{Component: component}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, view := range plan.Views {
		for _, field := range view.Fields {
			metadata := reflect.StructTag(field.Tag)
			if metadata.Get("sqlOutput") != "user_id" {
				continue
			}
			found = true
			if metadata.Get("sqlx") != "id|user_id" || metadata.Get("selectorAlias") != "user_selector" {
				t.Fatalf("physical/projection controls drifted: %s", field.Tag)
			}
			if metadata.Get("docTable") != "" || metadata.Get("docColumn") != "" {
				t.Fatalf("documentation transport emitted: %s", field.Tag)
			}
		}
	}
	if !found {
		t.Fatal("generated alias field missing")
	}
	if component.RootView.Source.SQL != "SELECT id AS user_id FROM users" || component.RootView.Columns[0].Source != "id" {
		t.Fatal("generation mutated source SQL/mapping")
	}
}
