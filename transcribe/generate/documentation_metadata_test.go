package generate

import (
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"
)

func TestGeneratedDocumentationOrigins(t *testing.T) {
	c := &spec.Component{Name: "Users", RootView: &spec.View{Name: "Users", Source: &spec.ViewSource{SQL: "SELECT u.id AS user_id,COUNT(*) AS total FROM users u GROUP BY u.id"}, Columns: []*spec.Column{{Name: "UserID", Source: "user_id", Type: spec.TypeRef{Name: "int"}}, {Name: "Total", Source: "total", Type: spec.TypeRef{Name: "int"}}}}}
	plan, err := New(Input{Component: c}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range plan.Views {
		for _, f := range v.Fields {
			switch f.Name {
			case "UserID":
				metadata, err := tag.ParseField(reflect.StructField{Name: f.Name, Tag: reflect.StructTag(f.Tag)})
				if err != nil {
					t.Fatal(err)
				}
				if metadata.DocumentationOrigin == nil || metadata.DocumentationOrigin.Table != "users" || metadata.DocumentationOrigin.Column != "id" {
					t.Fatalf("%s", f.Tag)
				}
			case "Total":
				if reflect.StructTag(f.Tag).Get("docTable") != "-" {
					t.Fatalf("aggregate acquired dictionary origin: %s", f.Tag)
				}
			}
		}
	}
	if c.RootView.DocumentationTable != "" {
		t.Fatal("generation changed caller")
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
	want[0].Tag = `sqlx:"other_id"`
	if err := validateBorrowedStruct(decl, nil, "example.com/p", want); err == nil {
		t.Fatal("physical mapping drift accepted")
	}
}
