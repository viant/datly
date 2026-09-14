package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestReaderColumnDeclarations(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		columns   int
		wantErr   bool
	}{
		{"logical", "SELECT r.*,CAST(r.bounds AS model.Bounds),tag(r.bounds,'sqlx:\"-\"'),tag(r.unit,'internal:\"true\"') FROM records r", 2, false},
		{"physical cast", "SELECT r.*,CAST(r.body AS model.Body) FROM records r", 1, false},
		{"same canonical aliases", "SELECT r.*,CAST(r.body AS model.Body),CAST(r.body AS model2.Body) FROM records r", 1, false},
		{"executable", "SELECT CAST(r.id AS INTEGER) AS id FROM records r", 0, false},
		{"validate", "SELECT r.*,tag(r.name,'validate:\"required\"') FROM records r", 1, false},
		{"validate shorthand", "SELECT r.*,tag(r.name,'validate:required') FROM records r", 1, false},
		{"malformed tag", "SELECT r.*,tag(r.name,'validate:\"required\" trailing') FROM records r", 0, true},
		{"unknown namespace", "SELECT r.*,CAST(other.bounds AS model.Bounds) FROM records r", 0, true},
		{"conflicting cast", "SELECT r.*,CAST(r.body AS model.Body),CAST(r.body AS model.Other) FROM records r", 0, true},
		{"conflicting tag", "SELECT r.*,tag(r.name,'json:\"a\"'),tag(r.name,'json:\"b\"') FROM records r", 0, true},
		{"entire projection", "SELECT CAST(r.body AS model.Body) FROM records r", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: tc.sql}}, SQL: tc.sql, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "model", Package: "example.com/model"}, {Alias: "model2", Package: "example.com/model"}}}})
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Columns) != tc.columns {
				t.Fatalf("columns=%+v", got.Columns)
			}
			if tc.name == "executable" {
				if !strings.Contains(got.Source.SQL, "CAST") {
					t.Fatal(got.Source.SQL)
				}
				return
			}
			if strings.Contains(strings.ToLower(got.Source.SQL), "cast(") || strings.Contains(strings.ToLower(got.Source.SQL), "tag(") {
				t.Fatalf("declaration leaked: %s", got.Source.SQL)
			}
			if tc.name == "logical" {
				if got.Columns[0].Type.Name != "Bounds" || got.Columns[0].Type.Package != "example.com/model" || got.Columns[0].Tag != `sqlx:"-"` || got.Columns[1].Tag != `internal:"true"` {
					t.Fatalf("columns=%+v/%+v", got.Columns[0], got.Columns[1])
				}
			}
			if tc.name == "physical cast" && got.Columns[0].Tag != "" {
				t.Fatal("CAST invented transient mapping")
			}
		})
	}
}

func TestColumnDeclarationRejectsAmbiguousSource(t *testing.T) {
	view := &spec.View{Name: "Records", Source: &spec.ViewSource{}, Columns: []*spec.Column{{Name: "a", Source: "foo"}, {Name: "b", Source: "foo"}}}
	_, err := NewReader().Compile(ReadInput{View: view, SQL: `SELECT r.*,tag(r.foo,'json:"value"') FROM records r`})
	if err == nil || !strings.Contains(err.Error(), "multiple canonical columns") {
		t.Fatalf("err=%v", err)
	}
	if view.Columns[0].Tag != "" || view.Columns[1].Tag != "" {
		t.Fatal("failed declaration mutated source")
	}
}

func TestColumnDeclarationPreservesExistingCanonicalCodec(t *testing.T) {
	view := &spec.View{Name: "Records", Source: &spec.ViewSource{}, Columns: []*spec.Column{{Name: "body", Type: spec.TypeRef{Package: "example.com/model", Name: "Body"}, Tag: `json:"body"`, Codec: &spec.Codec{Body: "existing"}}}}
	got, err := NewReader().Compile(ReadInput{View: view, SQL: `SELECT r.*,CAST(r.body AS model.Body),tag(r.body,'internal:"true"') FROM records r`, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "model", Package: "example.com/model"}}}})
	if err != nil {
		t.Fatal(err)
	}
	column := got.Columns[0]
	if column.Type != view.Columns[0].Type || column.Codec == nil || column.Codec.Body != "existing" || column.Tag != `json:"body" internal:"true"` {
		t.Fatalf("column=%+v", column)
	}
	if view.Columns[0].Tag != `json:"body"` {
		t.Fatal("mutated input column")
	}
}
