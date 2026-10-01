package compiler

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestBuildCurrentWildcardUsesPhysicalNonkeySource(t *testing.T) {
	for _, sql := range []string{"SELECT * FROM records", "SELECT * FROM (SELECT c.* FROM records c) rows", "SELECT id,preamble AS display FROM records"} {
		t.Run(sql, func(t *testing.T) {
			view := &spec.View{Name: "Records", Source: &spec.ViewSource{Table: "records", SQL: sql}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, Type: spec.TypeRef{Name: "string"}}, {Name: "Caption", Source: "preamble", Type: spec.TypeRef{Name: "string", Pointer: true}, Tag: `sqlx:"preamble"`}}}
			generated, err := (&Compiler{}).BuildInput(Request{Component: &spec.Component{Name: "Records", RootView: view}, Operation: plan.OperationPatch}, "Record")
			if err != nil {
				t.Fatal(err)
			}
			current := generated.Component.Views[0]
			wanted := "r.preamble"
			if strings.Contains(sql, "AS display") {
				wanted = "r.display"
			}
			if !strings.Contains(current.Source.SQL, wanted) || strings.Contains(current.Source.SQL, "r.Caption") {
				t.Fatalf("current source %s", current.Source.SQL)
			}
			if current.Columns[1].Name != "Caption" {
				t.Fatal("Go alias lost")
			}
		})
	}
}
