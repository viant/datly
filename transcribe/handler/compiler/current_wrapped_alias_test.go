package compiler

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestCurrentWildcardWrapperPreservesDerivedKeyAliases(t *testing.T) {
	for _, projection := range []string{"*", "scope.*"} {
		sql := "SELECT " + projection + " FROM (SELECT p.id AS pod_id, s.id AS student_id FROM (pods) p JOIN students s ON s.account_id=p.account_id) scope"
		view := &spec.View{Name: "Scope", Source: &spec.ViewSource{Table: "pods", SQL: sql}, Columns: []*spec.Column{
			{Name: "pod_id", Source: "id", PrimaryKey: true, Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"pod_id,primaryKey=true"`},
			{Name: "student_id", Source: "student_id", PrimaryKey: true, Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"student_id,primaryKey=true"`},
		}}
		generated, err := (&Compiler{}).BuildInput(Request{Component: &spec.Component{Name: "Scope", RootView: view}, Operation: plan.OperationPatch}, "Record")
		if err != nil {
			t.Fatal(err)
		}
		current := generated.Component.Views[0].Source.SQL
		if !strings.Contains(current, "SELECT r.pod_id, r.student_id FROM (") {
			t.Fatalf("current query lost derived aliases: %s", current)
		}
		for _, parameter := range generated.Component.Parameters {
			if parameter.Name == "ScopeKeys" && parameter.DeclarationSQL != "SELECT PodId AS PodId, StudentId AS StudentId FROM `/`" {
				t.Fatalf("physical id collision changed composite helper: %s", parameter.DeclarationSQL)
			}
		}
	}
}

func TestCurrentSourceWildcardDoesNotAdoptAnotherNamespace(t *testing.T) {
	outputs := currentSourceOutputs("SELECT other.* FROM (SELECT id AS root_key FROM roots) scope JOIN other ON other.id=scope.root_key")
	if outputs["id"] != "" || outputs["root_key"] != "" {
		t.Fatalf("unrelated wildcard adopted root projection: %v", outputs)
	}
}

func TestCurrentSQLResultWinsOverJoinedPhysicalColumnName(t *testing.T) {
	for _, sql := range []string{
		"SELECT o.id,viewer.id AS viewer_user_id FROM objectives o JOIN users viewer ON viewer.id=o.owner_id",
		"SELECT * FROM (SELECT o.id,viewer.id AS viewer_user_id FROM objectives o JOIN users viewer ON viewer.id=o.owner_id) scope",
		"SELECT scope.* FROM (SELECT viewer.id AS viewer_user_id,o.id FROM objectives o JOIN users viewer ON viewer.id=o.owner_id) scope",
	} {
		outputs := currentSourceOutputs(sql)
		if outputs["id"] != "id" || outputs["viewer_user_id"] != "viewer_user_id" {
			t.Fatalf("result labels lost: %v", outputs)
		}
	}
	outputs := currentSourceOutputs("SELECT * FROM (SELECT p.id AS pod_id,s.id AS student_id FROM pods p JOIN students s ON s.pod_id=p.id) scope")
	if outputs["id"] != "" || outputs["pod_id"] != "pod_id" || outputs["student_id"] != "student_id" {
		t.Fatalf("ambiguous physical id adopted: %v", outputs)
	}
}
