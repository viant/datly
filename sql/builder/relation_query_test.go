package builder

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestPreparedRelationSQL_NormalizesAndExpandsNonWindowSQL(t *testing.T) {
	component := &spec.Component{
		Name: "UsersComponent",
		RootView: &spec.View{
			Name: "Users",
		},
	}
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{SQL: ` ? SELECT COUNT(1) FROM ($View.Users.NonWindowSQL) t `}, reflect.TypeOf(struct {
		Total int
	}{}))
	if relation == nil {
		t.Fatalf("expected summary relation")
	}
	actual, err := PreparedRelationSQL(component, relation, "SELECT id FROM users")
	if err != nil {
		t.Fatalf("PreparedRelationSQL failed: %v", err)
	}
	if actual != "SELECT COUNT(1) FROM (SELECT id FROM users) t" {
		t.Fatalf("unexpected prepared summary SQL: %q", actual)
	}
}

func TestPreparedRelationSQL_ErrorsOnMissingRootNonWindowSQL(t *testing.T) {
	component := &spec.Component{Name: "Users"}
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{SQL: `SELECT COUNT(1) FROM ($View.NonWindowSQL) t`}, reflect.TypeOf(struct {
		Total int
	}{}))
	if relation == nil {
		t.Fatalf("expected summary relation")
	}
	if _, err := PreparedRelationSQL(component, relation, ""); err == nil {
		t.Fatalf("expected missing root non-window SQL to fail")
	} else if !strings.Contains(err.Error(), "prepared relation query") || !strings.Contains(err.Error(), "Summary") {
		t.Fatalf("unexpected relation-query error: %v", err)
	}
}

func TestPreparedRelationSQL_RejectsUnknownParentAlias(t *testing.T) {
	component := &spec.Component{Name: "Users", RootView: &spec.View{Name: "Users"}}
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
		SQL: `SELECT COUNT(1) FROM ($View.Other.NonWindowSQL) parent`,
	}, reflect.TypeOf(struct{ Total int }{}))
	_, err := PreparedRelationSQL(component, relation, "SELECT id FROM users")
	if err == nil || !strings.Contains(err.Error(), `unknown parent view alias "Other"`) {
		t.Fatalf("expected unknown alias error, got %v", err)
	}
}

func TestPreparedRelationSQL_ReturnsEmptyForNonCandidate(t *testing.T) {
	actual, err := PreparedRelationSQL(&spec.Component{Name: "Users"}, nil, "SELECT 1")
	if err != nil {
		t.Fatalf("PreparedRelationSQL failed: %v", err)
	}
	if actual != "" {
		t.Fatalf("expected empty SQL for non-candidate relation, got %q", actual)
	}
}

func TestNormalizePreparedRelationSQL(t *testing.T) {
	actual := NormalizePreparedRelationSQL(" ? SELECT 1 ")
	if actual != "SELECT 1" {
		t.Fatalf("unexpected normalized prepared SQL: %q", actual)
	}
}

func TestPreparedRelationNonWindowTokensAndAliases(t *testing.T) {
	component := &spec.Component{
		Name: "UsersComponent",
		RootView: &spec.View{
			Name: "Users",
		},
	}
	tokens := PreparedRelationNonWindowTokens(component)
	if len(tokens) < 2 {
		t.Fatalf("expected multiple non-window tokens, got %+v", tokens)
	}
	seen := map[string]bool{}
	for _, token := range tokens {
		seen[token] = true
	}
	if !seen["$View.NonWindowSQL"] || !seen["$View.Users.NonWindowSQL"] || !seen["$View.UsersComponent.NonWindowSQL"] {
		t.Fatalf("unexpected token set: %+v", tokens)
	}
}

func TestMissingPreparedRelationRootSQL_DefaultNames(t *testing.T) {
	err := MissingPreparedRelationRootSQL(nil, &data.Relation{})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "component ") || !strings.Contains(err.Error(), "<unnamed>") {
		t.Fatalf("unexpected default-name error: %v", err)
	}
}

func TestPreparedRelationSQL_ReturnsEmptyForBlankRelationSQL(t *testing.T) {
	relation := &data.Relation{
		Kind:        spec.RelationKindDerived,
		Name:        "Summary",
		Holder:      "Summary",
		Cardinality: spec.CardinalityOne,
		Of: &data.RelationRef{
			View:          &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: " ? "}}},
			MatchStrategy: data.MatchSequential,
		},
	}
	actual, err := PreparedRelationSQL(&spec.Component{Name: "Users"}, relation, "SELECT 1")
	if err != nil {
		t.Fatalf("PreparedRelationSQL failed: %v", err)
	}
	if actual != "" {
		t.Fatalf("expected empty SQL for blank relation source, got %q", actual)
	}
}
