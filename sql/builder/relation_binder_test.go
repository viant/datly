package builder

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestPreparedRelationBinder_Bind_ExpandsRepeatedNonWindowSQLAndPreservesArgOrder(t *testing.T) {
	component := &spec.Component{
		Name: "UsersComponent",
		RootView: &spec.View{
			Name: "Users",
		},
	}
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
		SQL: `SELECT :limit AS limit_value, (SELECT COUNT(1) FROM ($View.Users.NonWindowSQL) a) AS left_total, (SELECT COUNT(1) FROM ($View.NonWindowSQL) b) AS right_total, :offset AS offset_value`,
	}, reflect.TypeOf(struct {
		Total int
	}{}))
	if relation == nil {
		t.Fatalf("expected summary relation")
	}
	binder := PreparedRelationBinder{
		Component:         component,
		Relation:          relation,
		RootNonWindowSQL:  "SELECT id FROM users WHERE tenant_id = ? AND active = ?",
		RootArgs:          []any{7, true},
		ParameterResolver: parameterResolver(map[string]any{"limit": 10, "offset": 20}),
	}
	actualSQL, actualArgs, err := binder.Bind()
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	expectedSQL := "SELECT ? AS limit_value, (SELECT COUNT(1) FROM (SELECT id FROM users WHERE tenant_id = ? AND active = ?) a) AS left_total, (SELECT COUNT(1) FROM (SELECT id FROM users WHERE tenant_id = ? AND active = ?) b) AS right_total, ? AS offset_value"
	if actualSQL != expectedSQL {
		t.Fatalf("unexpected bound SQL: %q", actualSQL)
	}
	expectedArgs := []any{10, 7, true, 7, true, 20}
	if !reflect.DeepEqual(actualArgs, expectedArgs) {
		t.Fatalf("unexpected bound args: %#v", actualArgs)
	}
}

func TestPreparedRelationBinder_Bind_SkipsProtectedNonWindowSQL(t *testing.T) {
	component := &spec.Component{Name: "Users", RootView: &spec.View{Name: "Users"}}
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
		SQL: `SELECT '$View.Users.NonWindowSQL' AS literal_value,
COUNT(*) AS total FROM ($View.Users.NonWindowSQL) parent
-- $View.Users.NonWindowSQL`,
	}, reflect.TypeOf(struct{ Total int }{}))
	binder := PreparedRelationBinder{
		Component:        component,
		Relation:         relation,
		RootNonWindowSQL: "SELECT id FROM users WHERE tenant_id = ? AND active = ?",
		RootArgs:         []any{7, true},
	}
	actualSQL, actualArgs, err := binder.Bind()
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	if !strings.Contains(actualSQL, `'$View.Users.NonWindowSQL'`) || !strings.Contains(actualSQL, `-- $View.Users.NonWindowSQL`) {
		t.Fatalf("protected SQL text changed: %s", actualSQL)
	}
	if strings.Count(actualSQL, "SELECT id FROM users WHERE tenant_id = ? AND active = ?") != 1 {
		t.Fatalf("expected one executable parent expansion: %s", actualSQL)
	}
	if !reflect.DeepEqual(actualArgs, []any{7, true}) {
		t.Fatalf("unexpected args: %#v", actualArgs)
	}
}

func TestPreparedRelationBinder_Bind_RejectsUnknownParentAliasWithRootArgs(t *testing.T) {
	component := &spec.Component{Name: "Users", RootView: &spec.View{Name: "Users"}}
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
		SQL: `SELECT COUNT(*) FROM ($View.Other.NonWindowSQL) parent`,
	}, reflect.TypeOf(struct{ Total int }{}))
	binder := PreparedRelationBinder{
		Component:        component,
		Relation:         relation,
		RootNonWindowSQL: "SELECT id FROM users WHERE tenant_id = ?",
		RootArgs:         []any{7},
	}
	_, _, err := binder.Bind()
	if err == nil || !strings.Contains(err.Error(), `unknown parent view alias "Other"`) {
		t.Fatalf("expected unknown alias error, got %v", err)
	}
}

func TestPreparedRelationBinder_Bind_ErrorsOnMissingRootNonWindowSQLForSegmentBinding(t *testing.T) {
	component := &spec.Component{Name: "Users"}
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
		SQL: `SELECT COUNT(1) FROM ($View.NonWindowSQL) t`,
	}, reflect.TypeOf(struct {
		Total int
	}{}))
	if relation == nil {
		t.Fatalf("expected summary relation")
	}
	binder := PreparedRelationBinder{
		Component: component,
		Relation:  relation,
		RootArgs:  []any{1},
	}
	_, _, err := binder.Bind()
	if err == nil {
		t.Fatalf("expected missing root non-window SQL to fail")
	}
	if !strings.Contains(err.Error(), "prepared relation query") || !strings.Contains(err.Error(), "Summary") {
		t.Fatalf("unexpected missing-root error: %v", err)
	}
}

func TestPreparedRelationBinder_Bind_UsesPreparedSQLPathWithoutRootArgs(t *testing.T) {
	component := &spec.Component{
		Name: "UsersComponent",
		RootView: &spec.View{
			Name: "Users",
		},
	}
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
		SQL: `SELECT :limit AS limit_value`,
	}, reflect.TypeOf(struct {
		Total int
	}{}))
	if relation == nil {
		t.Fatalf("expected summary relation")
	}
	binder := PreparedRelationBinder{
		Component:         component,
		Relation:          relation,
		ParameterResolver: parameterResolver(map[string]any{"limit": 10}),
	}
	actualSQL, actualArgs, err := binder.Bind()
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	if actualSQL != "SELECT ? AS limit_value" {
		t.Fatalf("unexpected bound SQL: %q", actualSQL)
	}
	expectedArgs := []any{10}
	if !reflect.DeepEqual(actualArgs, expectedArgs) {
		t.Fatalf("unexpected bound args: %#v", actualArgs)
	}
}

func TestPreparedRelationBinder_Bind_ReturnsEmptyForBlankAuthoredSQL(t *testing.T) {
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{SQL: " ? "}, reflect.TypeOf(struct {
		Total int
	}{}))
	if relation == nil {
		t.Fatalf("expected summary relation")
	}
	binder := PreparedRelationBinder{Relation: relation, RootArgs: []any{1}}
	actualSQL, actualArgs, err := binder.Bind()
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	if actualSQL != "" || actualArgs != nil {
		t.Fatalf("expected empty bind result, got %q %#v", actualSQL, actualArgs)
	}
}

func TestPreparedRelationBinder_bindPreparedSQL_ReturnsEmptyWhenPreparedRelationSQLEmpty(t *testing.T) {
	binder := PreparedRelationBinder{
		Relation: &data.Relation{
			Of: &data.RelationRef{
				View: &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: " ? "}}},
			},
		},
	}
	actualSQL, actualArgs, err := binder.bindPreparedSQL()
	if err != nil {
		t.Fatalf("bindPreparedSQL failed: %v", err)
	}
	if actualSQL != "" || actualArgs != nil {
		t.Fatalf("expected empty prepared SQL bind result, got %q %#v", actualSQL, actualArgs)
	}
}

func TestPreparedRelationBinder_nextToken_NoMatch(t *testing.T) {
	binder := PreparedRelationBinder{}
	token, idx := binder.nextToken("SELECT 1")
	if token != "" || idx != -1 {
		t.Fatalf("expected no token match, got %q %d", token, idx)
	}
}

func TestPreparedRelationBinder_Bind_GuardsAndHelpers(t *testing.T) {
	t.Run("nil relation returns empty", func(t *testing.T) {
		binder := PreparedRelationBinder{}
		actualSQL, actualArgs, err := binder.Bind()
		if err != nil {
			t.Fatalf("unexpected bind guard error: %v", err)
		}
		if actualSQL != "" || actualArgs != nil {
			t.Fatalf("expected empty guard bind result, got %q %#v", actualSQL, actualArgs)
		}
	})

	t.Run("bind segments blank sql", func(t *testing.T) {
		binder := PreparedRelationBinder{}
		actualSQL, actualArgs, err := binder.bindSegments("   ")
		if err != nil {
			t.Fatalf("unexpected blank bindSegments error: %v", err)
		}
		if actualSQL != "" || actualArgs != nil {
			t.Fatalf("expected empty blank bindSegments result, got %q %#v", actualSQL, actualArgs)
		}
	})

	t.Run("bind segments without non window token binds suffix only", func(t *testing.T) {
		binder := PreparedRelationBinder{ParameterResolver: parameterResolver(map[string]any{"limit": 11})}
		actualSQL, actualArgs, err := binder.bindSegments("SELECT :limit AS limit_value")
		if err != nil {
			t.Fatalf("unexpected suffix-only bindSegments error: %v", err)
		}
		if actualSQL != "SELECT ? AS limit_value" {
			t.Fatalf("unexpected suffix-only bound SQL: %q", actualSQL)
		}
		if !reflect.DeepEqual(actualArgs, []any{11}) {
			t.Fatalf("unexpected suffix-only bound args: %#v", actualArgs)
		}
	})

	t.Run("bind segments errors when token requires missing root sql", func(t *testing.T) {
		component := &spec.Component{Name: "Users"}
		relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
			SQL: `SELECT COUNT(1) FROM ($View.NonWindowSQL) t`,
		}, reflect.TypeOf(struct{ Total int }{}))
		if relation == nil {
			t.Fatalf("expected summary relation")
		}
		binder := PreparedRelationBinder{
			Component: component,
			Relation:  relation,
		}
		if _, _, err := binder.bindSegments("SELECT COUNT(1) FROM ($View.NonWindowSQL) t"); err == nil {
			t.Fatalf("expected missing root sql bindSegments error")
		}
	})

	t.Run("bind segments errors on missing suffix placeholder", func(t *testing.T) {
		component := &spec.Component{
			Name: "UsersComponent",
			RootView: &spec.View{
				Name: "Users",
			},
		}
		relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
			SQL: `SELECT COUNT(1) FROM ($View.NonWindowSQL) t WHERE id = :Missing`,
		}, reflect.TypeOf(struct{ Total int }{}))
		if relation == nil {
			t.Fatalf("expected summary relation")
		}
		binder := PreparedRelationBinder{
			Component:        component,
			Relation:         relation,
			RootNonWindowSQL: "SELECT id FROM users",
			RootArgs:         []any{1},
		}
		if _, _, err := binder.bindSegments("SELECT COUNT(1) FROM ($View.NonWindowSQL) t WHERE id = :Missing"); err == nil {
			t.Fatalf("expected missing suffix placeholder error")
		}
	})

	t.Run("bind segments errors on missing prefix placeholder", func(t *testing.T) {
		component := &spec.Component{
			Name: "UsersComponent",
			RootView: &spec.View{
				Name: "Users",
			},
		}
		relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
			SQL: `SELECT :Missing, COUNT(1) FROM ($View.NonWindowSQL) t`,
		}, reflect.TypeOf(struct{ Total int }{}))
		if relation == nil {
			t.Fatalf("expected summary relation")
		}
		binder := PreparedRelationBinder{
			Component:        component,
			Relation:         relation,
			RootNonWindowSQL: "SELECT id FROM users",
			RootArgs:         []any{1},
		}
		if _, _, err := binder.bindSegments("SELECT :Missing, COUNT(1) FROM ($View.NonWindowSQL) t"); err == nil {
			t.Fatalf("expected missing prefix placeholder error")
		}
	})

	t.Run("next token picks earliest alias", func(t *testing.T) {
		component := &spec.Component{
			Name: "UsersComponent",
			RootView: &spec.View{
				Name: "Users",
			},
		}
		binder := PreparedRelationBinder{Component: component}
		token, idx := binder.nextToken("SELECT 1 FROM ($View.NonWindowSQL) a JOIN ($View.Users.NonWindowSQL) b ON 1=1")
		if token != "$View.NonWindowSQL" || idx == -1 {
			t.Fatalf("expected earliest non-window token, got %q %d", token, idx)
		}
	})
}

func TestPreparedRelationBinder_BindPreparedSQL_ErrorsOnMissingInputValue(t *testing.T) {
	component := &spec.Component{
		Name: "UsersComponent",
		RootView: &spec.View{
			Name: "Users",
		},
	}
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
		SQL: `SELECT :limit AS limit_value`,
	}, reflect.TypeOf(struct {
		Total int
	}{}))
	if relation == nil {
		t.Fatalf("expected summary relation")
	}
	binder := PreparedRelationBinder{
		Component: component,
		Relation:  relation,
	}
	_, _, err := binder.bindPreparedSQL()
	if err == nil || !strings.Contains(err.Error(), "parameter resolver is required for named placeholder limit") {
		t.Fatalf("expected missing input placeholder error, got %v", err)
	}
}

func TestPreparedRelationBinder_bindPreparedSQL_ErrorsOnMissingRootNonWindowSQL(t *testing.T) {
	component := &spec.Component{Name: "Users"}
	relation := mustSummaryRelation(t, &data.View{}, &spec.ViewSource{
		SQL: `SELECT COUNT(1) FROM ($View.NonWindowSQL) t`,
	}, reflect.TypeOf(struct {
		Total int
	}{}))
	if relation == nil {
		t.Fatalf("expected summary relation")
	}
	binder := PreparedRelationBinder{
		Component: component,
		Relation:  relation,
	}
	_, _, err := binder.bindPreparedSQL()
	if err == nil || !strings.Contains(err.Error(), "prepared relation query") {
		t.Fatalf("expected missing root non-window SQL error, got %v", err)
	}
}
