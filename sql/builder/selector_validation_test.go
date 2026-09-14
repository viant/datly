package builder

import (
	"context"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/criteria"
	"github.com/viant/sqlx/io/read/cache"
	xstate "github.com/viant/xdatly/state"
	"reflect"
	"testing"
)

func TestRegisteredCriteriaIncludesAuthoredProjection(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE entries(id INTEGER, name TEXT)", "INSERT INTO entries VALUES(1,'alpha'),(2,'beta')"); err != nil {
		t.Fatal(err)
	}
	compiled := &criteria.Compiler{Columns: map[string]criteria.Column{"id": {Expression: "id", Type: reflect.TypeOf(int8(0))}}}
	for _, tt := range []struct {
		name, expression string
		allowed          []spec.FieldPath
		reject           bool
	}{
		{"authored nonoutput column", "name='alpha'", []spec.FieldPath{"name"}, false},
		{"denied authored column", "name='alpha'", []spec.FieldPath{"id"}, true},
		{"unknown column", "secret='alpha'", []spec.FieldPath{"*"}, true},
		{"typed metadata wins", "id=256", []spec.FieldPath{"id"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			query, err := NewBuilder().Build(ctx, WithBuilderSQL("SELECT id, name FROM entries"), WithBuilderCriteriaCompiler(compiled), WithBuilderSelectorPolicy(&spec.Selector{AllowCriteria: true, Filterable: tt.allowed}), WithBuilderSelector(&xstate.Selector{Criteria: tt.expression}))
			if tt.reject {
				if err == nil {
					t.Fatal("unsafe criteria accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			type row struct {
				ID   int    `sqlx:"id"`
				Name string `sqlx:"name"`
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: query.SQL, Args: query.Args}, []row{{1, "alpha"}})
		})
	}
}

func TestBuilderValidatedCriteriaSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, tenant INTEGER)", "INSERT INTO records VALUES(1,7),(2,7),(3,8)"); err != nil {
		t.Fatal(err)
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	for _, source := range []string{
		"SELECT id FROM records WHERE tenant=7 ORDER BY id",
		"SELECT id FROM records WHERE tenant=7 $AND_SELECTOR_CRITERIA ORDER BY id",
	} {
		for _, bound := range []bool{false, true} {
			for _, tt := range []struct {
				name, criteria string
				reject         bool
			}{
				{"or scope", "id=2 OR id=3", false},
				{"denied column", "tenant=8", true},
				{"injection", "id=2 OR 1=1", true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					selector := &xstate.Selector{Criteria: tt.criteria}
					options := []BuilderOption{WithBuilderSQL(source), WithBuilderSelector(selector), WithBuilderView(&data.View{Columns: []*data.Column{{Name: "id"}, {Name: "tenant"}}}), WithBuilderSelectorPolicy(&spec.Selector{AllowCriteria: true, Filterable: []spec.FieldPath{"id"}})}
					var query *cache.ParmetrizedQuery
					var err error
					if bound {
						query, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: source}, options...)
					} else {
						query, err = NewBuilder().Build(ctx, options...)
					}
					if selector.Criteria != tt.criteria {
						t.Fatal("caller selector mutated")
					}
					if tt.reject {
						if err == nil {
							t.Fatal("expected rejection")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					h.AssertQuery(t, ctx, sqlite.Query{SQL: query.SQL, Args: query.Args}, []row{{ID: 2}})
				})
			}
		}
	}
}

func TestBuilderCriteriaAliasPolicySQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2)"); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, source, criteria string
		policy                 *spec.Selector
		reject                 bool
	}{
		{"nil policy known projection", "SELECT id FROM records", "id=1", nil, false},
		{"nil policy wildcard", "SELECT * FROM records", "id=1", nil, true},
		{"nil policy unknown column", "SELECT id FROM records", "secret=1", nil, true},
		{"physical allowed", "SELECT id AS public_id FROM records", "id=1", &spec.Selector{AllowCriteria: true, Filterable: []spec.FieldPath{"id"}}, false},
		{"alias denied", "SELECT id AS public_id FROM records", "public_id=1", &spec.Selector{AllowCriteria: true, Filterable: []spec.FieldPath{"id"}}, true},
		{"alias allowed", "SELECT id AS public_id FROM records", "public_id=1", &spec.Selector{AllowCriteria: true, Filterable: []spec.FieldPath{"public_id"}}, false},
		{"physical denied", "SELECT id AS public_id FROM records", "id=1", &spec.Selector{AllowCriteria: true, Filterable: []spec.FieldPath{"public_id"}}, true},
		{"empty allowlist", "SELECT id FROM records", "id=1", &spec.Selector{AllowCriteria: true}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			query, err := NewBuilder().Build(ctx, WithBuilderSQL(tt.source), WithBuilderSelectorPolicy(tt.policy), WithBuilderSelector(&xstate.Selector{Criteria: tt.criteria}))
			if tt.reject {
				if err == nil {
					t.Fatal("expected rejection")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			type row struct {
				ID int `sqlx:"id"`
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id FROM records WHERE id IN (" + query.SQL + ")", Args: query.Args}, []row{{ID: 1}})
		})
	}
}
