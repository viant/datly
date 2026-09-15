package builder

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	sqlconfig "github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/read/cache"
	xstate "github.com/viant/xdatly/state"
)

func TestNamedSourcePredicateScopeSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, `CREATE TABLE children(id INTEGER,parent_id INTEGER,tenant_id INTEGER,label TEXT,active INTEGER)`,
		`INSERT INTO children VALUES(1,10,7,NULL,1),(2,10,7,'b',1),(3,20,7,'other',1),(4,10,8,'tenant',1),(5,10,7,'hidden',0),(6,10,7,'z',1)`); err != nil {
		t.Fatal(err)
	}
	dialect, err := sqlconfig.Dialect(ctx, h.DB)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, source, baseExpression                                               string
		composite, explicit, empty, skip, allowNulls, paging, partition, baseOrder bool
		positional                                                                 bool
		want                                                                       []int
		args                                                                       []any
	}{
		{name: "narrow before named binding", source: `SELECT :unused AS dropped,child.id,child.label FROM (SELECT c.* FROM children c WHERE c.active=:active) child`, want: []int{1, 2, 6}, args: []any{1, 10, 7}},
		{name: "scalar nullable", want: []int{1, 2, 6}, args: []any{1, 10, 7}},
		{name: "scalar allow nulls", allowNulls: true, want: []int{1, 2, 6}, args: []any{1, 10, 7}},
		{name: "composite nullable", composite: true, want: []int{1, 2, 6}, args: []any{1, 7, 10, 7}},
		{name: "composite authored positional", composite: true, positional: true, want: []int{1, 2, 6}, args: []any{1, 7, 7, 10}},
		{name: "composite empty", composite: true, empty: true, args: []any{1, 7}},
		{name: "scalar empty", empty: true, args: []any{1, 7}},
		{name: "explicit scalar", explicit: true, want: []int{1, 2, 6}, args: []any{1, 10, 7}},
		{name: "explicit composite", explicit: true, composite: true, want: []int{1, 2, 6}, args: []any{1, 7, 10, 7}},
		{name: "parent filter skipped", skip: true, want: []int{1, 2, 3, 6}, args: []any{1, 7}},
		{name: "projection order and page", paging: true, want: []int{2}, args: []any{1, 10, 7}},
		{name: "source alias partition", partition: true, want: []int{2, 6}, args: []any{1, 10, 7, 1}},
		{name: "qualified base function", baseExpression: "ABS(child.id) DESC", want: []int{6, 2, 1}, args: []any{1, 10, 7}},
		{name: "qualified base ordering", baseOrder: true, want: []int{6, 2, 1}, args: []any{1, 10, 7}},
		{name: "authored disjunction no parent", source: `SELECT child.* FROM (SELECT c.* FROM children c WHERE c.active=:active) child WHERE child.id=1 OR child.active=1`, empty: true, args: []any{1, 7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := tc.source
			if source == "" {
				source = `SELECT child.* FROM (SELECT c.* FROM children c WHERE c.active=:active) child`
			}
			if tc.positional {
				source = strings.Replace(source, ":active", "?", 1)
			}
			if tc.explicit {
				source += " WHERE $COLUMN_IN"
			}
			view := &data.View{Spec: spec.View{Name: "Child", Namespace: "child", Source: &spec.ViewSource{SQL: source}, AllowNulls: &tc.allowNulls, Selector: &spec.Selector{AllowFields: true, AllowCriteria: true, Filterable: []spec.FieldPath{"tenant_id"}, AllowOrderBy: true, AllowLimit: true, AllowOffset: true, AllowPage: true}}, Columns: []*data.Column{
				{Name: "id", Column: "id", Nullable: true, NullFallback: "0"}, {Name: "parent_id", Column: "parent_id"}, {Name: "tenant_id", Column: "tenant_id"}, {Name: "label", Column: "label", Nullable: true, NullFallback: "''"}, {Name: "active", Column: "active"},
			}}
			relation := &data.Relation{Of: &data.RelationRef{View: view, On: data.Links{{Namespace: "child", Column: "parent_id"}}}}
			selector := &xstate.Selector{Criteria: "tenant_id = ?", Placeholders: []any{7}, OrderBy: "id ASC"}
			opts := []BuilderOption{WithBuilderView(view), WithBuilderRelation(relation), WithBuilderDialect(dialect), WithBuilderProjection([]string{"id", "label"}), WithBuilderSelector(selector), WithBuilderParameterResolver(func(name string) (any, bool, error) { return 1, name == "active", nil }), WithBuilderSkipRelationFilter(tc.skip)}
			if tc.composite {
				rows := [][]interface{}{{7, 10}}
				if tc.empty {
					rows = nil
				}
				opts = append(opts, WithBuilderCompositeArgs([]string{"child.tenant_id", "child.parent_id"}, rows))
				if tc.positional {
					opts = append(opts, WithBuilderPositionalArgs([]any{1}))
				}
			} else if !tc.empty && !tc.skip {
				opts = append(opts, WithBuilderPositionalArgs([]any{10}))
			}
			if tc.paging {
				selector.OrderBy = "1 DESC"
				selector.Limit = 1
				selector.Page = 2
			}
			if tc.baseOrder || tc.baseExpression != "" {
				selector.OrderBy = ""
				order := "child.id DESC"
				if tc.baseExpression != "" {
					order = tc.baseExpression
				}
				opts = append(opts, WithBuilderControls(&spec.ViewControls{OrderBy: order}))
			}
			if tc.partition {
				opts = append(opts, WithBuilderPartition(&PartitionInput{Expression: "child.id > ?", Args: []any{1}}))
			}
			query, err := NewBuilder().Build(ctx, opts...)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(query.Args, tc.args) {
				t.Fatalf("args=%v want=%v SQL=%s", query.Args, tc.args, query.SQL)
			}
			rows, err := h.DB.QueryContext(ctx, query.SQL, query.Args...)
			if err != nil {
				t.Fatalf("%v\n%s", err, query.SQL)
			}
			defer rows.Close()
			var ids []int
			for rows.Next() {
				var id int
				var label sql.NullString
				if err := rows.Scan(&id, &label); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
				if id == 1 && label.Valid == tc.allowNulls {
					t.Fatalf("NULL fallback changed: %+v", label)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(ids, tc.want) {
				t.Fatalf("ids=%v want=%v SQL=%s", ids, tc.want, query.SQL)
			}
		})
	}
}

func TestNamedSourceBoundQueryScopeSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, `CREATE TABLE records(id INTEGER,parent_id INTEGER,tenant INTEGER,label TEXT)`, `INSERT INTO records VALUES(1,10,7,NULL),(2,10,7,'b'),(3,20,7,'other')`); err != nil {
		t.Fatal(err)
	}
	view := &data.View{Columns: []*data.Column{{Name: "id", Column: "id"}, {Name: "tenant", Column: "tenant"}, {Name: "label", Column: "label", Nullable: true, NullFallback: "''"}}}
	source := &cache.ParmetrizedQuery{SQL: `SELECT record.* FROM (SELECT r.* FROM records r WHERE r.tenant=?) record WHERE record.parent_id=?`, Args: []interface{}{7, 10}, By: "id", In: []interface{}{2}}
	query, err := NewBuilder().ShapeBound(source, WithBuilderView(view), WithBuilderProjection([]string{"label"}), WithBuilderControls(&spec.ViewControls{OrderBy: "record.id DESC"}), WithBuilderSelector(&xstate.Selector{Criteria: "id > ?", Placeholders: []any{1}}), WithBuilderPartition(&PartitionInput{Expression: "record.id < ?", Args: []any{3}}))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(query.Args, []interface{}{7, 10, 1, 3}) || query.By != "id" || !reflect.DeepEqual(query.In, source.In) {
		t.Fatalf("bound metadata/args=%+v", query)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: query.SQL, Args: query.Args}, []struct{ Label string }{{"b"}})
	if !reflect.DeepEqual(source.Args, []interface{}{7, 10}) {
		t.Fatal("bound source mutated")
	}
}

func TestRelationFilterPrecedesNullKeyFallbackSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, `CREATE TABLE children(id INTEGER,tenant INTEGER,parent_id INTEGER)`, `INSERT INTO children VALUES(1,7,NULL),(2,7,0),(3,8,0)`); err != nil {
		t.Fatal(err)
	}
	dialect, err := sqlconfig.Dialect(ctx, h.DB)
	if err != nil {
		t.Fatal(err)
	}
	for _, composite := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "composite"}[composite], func(t *testing.T) {
			view := &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: `SELECT child.* FROM (SELECT c.* FROM children c WHERE c.tenant=7) child`}}, Columns: []*data.Column{{Name: "id", Column: "id"}, {Name: "tenant", Column: "tenant"}, {Name: "parent_id", Column: "parent_id", Nullable: true, NullFallback: "0"}}}
			relation := &data.Relation{Of: &data.RelationRef{View: view, On: data.Links{{Namespace: "child", Column: "parent_id"}}}}
			opts := []BuilderOption{WithBuilderView(view), WithBuilderRelation(relation), WithBuilderDialect(dialect)}
			if composite {
				opts = append(opts, WithBuilderCompositeArgs([]string{"child.tenant", "child.parent_id"}, [][]interface{}{{7, 0}}))
			} else {
				opts = append(opts, WithBuilderPositionalArgs([]any{0}))
			}
			query, err := NewBuilder().Build(ctx, opts...)
			if err != nil {
				t.Fatal(err)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: query.SQL, Args: query.Args}, []struct {
				ID       int `sqlx:"id"`
				Tenant   int `sqlx:"tenant"`
				ParentID int `sqlx:"parent_id"`
			}{{2, 7, 0}})
		})
	}
}
