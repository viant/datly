package builder

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
)

func TestBuilderCriteriaTokensDataDriven(t *testing.T) {
	type input struct{ ID int }
	scalar := &data.Relation{Of: &data.RelationRef{On: data.Links{{Column: "user_id"}}}}
	composite := &data.Relation{Of: &data.RelationRef{On: data.Links{{Column: "feature_type"}, {Column: "value"}}}}
	rows := [][]interface{}{{"country", "PL"}, {"country", "US"}}
	tests := []struct {
		name       string
		sql        string
		relation   *data.Relation
		positional []any
		columns    []string
		rows       [][]interface{}
		wantSQL    string
		wantArgs   []interface{}
	}{
		{
			name: "scalar and", sql: "SELECT * FROM accounts WHERE id = :ID $AND_CRITERIA ORDER BY id",
			relation: scalar, positional: []any{7, 8},
			wantSQL: "SELECT * FROM accounts WHERE id = ? AND (user_id IN (?,?)) ORDER BY id", wantArgs: []interface{}{10, 7, 8},
		},
		{
			name: "scalar or", sql: "SELECT * FROM accounts WHERE id = :ID $OR_CRITERIA ORDER BY id",
			relation: scalar, positional: []any{7, 8},
			wantSQL: "SELECT * FROM accounts WHERE id = ? OR (user_id IN (?,?)) ORDER BY id", wantArgs: []interface{}{10, 7, 8},
		},
		{
			name: "empty scalar", sql: "SELECT * FROM accounts $WHERE_CRITERIA ORDER BY id",
			relation: scalar, wantSQL: "SELECT * FROM accounts WHERE 1 = 0 ORDER BY id",
		},
		{
			name: "composite where", sql: "SELECT * FROM signals $WHERE_CRITERIA ORDER BY value",
			relation: composite, columns: []string{"feature_type", "value"}, rows: rows,
			wantSQL: "SELECT * FROM signals WHERE (feature_type, value) IN ((?, ?), (?, ?)) ORDER BY value", wantArgs: []interface{}{"country", "PL", "country", "US"},
		},
		{
			name: "composite and", sql: "SELECT * FROM signals WHERE metric > 0 $AND_CRITERIA ORDER BY value",
			relation: composite, columns: []string{"feature_type", "value"}, rows: rows,
			wantSQL: "SELECT * FROM signals WHERE metric > 0 AND ((feature_type, value) IN ((?, ?), (?, ?))) ORDER BY value", wantArgs: []interface{}{"country", "PL", "country", "US"},
		},
		{
			name: "composite or", sql: "SELECT * FROM signals WHERE 1 = 0 $OR_CRITERIA ORDER BY value",
			relation: composite, columns: []string{"feature_type", "value"}, rows: rows,
			wantSQL: "SELECT * FROM signals WHERE 1 = 0 OR ((feature_type, value) IN ((?, ?), (?, ?))) ORDER BY value", wantArgs: []interface{}{"country", "PL", "country", "US"},
		},
		{
			name: "strip absent where", sql: "SELECT * FROM accounts WHERE $WHERE_CRITERIA ORDER BY id",
			wantSQL: "SELECT * FROM accounts ORDER BY id",
		},
		{
			name: "strip absent and", sql: "SELECT * FROM accounts WHERE active = 1 $AND_CRITERIA ORDER BY id",
			wantSQL: "SELECT * FROM accounts WHERE active = 1 ORDER BY id",
		},
		{
			name: "strip absent or", sql: "SELECT * FROM accounts WHERE active = 0 $OR_CRITERIA ORDER BY id",
			wantSQL: "SELECT * FROM accounts WHERE active = 0 ORDER BY id",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := []BuilderOption{
				WithBuilderSQL(test.sql), WithBuilderInput(reflect.ValueOf(input{ID: 10})),
				withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"ID": "ID"}),
			}
			if test.relation != nil {
				options = append(options, WithBuilderRelation(test.relation))
			}
			if test.columns != nil {
				options = append(options, WithBuilderCompositeArgs(test.columns, test.rows))
			} else if test.positional != nil {
				options = append(options, WithBuilderPositionalArgs(test.positional))
			}
			query, err := NewBuilder().Build(context.Background(), options...)
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			if query.SQL != test.wantSQL || !reflect.DeepEqual(query.Args, test.wantArgs) {
				t.Fatalf("Build() = SQL %q args %#v, want SQL %q args %#v", query.SQL, query.Args, test.wantSQL, test.wantArgs)
			}
		})
	}
}

func TestBuilderCacheSQLStripsRelationFiltersAndPreservesMatcher(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx,
		`CREATE TABLE accounts (id INTEGER, user_id INTEGER, name TEXT)`,
		`INSERT INTO accounts VALUES (1, 7, 'a1'), (2, 7, 'a2'), (3, 9, 'b3')`,
		`CREATE TABLE signals (feature_type TEXT, value TEXT, metric INTEGER)`,
		`INSERT INTO signals VALUES ('country', 'PL', 10), ('country', 'US', 20), ('country', 'DE', 30)`,
	); err != nil {
		t.Fatalf("setup: %v", err)
	}
	scalar := &data.Relation{Of: &data.RelationRef{On: data.Links{{Column: "user_id"}}}}
	composite := &data.Relation{Of: &data.RelationRef{On: data.Links{{Column: "feature_type"}, {Column: "value"}}}}
	tests := []struct {
		name       string
		sql        string
		relation   *data.Relation
		positional []any
		columns    []string
		rows       [][]interface{}
		wantSQL    string
		wantRows   int
		verify     func(*testing.T, *data.Relation, any)
	}{
		{
			name: "scalar criteria", sql: "SELECT id, user_id, name FROM accounts WHERE name LIKE 'a%' $AND_CRITERIA ORDER BY id",
			relation: scalar, positional: []any{7, 8},
			wantSQL: "SELECT id, user_id, name FROM accounts WHERE name LIKE 'a%' ORDER BY id", wantRows: 2,
		},
		{
			name: "composite column", sql: "SELECT feature_type, value, metric FROM signals WHERE $COLUMN_IN ORDER BY value",
			relation: composite, columns: []string{"feature_type", "value"}, rows: [][]interface{}{{"country", "PL"}, {"country", "US"}},
			wantSQL: "SELECT feature_type, value, metric FROM signals ORDER BY value", wantRows: 3,
		},
		{
			name: "composite and", sql: "SELECT feature_type, value, metric FROM signals WHERE metric > 0 $AND_COLUMN_IN ORDER BY value",
			relation: composite, columns: []string{"feature_type", "value"}, rows: [][]interface{}{{"country", "PL"}, {"country", "US"}},
			wantSQL: "SELECT feature_type, value, metric FROM signals WHERE metric > 0 ORDER BY value", wantRows: 3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := []BuilderOption{
				WithBuilderSQL(test.sql), WithBuilderInput(reflect.ValueOf(struct{}{})), WithBuilderRelation(test.relation),
			}
			if test.columns != nil {
				options = append(options, WithBuilderCompositeArgs(test.columns, test.rows))
			} else {
				options = append(options, WithBuilderPositionalArgs(test.positional))
			}
			query, err := NewBuilder().CacheSQL(ctx, options...)
			if err != nil {
				t.Fatalf("CacheSQL() error = %v", err)
			}
			if query.SQL != test.wantSQL || len(query.Args) != 0 {
				t.Fatalf("CacheSQL() = SQL %q args %#v, want SQL %q", query.SQL, query.Args, test.wantSQL)
			}
			if test.columns == nil {
				if query.By != "user_id" || !reflect.DeepEqual(query.In, []interface{}{7, 8}) {
					t.Fatalf("unexpected scalar matcher: by=%q in=%#v", query.By, query.In)
				}
			} else if !reflect.DeepEqual(query.ByColumns, test.columns) || !reflect.DeepEqual(query.InTuples, test.rows) {
				t.Fatalf("unexpected composite matcher: columns=%#v rows=%#v", query.ByColumns, query.InTuples)
			}
			rows, err := harness.DB.QueryContext(ctx, query.SQL, query.Args...)
			if err != nil {
				t.Fatalf("query cache SQL: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Close(); err != nil {
				t.Fatalf("close rows: %v", err)
			}
			if count != test.wantRows {
				t.Fatalf("cache SQL rows = %d, want %d", count, test.wantRows)
			}
		})
	}
}
