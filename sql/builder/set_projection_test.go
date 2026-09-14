package builder

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestBuilder_BuildExecutesProjectedSetQuery_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE set_current (id INTEGER, name TEXT);`,
		`CREATE TABLE set_archived (user_id INTEGER, full_name TEXT);`,
		`INSERT INTO set_current(id, name) VALUES (1, 'alpha'), (2, 'bravo');`,
		`INSERT INTO set_archived(user_id, full_name) VALUES (3, 'charlie'), (4, 'delta');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	limit := 2
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderSQL(`SELECT id, name FROM set_current
UNION ALL
SELECT user_id AS id, full_name AS name FROM set_archived`),
		WithBuilderProjection([]string{"name"}),
		WithBuilderControls(&spec.ViewControls{OrderBy: "name DESC", Limit: &limit}),
		WithBuilderInput(reflect.ValueOf(struct{}{})),
	)
	if err != nil {
		t.Fatalf("build projected set query: %v", err)
	}
	rows, err := h.DB.QueryContext(context.Background(), query.SQL, query.Args...)
	if err != nil {
		t.Fatalf("execute projected set query %q: %v", query.SQL, err)
	}
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan projected set row: %v", err)
		}
		actual = append(actual, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate projected set rows: %v", err)
	}
	if !reflect.DeepEqual(actual, []string{"delta", "charlie"}) {
		t.Fatalf("unexpected projected set rows: %+v", actual)
	}
}
