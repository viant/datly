package builder

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/io/read/cache"
)

func TestBuilder_PartitionInputSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE events (id INTEGER, tenant_id INTEGER, shard INTEGER);`,
		`CREATE TABLE events_2026 (id INTEGER, tenant_id INTEGER, shard INTEGER);`,
		`INSERT INTO events(id, tenant_id, shard) VALUES (1, 9, 2);`,
		`INSERT INTO events_2026(id, tenant_id, shard) VALUES (2, 9, 1), (3, 9, 2), (4, 8, 2);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{ Tenant int }
	source := &spec.ViewSource{
		Table: "events",
		SQL:   "SELECT id, tenant_id, shard FROM events WHERE tenant_id = :Tenant ORDER BY id",
	}
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderSource(source),
		WithBuilderInput(reflect.ValueOf(input{Tenant: 9})),
		withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"Tenant": "Tenant"}),
		WithBuilderPartition(&PartitionInput{
			Table:      "events_2026",
			Expression: "shard = ?",
			Args:       []any{2},
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if !reflect.DeepEqual(query.Args, []interface{}{9, 2}) {
		t.Fatalf("unexpected args: %#v", query.Args)
	}
	rows, err := h.DB.QueryContext(context.Background(), query.SQL, query.Args...)
	if err != nil {
		t.Fatalf("query failed: %v; SQL=%s", err, query.SQL)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected partition row")
	}
	var id, tenantID, shard int
	if err := rows.Scan(&id, &tenantID, &shard); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if id != 3 || tenantID != 9 || shard != 2 {
		t.Fatalf("unexpected partition row: %d %d %d", id, tenantID, shard)
	}
	if rows.Next() {
		t.Fatalf("expected exactly one partition row")
	}
}

func TestBuilder_PartitionArgumentOrderBeforeHaving(t *testing.T) {
	type input struct {
		Kind string
		Min  int
	}
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderSource(&spec.ViewSource{
			Table: "events",
			SQL:   "SELECT tenant_id, COUNT(*) AS total FROM events WHERE kind = :Kind GROUP BY tenant_id HAVING COUNT(*) > :Min",
		}),
		WithBuilderInput(reflect.ValueOf(input{Kind: "click", Min: 3})),
		withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"Kind": "Kind", "Min": "Min"}),
		WithBuilderPartition(&PartitionInput{Expression: "shard = ?", Args: []any{7}}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if !strings.Contains(query.SQL, "WHERE kind = ? AND (shard = ?) GROUP BY") {
		t.Fatalf("unexpected partition SQL: %s", query.SQL)
	}
	if !reflect.DeepEqual(query.Args, []interface{}{"click", 7, 3}) {
		t.Fatalf("expected partition argument before HAVING argument, got %#v", query.Args)
	}
}

func TestBuilder_ShapeBoundAppliesPartition(t *testing.T) {
	query, err := NewBuilder().ShapeBound(
		&cache.ParmetrizedQuery{SQL: "SELECT COUNT(*) AS count FROM events WHERE tenant_id = ?", Args: []interface{}{9}},
		WithBuilderView(&data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS count FROM events"}}}),
		WithBuilderPartition(&PartitionInput{Expression: "shard = ?", Args: []any{2}}),
	)
	if err != nil {
		t.Fatalf("shape bound failed: %v", err)
	}
	if !strings.Contains(query.SQL, "WHERE tenant_id = ? AND (shard = ?)") {
		t.Fatalf("unexpected partition SQL: %s", query.SQL)
	}
	if !reflect.DeepEqual(query.Args, []interface{}{9, 2}) {
		t.Fatalf("unexpected partition arguments: %#v", query.Args)
	}
}

func TestBuilder_PartitionInputValidation(t *testing.T) {
	tests := []struct {
		name      string
		source    *spec.ViewSource
		partition *PartitionInput
		wantErr   string
	}{
		{
			name:      "table requires metadata",
			source:    &spec.ViewSource{SQL: "SELECT id FROM events"},
			partition: &PartitionInput{Table: "events_2026"},
			wantErr:   "requires source table metadata",
		},
		{
			name:      "argument count",
			source:    &spec.ViewSource{SQL: "SELECT id FROM events"},
			partition: &PartitionInput{Expression: "shard = ?", Args: []any{1, 2}},
			wantErr:   "1 placeholders but 2 arguments",
		},
		{
			name:      "named placeholder rejected",
			source:    &spec.ViewSource{SQL: "SELECT id FROM events"},
			partition: &PartitionInput{Expression: "shard = :Shard", Args: []any{1}},
			wantErr:   "must use positional placeholders",
		},
		{
			name:      "arguments require expression",
			source:    &spec.ViewSource{SQL: "SELECT id FROM events"},
			partition: &PartitionInput{Args: []any{1}},
			wantErr:   "arguments require an expression",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewBuilder().Build(context.Background(),
				WithBuilderSource(test.source),
				WithBuilderInput(reflect.ValueOf(struct{}{})),
				WithBuilderPartition(test.partition),
			)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("expected error containing %q, got %v", test.wantErr, err)
			}
		})
	}
}
