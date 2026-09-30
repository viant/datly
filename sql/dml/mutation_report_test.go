package dml

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/mattn/go-sqlite3"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/sqlx"
	"testing"
)

func TestMutationReportPreservesExecutedCountsAndQueuedWork(t *testing.T) {
	type row struct {
		ID   int    `sqlx:"id,primaryKey"`
		Name string `sqlx:"name"`
	}
	type useCase struct {
		desc   string
		input  func(*Data) error
		expect int64
		failed bool
		queued int
	}
	for _, tc := range []useCase{
		{desc: "insert", input: func(d *Data) error { return d.Insert("items", &row{ID: 2, Name: "new"}) }, expect: 1, queued: 1},
		{desc: "ignored insert", input: func(d *Data) error { return d.Insert("items", &row{ID: 3, Name: "ignored"}) }, queued: 1},
		{desc: "guard miss", input: func(d *Data) error {
			return d.UpdateWithCriteria("items", &row{ID: 1, Name: "new"}, &sqlx.Criteria{Expression: "name = ?", Placeholders: []any{"stale"}})
		}, failed: true, queued: 1},
		{desc: "duplicate before unexecuted sibling", input: func(d *Data) error {
			if err := d.Insert("items", &row{ID: 1, Name: "duplicate"}); err != nil {
				return err
			}
			return d.Insert("items", &row{ID: 4, Name: "later"})
		}, failed: true, queued: 2},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			h := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO items VALUES(1,'old')", "CREATE TRIGGER ignored BEFORE INSERT ON items WHEN NEW.id=3 BEGIN SELECT RAISE(IGNORE); END"); err != nil {
				t.Fatal(err)
			}
			d := NewData(h.DB)
			if err := d.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if err := tc.input(d); err != nil {
				t.Fatal(err)
			}
			err := d.Complete(ctx, nil)
			if (err != nil) != tc.failed {
				t.Fatalf("completion=%v", err)
			}
			report := d.MutationReport()
			if report.Queued != tc.queued || len(report.Results) != 1 || report.Results[0].Affected != tc.expect || (report.Results[0].Error != nil) != tc.failed {
				t.Fatalf("report=%+v", report)
			}
			report.Results[0].Affected = 99
			if d.MutationReport().Results[0].Affected == 99 {
				t.Fatal("report aliases execution evidence")
			}
		})
	}
}

type recoverySQLState string

func (e recoverySQLState) Error() string    { return string(e) }
func (e recoverySQLState) SQLState() string { return string(e) }

func TestMutationContentionUsesDriverEvidence(t *testing.T) {
	for _, tc := range []struct {
		input  error
		expect bool
	}{
		{sqlite3.Error{Code: sqlite3.ErrBusy, ExtendedCode: sqlite3.ErrBusySnapshot}, true},
		{sqlite3.Error{Code: sqlite3.ErrLocked}, true},
		{sqlite3.Error{Code: sqlite3.ErrConstraint}, false},
		{&mysql.MySQLError{Number: 1213}, true},
		{&mysql.MySQLError{Number: 1205}, true},
		{&mysql.MySQLError{Number: 1452}, false},
		{recoverySQLState("40001"), true},
		{recoverySQLState("40P01"), true},
		{recoverySQLState("23503"), false},
		{errors.New("database is locked"), false},
	} {
		if got := mutationContention(fmt.Errorf("execution: %w", tc.input)); got != tc.expect {
			t.Fatalf("%T %v: contention=%v", tc.input, tc.input, got)
		}
	}
}
