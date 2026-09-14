package dml

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	sqldialect "github.com/viant/sqlx/metadata/info/dialect"
	xexec "github.com/viant/xdatly/exec"
	xhandler "github.com/viant/xdatly/handler"
)

type metricRow struct {
	ID   int    `sqlx:"id,primaryKey"`
	Name string `sqlx:"name"`
}

func TestTypedDMLMetricsTransactionSQLite(t *testing.T) {
	for _, mode := range []string{"commit", "rollback", "caller-commit", "caller-rollback", "failure-before-execution", "native-error"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			ec := xexec.New()
			ctx := xexec.WithContext(context.Background(), ec)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			var options []Option
			var supplied *sql.Tx
			if mode == "caller-commit" || mode == "caller-rollback" {
				var err error
				supplied, err = h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer supplied.Rollback()
				options = append(options, WithTx(supplied))
			}
			d := NewData(h.DB, options...)
			if err := d.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if err := d.Insert("records", &metricRow{ID: 1, Name: "one"}); err != nil {
				t.Fatal(err)
			}
			if len(ec.Metrics) != 0 {
				t.Fatal("queued work produced metrics")
			}
			cause := errors.New("handler failure")
			if mode == "failure-before-execution" {
				if !errors.Is(d.Complete(ctx, cause), cause) {
					t.Fatal("failure lost")
				}
				if len(ec.Metrics) != 0 {
					t.Fatal("unexecuted work produced metrics")
				}
				return
			}
			start := time.Now()
			if err := d.Flush(ctx, ""); err != nil {
				t.Fatal(err)
			}
			if len(ec.Metrics) != 1 {
				t.Fatalf("executed metrics=%d", len(ec.Metrics))
			}
			if err := d.Flush(ctx, ""); err != nil {
				t.Fatal(err)
			}
			if len(ec.Metrics) != 1 {
				t.Fatal("repeated flush duplicated metrics")
			}
			first := *ec.Metrics[0]
			if first.Type != "INSERT" || first.View != "records" || first.Rows != 1 || first.Error != "" || first.ID != "" || len(first.Executions) != 0 || first.StartTime.Before(start) || first.EndTime.Before(first.StartTime) || first.Elapsed == "" {
				t.Fatalf("invalid native metric: %+v", first)
			}
			var completion error
			if mode == "rollback" {
				completion = cause
			}
			if mode == "native-error" {
				if err := d.Insert("records", &metricRow{ID: 1, Name: "two"}); err != nil {
					t.Fatal(err)
				}
				if err := d.Flush(ctx, ""); err == nil {
					t.Fatal("expected real duplicate key error")
				}
				if len(ec.Metrics) != 2 || ec.Metrics[1].Error == "" || ec.Metrics[1].Rows != 0 {
					t.Fatalf("native failure capture: %+v", ec.Metrics)
				}
			}
			err := d.Complete(ctx, completion)
			if (err != nil) != (mode == "rollback" || mode == "native-error") {
				t.Fatalf("completion: %v", err)
			}
			wantState := xhandler.TransactionCommitted
			wantRows := 1
			if mode == "rollback" || mode == "native-error" {
				wantState = xhandler.TransactionRolledBack
				wantRows = 0
			}
			if supplied != nil {
				wantState = xhandler.TransactionCallerPending
				var rows int
				if err := supplied.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&rows); err != nil || rows != 1 {
					t.Fatalf("pending rows=%d err=%v", rows, err)
				}
				if mode == "caller-rollback" {
					err = supplied.Rollback()
					wantRows = 0
				} else {
					err = supplied.Commit()
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if d.TransactionOutcome().State != wantState {
				t.Fatalf("outcome=%+v", d.TransactionOutcome())
			}
			if !reflect.DeepEqual(*ec.Metrics[0], first) {
				t.Fatal("transaction completion rewrote the execution record")
			}
			var count int
			if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != wantRows {
				t.Fatalf("rows=%d want=%d err=%v", count, wantRows, err)
			}
		})
	}
}

func TestTypedDMLMetricsUpdateDeleteAndSetupFailureSQLite(t *testing.T) {
	h := sqlite.New(t)
	ec := xexec.New()
	ctx := xexec.WithContext(context.Background(), ec)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'one')"); err != nil {
		t.Fatal(err)
	}
	d := NewData(h.DB)
	for _, item := range []struct {
		kind string
		run  func(string, any) error
		rows int
	}{{"UPDATE", d.Update, 1}, {"DELETE", d.Delete, 1}, {"DELETE", d.Delete, 0}} {
		if err := item.run("records", &metricRow{ID: 1, Name: "two"}); err != nil {
			t.Fatal(err)
		}
		if err := d.Flush(ctx, ""); err != nil {
			t.Fatal(err)
		}
		m := ec.Metrics[len(ec.Metrics)-1]
		if m.Type != item.kind || m.Rows != item.rows || m.Error != "" {
			t.Fatalf("metric=%+v", m)
		}
	}
	if len(ec.Metrics) != 3 {
		t.Fatal("wrong call count")
	}
	// Raw SQL, even when executed successfully, had no typed metric in original Datly.
	if err := d.Execute("INSERT INTO records VALUES(2,'two')"); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if len(ec.Metrics) != 3 {
		t.Fatal("invented raw SQL metric")
	}
	bad := NewData(nil)
	if err := bad.Insert("absent", &metricRow{ID: 2}); err != nil {
		t.Fatal(err)
	}
	if err := bad.Flush(ctx, ""); err == nil {
		t.Fatal("expected missing-database setup failure")
	}
	if len(ec.Metrics) != 3 {
		t.Fatal("service setup failure is not an executed DML call")
	}
}

func TestTypedDMLMetricsConcurrentFlushSQLite(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	ec := xexec.New()
	ctx = xexec.WithContext(ctx, ec)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	d := NewData(h.DB)
	if err := d.Insert("records", &metricRow{ID: 1, Name: "one"}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.Flush(ctx, ""); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(ec.Metrics) != 1 || ec.Metrics[0].Rows != 1 {
		t.Fatal("concurrent flush duplicated capture")
	}
}

func TestTypedInsertMetricGranularitySQLite(t *testing.T) {
	for _, mode := range []string{"slice", "separate-calls", "batch"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t)
			ec := xexec.New()
			ctx := xexec.WithContext(context.Background(), ec)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
				t.Fatal(err)
			}
			d := NewData(h.DB)
			if mode == "slice" {
				if err := d.Insert("records", []*metricRow{{ID: 1, Name: "one"}, {ID: 2, Name: "two"}}); err != nil {
					t.Fatal(err)
				}
			} else {
				a, b := &metricRow{Name: "one"}, &metricRow{Name: "two"}
				if mode == "batch" {
					dialect, err := d.dialectFor(ctx, h.DB)
					if err != nil {
						t.Fatal(err)
					}
					forced := *dialect
					forced.Insert = sqldialect.InsertWithMultiValues
					d.dialect = &forced
					a.ID = 1
					b.ID = 2
				}
				if err := d.Insert("records", a); err != nil {
					t.Fatal(err)
				}
				if err := d.Insert("records", b); err != nil {
					t.Fatal(err)
				}
			}
			if err := d.Flush(ctx, ""); err != nil {
				t.Fatal(err)
			}
			calls := 1
			if mode == "separate-calls" {
				calls = 2
			}
			if len(ec.Metrics) != calls {
				t.Fatalf("native call metrics=%d want=%d", len(ec.Metrics), calls)
			}
			rows := 0
			for _, metric := range ec.Metrics {
				rows += metric.Rows
				if metric.Type != "INSERT" || metric.Error != "" || len(metric.Executions) != 0 {
					t.Fatal("invalid insertion metric")
				}
			}
			if rows != 2 {
				t.Fatalf("affected rows=%d", rows)
			}
			if calls == 2 && ec.Metrics[1].StartTime.Before(ec.Metrics[0].EndTime) {
				t.Fatal("sequential native calls overlap")
			}
		})
	}
}

func TestTypedDMLExecutionSurvivesCommitFailureSQLite(t *testing.T) {
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	ec := xexec.New()
	ctx := xexec.WithContext(context.Background(), ec)
	if err := h.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE parent(id INTEGER PRIMARY KEY)", "CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"); err != nil {
		t.Fatal(err)
	}
	d := NewData(h.DB)
	if err := d.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := d.Insert("child", &struct {
		ID int `sqlx:"id"`
	}{ID: 99}); err != nil {
		t.Fatal(err)
	}
	if err := d.Complete(ctx, nil); err == nil {
		t.Fatal("expected deferred foreign-key commit failure")
	}
	if len(ec.Metrics) != 1 {
		t.Fatalf("metrics=%d", len(ec.Metrics))
	}
	metric := ec.Metrics[0]
	if metric.Type != "INSERT" || metric.Rows != 1 || metric.Error != "" || len(metric.Executions) != 0 {
		t.Fatalf("DML execution misreported: %+v", metric)
	}
	if outcome := d.TransactionOutcome(); outcome.State != xhandler.TransactionCommitUnknown || outcome.Error == nil {
		t.Fatalf("outcome=%+v", outcome)
	}
}
