package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/mysql/merge/metric"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/moption"
	"github.com/viant/sqlx/option"
	xexec "github.com/viant/xdatly/exec"
)

// The resolver is a transaction probe, not a replacement merge algorithm.
// These tests cover journal ownership; MySQL algorithm parity needs integration coverage.
type mergeTransactionProbe struct {
	exec func(context.Context, any, *sql.DB, string, ...moption.Option) (info.MergeResult, error)
}

func (*mergeTransactionProbe) DummyMergerConfigFn() {}
func (p *mergeTransactionProbe) Exec(ctx context.Context, rows any, db *sql.DB, table string, options ...moption.Option) (info.MergeResult, error) {
	return p.exec(ctx, rows, db, table, options...)
}
func init() {
	registry.RegisterMergeExecutorResolver(func(_ *info.Dialect, config info.MergeConfig) (io.MergeExecutor, error) {
		probe, ok := config.(*mergeTransactionProbe)
		if !ok {
			return nil, fmt.Errorf("transaction probe requires its own test config")
		}
		return probe, nil
	}, "SQLite")
}

func TestBufferedMergePreservesSQLXResultAndTransactionOwner(t *testing.T) {
	for _, mode := range []string{"commit", "late failure", "merge failure", "caller commit", "caller rollback", "failure before drain", "cancel before drain"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			ec := xexec.New()
			ctx := xexec.WithContext(context.Background(), ec)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			var supplied *sql.Tx
			var options []Option
			if mode == "caller commit" || mode == "caller rollback" {
				var err error
				supplied, err = h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer supplied.Rollback()
				options = append(options, WithTx(supplied))
			}
			data := NewData(h.DB, options...)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			cause := errors.New("late failure")
			calls := 0
			rows := &[]int{7}
			raw := &metric.Metric{Table: "audit", ToInsertCnt: 1, TotalTime: 3 * time.Millisecond}
			raw.Total.Report = []string{"original SQLX report"}
			cfg := &mergeTransactionProbe{exec: func(call context.Context, got any, db *sql.DB, table string, opts ...moption.Option) (info.MergeResult, error) {
				calls++
				if got != rows || db != h.DB || table != "audit" {
					t.Fatal("merge input replaced")
				}
				settings := moption.NewOptions(opts...)
				tx := settings.GetTransaction()
				if tx == nil || (supplied != nil && tx != supplied) {
					t.Fatal("merge did not receive owner's transaction")
				}
				if len(settings.GetCommonOptions()) != 1 {
					t.Fatal("original merge options dropped")
				}
				if _, err := tx.ExecContext(call, "INSERT INTO audit VALUES(7)"); err != nil {
					return nil, err
				}
				if mode == "merge failure" {
					raw.Err = cause
					return raw, cause
				}
				return raw, nil
			}}
			var result info.MergeResult
			if err := data.Merge("audit", rows, cfg, &result, moption.WithCommonOptions(option.Options{option.BatchSize(8000)})); err != nil {
				t.Fatal(err)
			}
			if calls != 0 || result != nil || len(ec.Metrics) != 0 {
				t.Fatal("queued merge executed eagerly")
			}
			if mode == "failure before drain" {
				if !errors.Is(data.Complete(ctx, cause), cause) {
					t.Fatal("cause lost")
				}
			} else if mode == "cancel before drain" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				if !errors.Is(data.Complete(canceled, nil), context.Canceled) {
					t.Fatal("cancellation lost")
				}
			} else {
				err := data.Flush(ctx, "audit")
				if mode == "merge failure" {
					if !errors.Is(err, cause) {
						t.Fatalf("merge error=%v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if calls != 1 || result != raw {
					t.Fatal("original SQLX result identity lost")
				}
				if result.Report() != raw.Report() || result.MergingTime() != raw.MergingTime() {
					t.Fatal("merge report changed")
				}
				db, tx := data.InvocationTransaction()
				if db != h.DB || tx == nil {
					t.Fatal("framework transaction missing")
				}
				var pending int
				if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&pending); err != nil || pending != 1 {
					t.Fatalf("pending=%d err=%v", pending, err)
				}
				var endCause error
				if mode == "late failure" || mode == "merge failure" {
					endCause = cause
				}
				err = data.Complete(ctx, endCause)
				if endCause != nil && !errors.Is(err, cause) {
					t.Fatalf("completion=%v", err)
				}
				if endCause == nil && err != nil {
					t.Fatal(err)
				}
				if supplied != nil {
					if mode == "caller commit" {
						err = supplied.Commit()
					} else {
						err = supplied.Rollback()
					}
					if err != nil {
						t.Fatalf("SQLX or DML completed caller transaction: %v", err)
					}
				}
				if len(ec.Metrics) != 1 || ec.Metrics[0].Type != "MERGE" || ec.Metrics[0].View != "audit" {
					t.Fatalf("merge metric=%+v", ec.Metrics)
				}
			}
			var count int
			if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 0
			if mode == "commit" || mode == "caller commit" {
				want = 1
			}
			if count != want {
				t.Fatalf("persisted rows=%d want=%d", count, want)
			}
			if mode == "failure before drain" || mode == "cancel before drain" {
				if calls != 0 || result != nil || len(ec.Metrics) != 0 {
					t.Fatal("unexecuted merge produced a result")
				}
			}
		})
	}
}

func TestMergeRejectsForeignTransactionAndSealedFrame(t *testing.T) {
	h := sqlite.New(t)
	data := NewData(h.DB)
	var result info.MergeResult
	cfg := &mergeTransactionProbe{}
	tx, err := h.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = data.Merge("audit", nil, cfg, &result, moption.WithTransaction(tx)); err == nil {
		t.Fatal("foreign transaction accepted")
	}
	data.SealComponent()
	if err = data.Merge("audit", nil, cfg, &result); !errors.Is(err, ErrComponentSealed) {
		t.Fatalf("sealed merge=%v", err)
	}
	if data.MutationReport().Queued != 0 {
		t.Fatal("rejected merge entered journal")
	}
}

func TestMergeRejectsUncommittedPrefixAndRollsItBack(t *testing.T) {
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	calls := 0
	cfg := &mergeTransactionProbe{exec: func(context.Context, any, *sql.DB, string, ...moption.Option) (info.MergeResult, error) {
		calls++
		return &metric.Metric{}, nil
	}}
	if err := data.Execute("INSERT INTO audit VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	var result info.MergeResult
	if err := data.Merge("audit", nil, cfg, &result); err != nil {
		t.Fatal(err)
	}
	err := data.Complete(ctx, nil)
	if !errors.Is(err, ErrMergeUncommittedPrefix) || calls != 0 || result != nil {
		t.Fatalf("invisible prefix accepted: %v calls=%d result=%v", err, calls, result)
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count); err != nil || count != 0 {
		t.Fatalf("prefix did not roll back: count=%d err=%v", count, err)
	}
}
