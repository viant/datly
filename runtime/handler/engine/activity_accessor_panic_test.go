package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

type activityAccessorHandler struct {
	cause                          error
	executes, accessors, finalizes int
}

func (*activityAccessorHandler) RequiresPreBindingTransaction() bool { return true }
func (h *activityAccessorHandler) EarlyErrorOutputEnabled() bool     { h.accessors++; panic(h.cause) }
func (h *activityAccessorHandler) Execute(context.Context, rh.Invocation) (any, error) {
	h.executes++
	return nil, nil
}
func (h *activityAccessorHandler) FinalizeOutcome(context.Context, rh.Invocation, any, xh.Outcome) error {
	h.finalizes++
	return nil
}

type activityAccessorSource struct {
	source dml.Source
	native *dml.Data
}

func (s *activityAccessorSource) Open(ctx context.Context) (xh.Data, error) {
	data, err := s.source.Open(ctx)
	if err != nil {
		return nil, err
	}
	s.native = data.(*dml.Data)
	// Queuing succeeds; executing this statement would be a distinct SQL failure.
	if err = data.Execute("INSERT INTO missing_accessor_table VALUES(1)"); err != nil {
		return nil, err
	}
	return data, nil
}
func TestActivityAccessorPanicStillAbortsNativeOwnerWithoutDrain(t *testing.T) {
	for _, caller := range []bool{false, true} {
		name := "owned"
		if caller {
			name = "caller"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			source := &activityAccessorSource{source: dml.Source{DB: db.DB}}
			if caller {
				tx, err := db.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(99,'prior')"); err != nil {
					t.Fatal(err)
				}
				source.source.Tx = tx
			}
			cause := errors.New("early output opt-in panic")
			h := &activityAccessorHandler{cause: cause}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), DataSource: source, Handler: h})
			var pe *dexec.PanicError
			if !errors.As(err, &pe) || pe.Cause() != cause || h.executes != 0 || h.accessors != 1 || h.finalizes != 1 {
				t.Fatalf("err=%v execute/access/final=%d/%d/%d", err, h.executes, h.accessors, h.finalizes)
			}
			want := xh.TransactionRolledBack
			if caller {
				want = xh.TransactionCallerPending
			}
			if source.native == nil || source.native.TransactionOutcome().State != want {
				t.Fatalf("native outcome=%+v want=%v", source.native.TransactionOutcome(), want)
			}
			if caller {
				var count int
				if err := source.source.Tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE id=99").Scan(&count); err != nil || count != 1 {
					t.Fatalf("caller prior work count=%d err=%v", count, err)
				}
				if _, err := source.source.Tx.ExecContext(ctx, "INSERT INTO records VALUES(100,'still usable')"); err != nil {
					t.Fatal(err)
				}
			} else {
				var count int
				if err := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 0 {
					t.Fatalf("physical count=%d err=%v", count, err)
				}
			}
		})
	}
}
