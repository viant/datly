package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type preparedFinalizer struct {
	calls                   int
	observed, errorToReturn error
}

func (o *preparedFinalizer) Finalize(_ context.Context, err error) error {
	o.calls++
	o.observed = err
	return o.errorToReturn
}

func TestErrorFinalizerObservesTransactionalPreparationSQLite(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		id                               int
		handlerFailure, finalizerFailure bool
		wantCount                        int
	}{
		{"success", 1, false, false, 1}, {"prepare failure", -1, false, false, 0}, {"finalizer veto", 1, false, true, 0}, {"handler failure", 1, true, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER CHECK(id>0))"); err != nil {
				t.Fatal(err)
			}
			expected := errors.New(tc.name)
			output := &preparedFinalizer{}
			if tc.finalizerFailure {
				output.errorToReturn = expected
			}
			actual, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), DataSource: sqldml.Source{DB: h.DB}, Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				value, _, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
				if err != nil {
					return nil, err
				}
				if err := value.(xhandler.DML).Execute("INSERT INTO audit VALUES(?)", tc.id); err != nil {
					return nil, err
				}
				if tc.handlerFailure {
					return output, expected
				}
				return output, nil
			})})
			if output.calls != 1 {
				t.Fatalf("finalizer calls=%d", output.calls)
			}
			if (output.observed != nil) != (tc.id < 0 || tc.handlerFailure) {
				t.Fatalf("observed=%v", output.observed)
			}
			if (err != nil) != (tc.id < 0 || tc.handlerFailure || tc.finalizerFailure) {
				t.Fatalf("error=%v", err)
			}
			if tc.id < 0 && actual != nil {
				t.Fatal("prepare failure retained output")
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct {
				Total int `sqlx:"total"`
			}{{tc.wantCount}})
		})
	}
}
