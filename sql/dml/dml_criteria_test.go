package dml

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

func TestCriteriaMutationExecutionSQLite(t *testing.T) {
	type input struct{ delete, race, caller, staleToken, rollback bool }
	type expect struct {
		conflict bool
		title    string
		count    int
	}
	type useCase struct {
		desc   string
		input  input
		expect expect
	}
	for _, tc := range []useCase{
		{"matching update", input{}, expect{title: "next", count: 1}},
		{"changed after update queued", input{race: true}, expect{conflict: true, title: "other", count: 1}},
		{"matching delete", input{delete: true}, expect{}},
		{"changed after delete queued", input{delete: true, race: true}, expect{conflict: true, title: "other", count: 1}},
		{"criteria combines with stale IfMatch", input{staleToken: true}, expect{conflict: true, title: "keep", count: 1}},
		{"caller rollback update", input{caller: true}, expect{title: "keep", count: 1}},
		{"caller rollback delete", input{caller: true, delete: true}, expect{title: "keep", count: 1}},
		{"late conflict rolls back earlier update", input{rollback: true}, expect{conflict: true, title: "keep", count: 1}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(name TEXT PRIMARY KEY,title TEXT NOT NULL,etag INTEGER NOT NULL)", "INSERT INTO records VALUES('one','keep',1)"); err != nil {
				t.Fatal(err)
			}
			var opts []Option
			if tc.input.caller {
				tx, err := h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				opts = append(opts, WithTx(tx))
			}
			data := NewData(h.DB, opts...)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			criteria := &sqlx.Criteria{Expression: "records.title = ? AND records.etag <= ?", Placeholders: []any{"keep", 1}}
			var writeOptions []xhandler.Option
			if tc.input.staleToken {
				writeOptions = append(writeOptions, xhandler.WithIfMatch("etag", 0))
			}
			if tc.input.rollback {
				if err := data.Update("records", &casRecord{Name: "one", Title: "earlier", Etag: 1}); err != nil {
					t.Fatal(err)
				}
			}
			row := &casSparseRecord{Name: "one", Title: "next", Has: &casSparseRecordHas{Name: true, Title: true}}
			var err error
			if tc.input.delete {
				err = data.DeleteWithCriteria("records", row, criteria, writeOptions...)
			} else {
				err = data.UpdateWithCriteria("records", row, criteria, writeOptions...)
			}
			if err != nil {
				t.Fatal(err)
			}
			// Neither mutation of the fragment nor reuse of its argument slice changes a queued guard.
			criteria.Expression = "1=1"
			criteria.Placeholders[0] = "other"
			if tc.input.race {
				if _, err = h.DB.ExecContext(ctx, "UPDATE records SET title='other',etag=2"); err != nil {
					t.Fatal(err)
				}
			}
			err = data.Complete(ctx, nil)
			var conflict *xhandler.Conflict
			if tc.expect.conflict {
				if !errors.As(err, &conflict) {
					t.Fatalf("expected conflict, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if tc.input.caller {
				if data.TransactionOutcome().State != xhandler.TransactionCallerPending {
					t.Fatalf("caller ownership lost: %+v", data.TransactionOutcome())
				}
				if err = data.tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			}
			var count int
			if err = h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != tc.expect.count {
				t.Fatalf("count=%d error=%v", count, err)
			}
			if count > 0 {
				var title string
				if err = h.DB.QueryRowContext(ctx, "SELECT title FROM records").Scan(&title); err != nil || title != tc.expect.title {
					t.Fatalf("title=%q error=%v", title, err)
				}
			}
		})
	}
}
