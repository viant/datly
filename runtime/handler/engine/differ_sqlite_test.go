package engine

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/testharness"
	rdiffer "github.com/viant/datly/runtime/differ"
	rhandler "github.com/viant/datly/runtime/handler"
	sqldml "github.com/viant/datly/sql/dml"
	xdiffer "github.com/viant/xdatly/differ"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

func TestHandlerTypedDifferenceAuditSQLite(t *testing.T) {
	type record struct{ Count int }
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "handler failure"}[fail], func(t *testing.T) {
			h := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY, count INTEGER)`, `INSERT INTO records VALUES(7,5)`, `CREATE TABLE audit(path TEXT, kind TEXT, previous INTEGER, current INTEGER)`); err != nil {
				t.Fatal(err)
			}
			expected := errors.New("business failure")
			_, err := New().Execute(ctx, Request{
				Input: testRouteInput(t, reflect.TypeFor[record]()), BoundInput: &record{Count: 0}, DataSource: sqldml.Source{DB: h.DB}, Capabilities: xhandler.Capabilities{Differ: rdiffer.New()},
				Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
					raw, found, err := inv.Binder.Lookup(ctx, xhandler.DifferKey)
					if err != nil || !found {
						return nil, errors.New("differ missing")
					}
					changes, err := raw.(xdiffer.Differ).Diff(ctx, &record{Count: 5}, inv.Input)
					if err != nil {
						return nil, err
					}
					rows := changes.ToChangeRecords(xdiffer.WithSource("records"), xdiffer.WithSourceID(7))
					if len(rows) != 1 || rows[0].To != 0 {
						return nil, errors.New("supplied zero was lost")
					}
					raw, found, err = inv.Binder.Lookup(ctx, xhandler.DMLKey)
					if err != nil || !found {
						return nil, errors.New("DML missing")
					}
					dml := raw.(xhandler.DML)
					if err := dml.Execute(`UPDATE records SET count=? WHERE id=?`, rows[0].To, rows[0].SourceID); err != nil {
						return nil, err
					}
					for _, row := range rows {
						if err := dml.Execute(`INSERT INTO audit VALUES(?,?,?,?)`, row.Path, row.Change, row.From, row.To); err != nil {
							return nil, err
						}
					}
					if fail {
						return nil, expected
					}
					return changes, nil
				}),
			})
			if fail {
				if !errors.Is(err, expected) {
					t.Fatalf("err=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			var count, audits int
			if err := h.DB.QueryRow(`SELECT count FROM records WHERE id=7`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if err := h.DB.QueryRow(`SELECT COUNT(*) FROM audit`).Scan(&audits); err != nil {
				t.Fatal(err)
			}
			if fail {
				if count != 5 || audits != 0 {
					t.Fatalf("failed handler persisted count=%d audits=%d", count, audits)
				}
			} else {
				if count != 0 || audits != 1 {
					t.Fatalf("count=%d audits=%d", count, audits)
				}
				var path, kind string
				var from, to int
				if err := h.DB.QueryRow(`SELECT path,kind,previous,current FROM audit`).Scan(&path, &kind, &from, &to); err != nil {
					t.Fatal(err)
				}
				if path != "Count" || kind != "update" || from != 5 || to != 0 {
					t.Fatalf("audit=%s %s %d %d", path, kind, from, to)
				}
			}
		})
	}
}
