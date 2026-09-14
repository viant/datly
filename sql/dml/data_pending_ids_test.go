package dml

import (
	"context"
	"fmt"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestPendingIDsShareSequencerAcrossDataComponents(t *testing.T) {
	for _, mode := range []string{"standalone", "owned", "caller transaction"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)", "INSERT INTO records VALUES(5,'stored')"); err != nil {
				t.Fatal(err)
			}
			var options []Option
			if mode == "caller transaction" {
				tx, err := h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				options = append(options, WithTx(tx))
			}
			data := NewData(h.DB, options...)
			if mode != "standalone" {
				if err := data.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				defer data.Complete(ctx, fmt.Errorf("test cleanup"))
			}
			child := data.ComponentData(ComponentImperative, "").(*Data)
			provided := &reservationRow{ID: 6, Name: "provided"}
			if err := child.Reserve(ctx, "records", provided, "ID"); err != nil {
				t.Fatal(err)
			}
			first, second := &reservationRow{Name: "first"}, &reservationRow{Name: "second"}
			if err := data.Allocate(ctx, "records", first, "ID"); err != nil {
				t.Fatal(err)
			}
			if err := child.Allocate(ctx, "records", second, "ID"); err != nil {
				t.Fatal(err)
			}
			if first.ID != 7 || second.ID != 8 || provided.ID != 6 {
				t.Fatalf("pending IDs=%d/%d/%d", provided.ID, first.ID, second.ID)
			}
			if len(data.queue) != 0 || len(child.queue) != 0 {
				t.Fatal("reservation queued a write")
			}
			if mode == "standalone" {
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS n FROM records"}, []struct{ N int }{{1}})
				return
			}
			for _, row := range []*reservationRow{provided, first, second} {
				if err := child.Insert("records", row); err != nil {
					t.Fatal(err)
				}
			}
			child.SealComponent()
			if err := data.Complete(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if first.ID != 7 || second.ID != 8 {
				t.Fatal("flush changed allocated identities")
			}
			if mode == "caller transaction" {
				if err := data.tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS n FROM records"}, []struct{ N int }{{1}})
			} else {
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id FROM records ORDER BY id"}, []struct{ ID int }{{5}, {6}, {7}, {8}})
			}
		})
	}
}
