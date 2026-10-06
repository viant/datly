package dml

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	sqlio "github.com/viant/sqlx/io"
)

// Both Note and ID shadow value-embedded names at different SQLX locations.
type queueShadowEmbedded struct {
	ID   *int `sqlx:"id,primaryKey"`
	Note *int `sqlx:"note"`
}
type queueShadowRow struct {
	queueShadowEmbedded
	ID   int    `sqlx:"other_id"`
	Name string `sqlx:"name"`
	Note *int   `sqlx:"other_note"`
}

func TestQueueContractCompiledShadowedLocations(t *testing.T) {
	for _, mode := range []string{"unchanged", "embedded-note", "embedded-identity", "outer-note"} {
		t.Run(mode, func(t *testing.T) {
			d, _, tx := queueFixture(t, true)
			if _, err := tx.Exec("ALTER TABLE queue_items ADD COLUMN other_id INTEGER"); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec("ALTER TABLE queue_items ADD COLUMN other_note INTEGER"); err != nil {
				t.Fatal(err)
			}
			id, note, outer := 1, 7, 11
			row := &queueShadowRow{queueShadowEmbedded: queueShadowEmbedded{ID: &id, Note: &note}, ID: 8, Name: "shadow", Note: &outer}
			// Check the witness against the same binder SQLX will consume.
			columns, binder, err := sqlio.StructColumnMapper(row)
			if err != nil {
				t.Fatal(err)
			}
			args := make([]any, len(columns))
			binder(row, args, 0, len(columns))
			for i, c := range columns {
				v := reflect.ValueOf(args[i])
				for v.Kind() == reflect.Pointer {
					v = v.Elem()
				}
				if c.Name() == "note" && v.Int() != 7 || c.Name() == "other_note" && v.Int() != 11 || c.Name() == "id" && v.Int() != 1 {
					t.Fatal("invalid native mapping witness", c.Name(), args[i])
				}
			}
			if err = d.InsertWithQueueContract("queue_items", row, rhandler.SourceRow); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "embedded-note":
				note = 9
			case "embedded-identity":
				id = 99
			case "outer-note":
				outer = 13
			}
			err = d.Complete(context.Background(), nil)
			if mode == "unchanged" {
				if err != nil {
					t.Fatal(err)
				}
				queuePrefix(t, tx, 1)
				var gotID, gotNote, gotOuter int
				if err = tx.QueryRow("SELECT id,note,other_note FROM queue_items").Scan(&gotID, &gotNote, &gotOuter); err != nil || gotID != 1 || gotNote != 7 || gotOuter != 11 {
					t.Fatal(gotID, gotNote, gotOuter, err)
				}
			} else {
				if err == nil {
					t.Fatal("mutated compiled field admitted")
				}
				queuePrefix(t, tx, 0)
			}
		})
	}
}

func TestQueueContractShadowedIdentityAdmission(t *testing.T) {
	d, _, tx := queueFixture(t, true)
	zero := 0
	note := 7
	row := &queueShadowRow{queueShadowEmbedded: queueShadowEmbedded{ID: &zero, Note: &note}, ID: 9, Name: "unallocated", Note: &note}
	for _, contract := range []rhandler.QueueContract{rhandler.SourceRow, rhandler.SourceSlice} {
		var value any = row
		if contract == rhandler.SourceSlice {
			value = []*queueShadowRow{row}
		}
		if err := d.InsertWithQueueContract("queue_items", value, contract); err == nil || len(d.queue) != 0 {
			t.Fatal("shadowed unresolved identity admitted", err)
		}
	}
	queuePrefix(t, tx, 0)
}

type queueCallbackRow struct {
	ID    int    `sqlx:"id,primaryKey"`
	Name  string `sqlx:"name"`
	Note  *int   `sqlx:"note"`
	calls *int   `sqlx:"-"`
	mode  string `sqlx:"-"`
}

func (r *queueCallbackRow) OnInsert(context.Context) error {
	*r.calls++
	if r.mode == "scalar" {
		r.Name = "changed-in-callback"
	} else {
		*r.Note = 99
	}
	return nil
}

type queueJSONCallback struct {
	calls  *int
	target *int
}

func (v *queueJSONCallback) MarshalJSON() ([]byte, error) {
	*v.calls++
	*v.target = 99
	return []byte(`"custom"`), nil
}

type queueJSONRow struct {
	ID   int                `sqlx:"id,primaryKey"`
	Name string             `sqlx:"name"`
	Note *queueJSONCallback `sqlx:"note,enc=JSON"`
}

func TestQueueContractUnsupportedExecutionCallbacksAtomic(t *testing.T) {
	for _, mode := range []string{"scalar", "other-referent", "json"} {
		t.Run(mode, func(t *testing.T) {
			for _, contract := range []rhandler.QueueContract{rhandler.SourceRow, rhandler.SourceSlice} {
				d, _, tx := queueFixture(t, true)
				note, calls := 7, 0
				// A prior admitted referent must survive rejection of the next operation.
				prior := &queueTestRow{ID: 1, Name: "prior", Note: &note}
				if err := d.InsertWithQueueContract("queue_items", prior, rhandler.SourceRow); err != nil {
					t.Fatal(err)
				}
				var value any
				if mode == "json" {
					row := &queueJSONRow{ID: 2, Name: "callback", Note: &queueJSONCallback{calls: &calls, target: &note}}
					value = row
					if contract == rhandler.SourceSlice {
						value = []*queueJSONRow{row}
					}
				} else {
					row := &queueCallbackRow{ID: 2, Name: "callback", Note: &note, calls: &calls, mode: mode}
					value = row
					if contract == rhandler.SourceSlice {
						value = []*queueCallbackRow{row}
					}
				}
				if err := d.InsertWithQueueContract("queue_items", value, contract); err == nil || len(d.queue) != 1 || calls != 0 || note != 7 {
					t.Fatal("unsupported callback invoked or appended", err, len(d.queue), calls, note)
				}
				queuePrefix(t, tx, 0)
				if err := d.Complete(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
				queuePrefix(t, tx, 1)
			}
		})
	}
}
func TestQueueContractOrdinaryCallbacksPreserved(t *testing.T) {
	for _, mode := range []string{"scalar", "other-referent", "json"} {
		t.Run(mode, func(t *testing.T) {
			d, _, tx := queueFixture(t, true)
			note, calls := 7, 0
			if mode == "json" {
				if err := d.Insert("queue_items", &queueJSONRow{ID: 1, Name: "ordinary", Note: &queueJSONCallback{calls: &calls, target: &note}}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := d.Insert("queue_items", &queueCallbackRow{ID: 1, Name: "ordinary", Note: &note, calls: &calls, mode: mode}); err != nil {
					t.Fatal(err)
				}
			}
			if err := d.Complete(context.Background(), nil); err != nil {
				t.Fatal(err)
			}
			queuePrefix(t, tx, 1)
			if calls != 1 {
				t.Fatal("ordinary callback changed", calls)
			}
			if mode == "scalar" {
				var name string
				if err := tx.QueryRow("SELECT name FROM queue_items").Scan(&name); err != nil || name != "changed-in-callback" {
					t.Fatal(name, err)
				}
			} else if note != 99 {
				t.Fatal("ordinary referent callback skipped", note)
			}
		})
	}
}

type queueConventionalID struct {
	ID   int
	Name string `sqlx:"name"`
}
type queuePrimaryID struct {
	Key  int    `sqlx:"id,primaryKey"`
	Name string `sqlx:"name"`
}
type queuePointerPrimaryID struct {
	Key  *int   `sqlx:"id,primaryKey"`
	Name string `sqlx:"name"`
}
type queueUnsignedID struct {
	ID   uint   `sqlx:"id,primaryKey"`
	Name string `sqlx:"name"`
}
type queueUnsignedPointerID struct {
	ID   *uint  `sqlx:"id,primaryKey"`
	Name string `sqlx:"name"`
}
type queuePresentHas struct{ ID, Name bool }
type queuePresentID struct {
	ID   int              `sqlx:"id,primaryKey"`
	Name string           `sqlx:"name"`
	Has  *queuePresentHas `sqlx:"-" setMarker:"true"`
}
type queuePresentPointerID struct {
	ID   *int             `sqlx:"id,primaryKey"`
	Name string           `sqlx:"name"`
	Has  *queuePresentHas `sqlx:"-" setMarker:"true"`
}

func TestQueueContractNativeIdentityPresenceAdmission(t *testing.T) {
	zero, one := 0, 1
	zeroUint, oneUint := uint(0), uint(1)
	cases := []struct {
		name  string
		row   any
		admit bool
	}{
		{"unsigned-zero", &queueUnsignedID{Name: "row"}, false},
		{"unsigned-pointer-zero", &queueUnsignedPointerID{ID: &zeroUint, Name: "row"}, false},
		{"unsigned-pointer-allocated", &queueUnsignedPointerID{ID: &oneUint, Name: "row"}, true},
		{"conventional-zero", &queueConventionalID{Name: "row"}, false},
		{"primary-zero", &queuePrimaryID{Name: "row"}, false},
		{"pointer-nil", &queuePointerPrimaryID{Name: "row"}, false},
		{"pointer-zero", &queuePointerPrimaryID{Key: &zero, Name: "row"}, false},
		{"pointer-allocated", &queuePointerPrimaryID{Key: &one, Name: "row"}, true},
		{"explicit-zero", &queuePresentID{Name: "row", Has: &queuePresentHas{ID: true, Name: true}}, true},
		{"absent-zero", &queuePresentID{Name: "row", Has: &queuePresentHas{Name: true}}, false},
		{"explicit-pointer-zero", &queuePresentPointerID{ID: &zero, Name: "row", Has: &queuePresentHas{ID: true, Name: true}}, true},
		{"explicit-pointer-nil", &queuePresentPointerID{Name: "row", Has: &queuePresentHas{ID: true, Name: true}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, contract := range []rhandler.QueueContract{rhandler.SourceRow, rhandler.SourceSlice} {
				d, _, tx := queueFixture(t, true)
				value := tc.row
				if contract == rhandler.SourceSlice {
					slice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(value)), 1, 1)
					slice.Index(0).Set(reflect.ValueOf(value))
					value = slice.Interface()
				}
				err := d.InsertWithQueueContract("queue_items", value, contract)
				if (err == nil) != tc.admit {
					t.Fatal("native identity admission", err, tc.admit)
				}
				queuePrefix(t, tx, 0)
				if !tc.admit {
					if len(d.queue) != 0 {
						t.Fatal("rejected identity appended")
					}
					var n int
					if err = tx.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'sqlx_sequence%'").Scan(&n); err != nil || n != 0 {
						t.Fatal("admission invoked allocator", n, err)
					}
					continue
				}
				if err = d.Complete(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
				queuePrefix(t, tx, 1)
				want := 0
				if tc.name == "pointer-allocated" || tc.name == "unsigned-pointer-allocated" {
					want = 1
				}
				var id int
				if err = tx.QueryRow("SELECT id FROM queue_items").Scan(&id); err != nil || id != want {
					t.Fatal("explicit identity changed", id, want, err)
				}
			}
		})
	}
}

func BenchmarkQueueContractOrdinaryPlanning(b *testing.B) {
	for _, size := range []int{4096, 32768} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			operations := make([]*dataOperation, size)
			for i := range operations {
				operations[i] = &dataOperation{kind: dataOpInsert, table: "records", data: &queueTestRow{ID: i + 1}}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				plan := buildExecutionPlan(operations)
				if len(plan) != 1 || len(plan[0].operations) != size {
					b.Fatal("ordinary batching changed")
				}
			}
		})
	}
}

func TestQueueContractUnsupportedCallbackDiagnostic(t *testing.T) {
	d, _, _ := queueFixture(t, false)
	calls := 0
	note := 7
	if err := d.InsertWithQueueContract("queue_items", &queueCallbackRow{ID: 1, calls: &calls, Note: &note}, rhandler.SourceRow); err == nil || !strings.Contains(err.Error(), "OnInsert") {
		t.Fatal(err)
	}
}
