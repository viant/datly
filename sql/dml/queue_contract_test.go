package dml

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"reflect"
	"testing"
)

type queueTestHas struct{ Name bool }
type queueTestRow struct {
	ID       int           `sqlx:"id,primaryKey"`
	Name     string        `sqlx:"name"`
	Note     *int          `sqlx:"note"`
	Business string        `sqlx:"-"`
	Has      *queueTestHas `sqlx:"-" setMarker:"true"`
}

func queueFixture(t *testing.T, caller bool) (*Data, *sql.DB, *sql.Tx) {
	t.Helper()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), `CREATE TABLE queue_items(id INTEGER PRIMARY KEY,name TEXT UNIQUE,note INTEGER)`, `CREATE TABLE queue_effects(id INTEGER)`, `CREATE TRIGGER queue_added AFTER INSERT ON queue_items BEGIN INSERT INTO queue_effects VALUES(new.id); END`); err != nil {
		t.Fatal(err)
	}
	var tx *sql.Tx
	var opts []Option
	if caller {
		var err error
		tx, err = h.DB.Begin()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { tx.Rollback() })
		opts = append(opts, WithTx(tx))
	}
	d := NewData(h.DB, opts...)
	if err := d.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	return d, h.DB, tx
}

func queuePrefix(t *testing.T, tx *sql.Tx, want int) {
	t.Helper()
	for _, table := range []string{"queue_items", "queue_effects"} {
		var n int
		if err := tx.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil || n != want {
			t.Fatalf("%s prefix=%d want=%d err=%v", table, n, want, err)
		}
	}
}

func TestQueueContractStatementBarriersAndOrdinaryBatch(t *testing.T) {
	for _, mode := range []string{"ordinary", "marked-first", "marked-second", "marked-child-frames", "two-sided"} {
		t.Run(mode, func(t *testing.T) {
			d, db, tx := queueFixture(t, true)
			var names []string
			if mode == "two-sided" {
				names = []string{"prior", "dup", "dup"}
			} else {
				names = []string{"dup", "dup"}
			}
			for i, name := range names {
				frame := d
				if mode == "marked-child-frames" {
					frame = d.ComponentData(ComponentImperative, "").(*Data)
				}
				row := &queueTestRow{ID: i + 1, Name: name}
				marked := mode == "marked-first" && i == 0 || mode == "marked-second" && i == 1 || mode == "marked-child-frames" || mode == "two-sided" && i == 0
				var err error
				if marked {
					err = frame.InsertWithQueueContract("queue_items", row, rhandler.SourceRow)
				} else {
					err = frame.Insert("queue_items", row)
				}
				if err != nil {
					t.Fatal(err)
				}
				if frame != d {
					frame.SealComponent()
				}
			}
			if err := d.Complete(context.Background(), nil); err == nil {
				t.Fatal("real UNIQUE failure required")
			}
			want := 1
			if mode == "ordinary" {
				want = 0
			}
			queuePrefix(t, tx, want)
			if _, err := tx.Exec(`INSERT INTO queue_items VALUES(999,'caller-usable',NULL)`); err != nil {
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			var n int
			if err := db.QueryRow(`SELECT COUNT(*) FROM queue_items`).Scan(&n); err != nil || n != 0 {
				t.Fatal(n, err)
			}
		})
	}
}

func TestQueueContractShallowStorageAndGuard(t *testing.T) {
	for _, mode := range []string{"public-scalar", "ignored-scalar", "shared-pointee", "presence", "stored-scalar", "reference-retarget"} {
		t.Run(mode, func(t *testing.T) {
			d, db, _ := queueFixture(t, false)
			note := 7
			r := &queueTestRow{ID: 1, Name: "admitted", Note: &note, Has: &queueTestHas{Name: true}}
			if err := d.InsertWithQueueContract("queue_items", r, rhandler.SourceRow); err != nil {
				t.Fatal(err)
			}
			stored := d.queue[0].data.(*queueTestRow)
			if stored == r || stored.Note != r.Note || stored.Has != r.Has {
				t.Fatal("execution payload must remain a shallow distinct struct")
			}
			switch mode {
			case "public-scalar":
				r.Name = "later"
			case "ignored-scalar":
				r.Business = "later"
			case "shared-pointee":
				note = 9
			case "presence":
				r.Has.Name = false
			case "stored-scalar":
				stored.Name = "tampered"
			case "reference-retarget":
				other := 7
				stored.Note = &other
			}
			err := d.Complete(context.Background(), nil)
			valid := mode == "public-scalar" || mode == "ignored-scalar"
			if (err == nil) != valid {
				t.Fatalf("mode=%s err=%v", mode, err)
			}
			var n int
			if e := db.QueryRow(`SELECT COUNT(*) FROM queue_items`).Scan(&n); e != nil {
				t.Fatal(e)
			}
			if valid {
				var name string
				if e := db.QueryRow(`SELECT name FROM queue_items`).Scan(&name); e != nil || name != "admitted" {
					t.Fatal(name, e)
				}
			} else if n != 0 {
				t.Fatal("guard failure wrote SQL", n)
			}
		})
	}
}

func TestQueueContractSourceSliceAggregateBoundary(t *testing.T) {
	for _, size := range []int{2, 100, 101} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			d, _, tx := queueFixture(t, true)
			rows := make([]*queueTestRow, size)
			for i := range rows {
				rows[i] = &queueTestRow{ID: i + 1, Name: fmt.Sprint(i)}
			}
			rows[size-1].Name = rows[0].Name
			if err := d.InsertWithQueueContract("queue_items", rows, rhandler.SourceSlice); err != nil {
				t.Fatal(err)
			}
			if len(d.queue) != 1 {
				t.Fatal("slice must be one admission")
			}
			stored := d.queue[0].data.([]*queueTestRow)
			if &stored[0] == &rows[0] || stored[0] != rows[0] {
				t.Fatal("slice backing must copy, pointers must survive")
			}
			rows[0] = &queueTestRow{ID: 999, Name: "public-slot-replacement"}
			if err := d.Complete(context.Background(), nil); err == nil {
				t.Fatal("real duplicate failure required")
			}
			want := 0
			if size == 101 {
				want = 100
			}
			queuePrefix(t, tx, want)
		})
	}
}

func TestQueueContractInvalidAdmissionAtomicAndAliases(t *testing.T) {
	d, _, _ := queueFixture(t, false)
	for _, tc := range []struct {
		data     any
		contract rhandler.QueueContract
	}{{nil, rhandler.SourceRow}, {(*queueTestRow)(nil), rhandler.SourceRow}, {[]*queueTestRow{{ID: 1}, nil}, rhandler.SourceSlice}, {[]queueTestRow{{ID: 1}}, rhandler.SourceSlice}, {&queueTestRow{ID: 1}, 0}, {&queueTestRow{ID: 1}, 99}} {
		if err := d.InsertWithQueueContract("queue_items", tc.data, tc.contract); err == nil || len(d.queue) != 0 {
			t.Fatal("invalid admission must be atomic", err, len(d.queue))
		}
	}
	if err := d.InsertWithQueueContract("queue_items", []*queueTestRow{}, rhandler.SourceSlice); err != nil || len(d.queue) != 0 {
		t.Fatal("empty slice is a no-op", err)
	}
	r := &queueTestRow{ID: 1, Name: "alias"}
	if err := d.InsertWithQueueContract("queue_items", r, rhandler.SourceRow); err != nil {
		t.Fatal(err)
	}
	if err := d.InsertWithQueueContract("queue_items", r, rhandler.SourceRow); err != nil || len(d.queue) != 2 {
		t.Fatal("storage policy must not deduplicate/reject aliases", err)
	}
	if err := d.Complete(context.Background(), nil); err == nil {
		t.Fatal("both aliased operations must reach duplicate SQL")
	}
}

func TestQueueContractFuturePayloadCheckedAfterPriorSQL(t *testing.T) {
	d, _, tx := queueFixture(t, true)
	one := &queueTestRow{ID: 1, Name: "one"}
	note := 7
	two := &queueTestRow{ID: 2, Name: "two", Note: &note}
	for _, r := range []*queueTestRow{one, two} {
		if err := d.InsertWithQueueContract("queue_items", r, rhandler.SourceRow); err != nil {
			t.Fatal(err)
		}
	}
	checks := 0
	if err := d.RegisterExecutionGuard(func(context.Context) error {
		checks++
		if checks == 3 {
			note = 9
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.Complete(context.Background(), nil); err == nil {
		t.Fatal("later step must reject a changed shared SQL referent")
	}
	queuePrefix(t, tx, 1)
}

func TestQueueContractSliceTwoSidedBarrier(t *testing.T) {
	for _, mode := range []string{"aggregate-first", "aggregate-second"} {
		t.Run(mode, func(t *testing.T) {
			d, _, tx := queueFixture(t, true)
			singleton := &queueTestRow{ID: 1, Name: "singleton"}
			aggregate := []*queueTestRow{{ID: 2, Name: "dup"}, {ID: 3, Name: "dup"}}
			var err error
			if mode == "aggregate-first" {
				err = d.InsertWithQueueContract("queue_items", []*queueTestRow{singleton}, rhandler.SourceSlice)
				if err == nil {
					err = d.Insert("queue_items", aggregate[0])
				}
				if err == nil {
					err = d.Insert("queue_items", aggregate[1])
				}
			} else {
				err = d.Insert("queue_items", singleton)
				if err == nil {
					err = d.InsertWithQueueContract("queue_items", aggregate, rhandler.SourceSlice)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = d.Complete(context.Background(), nil); err == nil {
				t.Fatal("genuine duplicate failure required")
			}
			queuePrefix(t, tx, 1)
		})
	}
}
func TestQueueContractDeleteUsesAdmittedIdentityOnly(t *testing.T) {
	d, db, tx := queueFixture(t, true)
	if _, err := tx.Exec("INSERT INTO queue_items(id,name,note) VALUES(1,'existing',7)"); err != nil {
		t.Fatal(err)
	}
	note := 7
	r := &queueTestRow{ID: 1, Name: "existing", Note: &note}
	if err := d.DeleteWithQueueContract("queue_items", r, rhandler.SourceRow); err != nil {
		t.Fatal(err)
	}
	r.ID = 99
	note = 9 // public identity scalar and non-DELETE SQL value are detached/irrelevant.
	if err := d.Complete(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRow("SELECT COUNT(*) FROM queue_items").Scan(&n); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM queue_items").Scan(&n); err != nil || n != 0 {
		t.Fatal(n, err)
	}
}

type queueAutoPointer struct {
	ID   *int   `sqlx:"id,primaryKey=true,autoincrement=true"`
	Name string `sqlx:"name"`
}

func TestQueueContractRejectsUnallocatedPointerIdentity(t *testing.T) {
	d, _, _ := queueFixture(t, false)
	zero := 0
	for _, id := range []*int{nil, &zero} {
		if err := d.InsertWithQueueContract("queue_items", &queueAutoPointer{ID: id}, rhandler.SourceRow); err == nil || len(d.queue) != 0 {
			t.Fatal("unallocated native ID admitted", err)
		}
	}
}

type queueEmbedded struct {
	Name string `sqlx:"name"`
	Note *int   `sqlx:"note"`
}
type queueMappedRow struct {
	ID int `sqlx:"id,primaryKey"`
	*queueEmbedded
}

func TestQueueContractEmbeddedNativeMappedEvidence(t *testing.T) {
	d, _, _ := queueFixture(t, false)
	note := 7
	r := &queueMappedRow{ID: 1, queueEmbedded: &queueEmbedded{Name: "one", Note: &note}}
	if err := d.InsertWithQueueContract("queue_items", r, rhandler.SourceRow); err != nil {
		t.Fatal(err)
	}
	if err := d.queue[0].validatePayload(); err != nil {
		t.Fatal(err)
	}
	note = 9
	if err := d.queue[0].validatePayload(); err == nil {
		t.Fatal("native embedded mapped SQL changed without rejection")
	}
}

type queueOpaqueValuer struct{ calls *int }

func (v queueOpaqueValuer) Value() (driver.Value, error) { *v.calls++; return "opaque", nil }
func TestQueueContractPureEvidenceRejectsOpaqueValuerAndTracksMaps(t *testing.T) {
	calls := 0
	if _, err := captureQueueValue(reflect.ValueOf(queueOpaqueValuer{calls: &calls}), 0); err == nil || calls != 0 {
		t.Fatal("opaque driver callback was executed or silently omitted", err, calls)
	}
	value := map[string][]int{"key": {1, 2}}
	image, err := captureQueueValue(reflect.ValueOf(value), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !image.equal(reflect.ValueOf(value), 0) {
		t.Fatal("unchanged pure image did not match")
	}
	value["key"][1] = 3
	if image.equal(reflect.ValueOf(value), 0) {
		t.Fatal("shared mapped collection mutation admitted")
	}
}
