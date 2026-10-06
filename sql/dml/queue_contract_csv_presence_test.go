package dml

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/structology"
	"github.com/viant/xunsafe"
)

type queueCSVState struct{ calls, target *int }

func (s queueCSVState) change() { *s.calls++; *s.target = 99 }

type queueCSVStringer struct{ queueCSVState }

func (v queueCSVStringer) String() string { v.change(); return "stringer" }

type queueCSVError struct{ queueCSVState }

func (v queueCSVError) Error() string { v.change(); return "error" }

type queueCSVFormatter struct{ queueCSVState }

func (v queueCSVFormatter) Format(s fmt.State, _ rune) { v.change(); fmt.Fprint(s, "formatter") }

type queueCSVPointerStringer struct{ queueCSVState }

func (v *queueCSVPointerStringer) String() string { v.change(); return "pointer stringer" }

type queueCSVRow struct {
	ID      int    `sqlx:"id,primaryKey"`
	Name    string `sqlx:"name"`
	Payload []any  `sqlx:"payload,enc=CSV"`
	Note    *int   `sqlx:"note"`
}

func queueCSVCallback(mode string, calls, target *int) any {
	s := queueCSVState{calls: calls, target: target}
	switch mode {
	case "Stringer":
		return queueCSVStringer{s}
	case "error":
		return queueCSVError{s}
	case "Formatter":
		return queueCSVFormatter{s}
	case "pointer-Stringer":
		return &queueCSVPointerStringer{s}
	}
	panic("unknown fixture callback")
}
func TestQueueContractCSVCallbacksRejectTerminalOperation(t *testing.T) {
	for _, mode := range []string{"Stringer", "error", "Formatter", "pointer-Stringer"} {
		for _, contract := range []rhandler.QueueContract{rhandler.SourceRow, rhandler.SourceSlice} {
			t.Run(fmt.Sprint(mode, "/", contract), func(t *testing.T) {
				d, _, tx := queueFixture(t, true)
				if _, err := tx.Exec("ALTER TABLE queue_items ADD COLUMN payload TEXT"); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec("INSERT INTO queue_items(id,name,note) VALUES(99,'caller-prior',7)"); err != nil {
					t.Fatal(err)
				}
				note, calls := 7, 0
				if err := d.InsertWithQueueContract("queue_items", &queueTestRow{ID: 1, Name: "queued-prior", Note: &note}, rhandler.SourceRow); err != nil {
					t.Fatal(err)
				}
				row := &queueCSVRow{ID: 2, Name: "terminal", Payload: []any{queueCSVCallback(mode, &calls, &note)}, Note: &note}
				var data any = row
				if contract == rhandler.SourceSlice {
					data = []*queueCSVRow{row}
				}
				// Rejection must precede callbacks and append. A former admitted terminal
				// operation can reach the native CSV formatter after the last step check.
				err := d.InsertWithQueueContract("queue_items", data, contract)
				if err == nil {
					executionErr := d.Complete(context.Background(), nil)
					var stored int
					queryErr := tx.QueryRow("SELECT note FROM queue_items WHERE id=2").Scan(&stored)
					t.Fatalf("CSV callback was admitted; calls=%d shared=%d stored=%d query=%v execution=%v", calls, note, stored, queryErr, executionErr)
				}
				if calls != 0 || note != 7 || len(d.queue) != 1 {
					t.Fatal("callback rejection was not atomic", calls, note, len(d.queue))
				}
				queuePrefix(t, tx, 1)
				var infrastructure int
				if err = tx.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'sqlx_sequence%'").Scan(&infrastructure); err != nil || infrastructure != 0 {
					t.Fatal("rejection invoked allocation", infrastructure, err)
				}
				if err = d.Complete(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
				queuePrefix(t, tx, 2)
				var stored int
				if err = tx.QueryRow("SELECT note FROM queue_items WHERE id=1").Scan(&stored); err != nil || stored != 7 {
					t.Fatal("prior referent changed", stored, err)
				}
			})
		}
	}
}
func TestQueueContractOrdinaryCSVFormattingCallbacksRemainNative(t *testing.T) {
	for _, mode := range []string{"Stringer", "error", "Formatter", "pointer-Stringer"} {
		t.Run(mode, func(t *testing.T) {
			d, _, tx := queueFixture(t, true)
			if _, err := tx.Exec("ALTER TABLE queue_items ADD COLUMN payload TEXT"); err != nil {
				t.Fatal(err)
			}
			note, calls := 7, 0
			row := &queueCSVRow{ID: 1, Name: "ordinary", Payload: []any{queueCSVCallback(mode, &calls, &note)}, Note: &note}
			if err := d.Insert("queue_items", row); err != nil {
				t.Fatal(err)
			}
			if err := d.Complete(context.Background(), nil); err != nil {
				t.Fatal(err)
			}
			queuePrefix(t, tx, 1)
			var stored int
			if err := tx.QueryRow("SELECT note FROM queue_items").Scan(&stored); err != nil || stored != 99 || note != 99 || calls != 1 {
				t.Fatal("native CSV callback not reached after binding", stored, note, calls, err)
			}
		})
	}
}

type queueRawStringer int

func (v queueRawStringer) String() string { return "not used by raw SQL binding" }

type queueRawStringerRow struct {
	ID   int              `sqlx:"id,primaryKey"`
	Name string           `sqlx:"name"`
	Note queueRawStringer `sqlx:"note"`
}

func TestQueueContractRawFormattingMethodsDoNotImplyCSV(t *testing.T) {
	d, _, tx := queueFixture(t, true)
	if err := d.InsertWithQueueContract("queue_items", &queueRawStringerRow{ID: 1, Name: "raw", Note: 7}, rhandler.SourceRow); err != nil {
		t.Fatal(err)
	}
	if err := d.Complete(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	var note int
	if err := tx.QueryRow("SELECT note FROM queue_items").Scan(&note); err != nil || note != 7 {
		t.Fatal(note, err)
	}
}

func queueNativePresenceRow(tag string) (reflect.Value, *queuePresentHas) {
	typ := reflect.StructOf([]reflect.StructField{
		{Name: "ID", Type: reflect.TypeFor[int](), Tag: `sqlx:"id,primaryKey"`},
		{Name: "Name", Type: reflect.TypeFor[string](), Tag: `sqlx:"name"`},
		{Name: "Has", Type: reflect.TypeFor[*queuePresentHas](), Tag: reflect.StructTag(tag)},
	})
	row := reflect.New(typ)
	row.Elem().FieldByName("Name").SetString("explicit-zero")
	has := &queuePresentHas{ID: true, Name: true}
	row.Elem().FieldByName("Has").Set(reflect.ValueOf(has))
	return row, has
}
func TestQueueContractEveryNativePresenceSpelling(t *testing.T) {
	spellings := []struct{ name, tag string }{
		{"setMarker-true", `sqlx:"-" setMarker:"true"`},
		{"setMarker-empty", `sqlx:"-" setMarker:""`},
		{"presenceIndex", `sqlx:"-" presenceIndex:""`},
		{"presence-true", `sqlx:"-,presence=true"`},
	}
	for _, spelling := range spellings {
		for _, contract := range []rhandler.QueueContract{rhandler.SourceRow, rhandler.SourceSlice} {
			for _, mutation := range []bool{false, true} {
				t.Run(fmt.Sprint(spelling.name, "/", contract, "/mutation=", mutation), func(t *testing.T) {
					d, _, tx := queueFixture(t, true)
					if _, err := tx.Exec("INSERT INTO queue_items VALUES(99,'caller-prior',7)"); err != nil {
						t.Fatal(err)
					}
					row, has := queueNativePresenceRow(spelling.tag)
					field, _ := row.Type().Elem().FieldByName("Has")
					if !structology.IsSetMarker(field.Tag) {
						t.Fatal("fixture is not a native presence spelling")
					}
					// Prove real SQLX marker selection recognizes explicit zero before admission.
					marker := &option.SetMarker{}
					columns, _, err := sqlio.StructColumnMapper(row.Type(), marker)
					if err != nil {
						t.Fatal(err)
					}
					identity := -1
					for i, c := range columns {
						if sqlio.IsIdentityColumn(c) {
							identity = i
						}
					}
					if marker.Marker == nil || identity < 0 || !marker.IsSet(xunsafe.AsPointer(row.Interface()), identity) {
						t.Fatal("SQLX did not recognize the fixture's explicit identity")
					}
					if err = d.InsertWithQueueContract("queue_items", &queueTestRow{ID: 1, Name: "queued-prior"}, rhandler.SourceRow); err != nil {
						t.Fatal(err)
					}
					data := row.Interface()
					if contract == rhandler.SourceSlice {
						slice := reflect.MakeSlice(reflect.SliceOf(row.Type()), 1, 1)
						slice.Index(0).Set(row)
						data = slice.Interface()
					}
					if err = d.InsertWithQueueContract("queue_items", data, contract); err != nil {
						t.Fatal(err)
					}
					if mutation {
						has.ID = false
					}
					err = d.Complete(context.Background(), nil)
					if mutation {
						if err == nil {
							t.Fatal("native selector mutation escaped guard")
						}
						queuePrefix(t, tx, 1)
					} else {
						if err != nil {
							t.Fatal(err)
						}
						queuePrefix(t, tx, 3)
						var id int
						if err = tx.QueryRow("SELECT id FROM queue_items WHERE name='explicit-zero'").Scan(&id); err != nil || id != 0 || row.Elem().FieldByName("ID").Int() != 0 {
							t.Fatal("explicit zero identity detached/allocated", id, err)
						}
					}
					var infrastructure int
					if err = tx.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'sqlx_sequence%'").Scan(&infrastructure); err != nil || infrastructure != 0 {
						t.Fatal("guard/admission allocated", infrastructure, err)
					}
					if _, err = tx.Exec("INSERT INTO queue_items VALUES(100,'caller-usable',7)"); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}
