package writer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type qcHas struct{ ID, Name, Note, Remove bool }
type qcRow struct {
	ID     int    `sqlx:"id,primaryKey=true,autoincrement=true"`
	Name   string `sqlx:"name"`
	Note   *int   `sqlx:"note"`
	Remove bool   `sqlx:"-" writer:"delete"`
	Has    *qcHas `sqlx:"-" setMarker:"true"`
}
type qcInput struct {
	Rows        []*qcRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=queue_items"`
	CurrentRows []*qcRow `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=queue_items"`
}
type qcOutput struct {
	Data   []*qcRow `parameter:"Data,kind=output,in=body"`
	Status string
}
type qcHook struct {
	mode    string
	input   *qcInput
	visited []int
}

func (h *qcHook) AfterQueue(_ context.Context, r *qcRow, _ xhandler.LifecycleContext[qcRow, xhandler.NoParent, qcOutput]) error {
	h.visited = append(h.visited, r.ID)
	switch h.mode {
	case "public-scalar":
		r.Name = "later-public"
	case "pointee":
		*r.Note = 99
	case "presence":
		r.Has.Name = false
	case "slot":
		h.input.Rows[0] = &qcRow{ID: r.ID, Name: r.Name, Has: r.Has}
	case "length":
		h.input.Rows = append(h.input.Rows, &qcRow{Name: "added"})
	case "panic":
		panic("queue hook panic")
	case "error":
		return errors.New("fixture after-queue rejection")
	}
	return nil
}

type qcJournal struct {
	*sqldml.Data
	input *qcInput
	calls *[]string
}

func (d *qcJournal) InsertWithQueueContract(table string, row any, contract rhandler.QueueContract) error {
	for _, r := range d.input.Rows {
		if r.ID == 0 {
			return fmt.Errorf("whole group was not allocated before first append")
		}
	}
	*d.calls = append(*d.calls, "append")
	return d.Data.InsertWithQueueContract(table, row, contract)
}
func (d *qcJournal) Start(ctx context.Context) error {
	*d.calls = append(*d.calls, "start")
	return d.Data.Start(ctx)
}

type qcAllocator struct{ journal *qcJournal }

func (a *qcAllocator) Allocate(ctx context.Context, table string, rows any, path string) error {
	values := reflect.ValueOf(rows)
	if values.Kind() != reflect.Slice || values.Len() != len(a.journal.input.Rows) {
		return fmt.Errorf("expected entire group")
	}
	for i, r := range a.journal.input.Rows {
		if values.Index(i).Pointer() != reflect.ValueOf(r).Pointer() {
			return fmt.Errorf("allocator lost public row identity")
		}
	}
	*a.journal.calls = append(*a.journal.calls, fmt.Sprintf("allocate:%d", values.Len()))
	return a.journal.Data.Allocate(ctx, table, rows, path)
}

type qcBinder struct {
	*sqBinder
	journal *qcJournal
	mode    string
}

func (b *qcBinder) Bind(_ context.Context, value any) error {
	if h, ok := value.(*qcHook); ok {
		h.mode = b.mode
		h.input = b.journal.input
	}
	return nil
}
func (b *qcBinder) Lookup(ctx context.Context, key xhandler.ValueKey) (any, bool, error) {
	switch key {
	case xhandler.DMLKey, xhandler.TransactionStarterKey:
		return b.journal, true, nil
	case xhandler.SequencerKey:
		return &qcAllocator{journal: b.journal}, true, nil
	}
	return b.sqBinder.Lookup(ctx, key)
}

func qcFixture(t *testing.T, caller bool, mode, operation string) (*Handler, rhandler.Invocation, *qcJournal, *sql.DB, *sql.Tx, *[]string) {
	t.Helper()
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, `CREATE TABLE queue_items(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE,note INTEGER)`, `CREATE TABLE queue_effects(id INTEGER)`, `CREATE TRIGGER queue_added AFTER INSERT ON queue_items BEGIN INSERT INTO queue_effects VALUES(new.id);END`); err != nil {
		t.Fatal(err)
	}
	var tx *sql.Tx
	var opts []sqldml.Option
	if caller {
		var err error
		tx, err = db.DB.Begin()
		if err != nil {
			t.Fatal(err)
		}
		opts = append(opts, sqldml.WithTx(tx))
		t.Cleanup(func() { tx.Rollback() })
	}
	data := sqldml.NewData(db.DB, opts...)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[qcRow]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: operation}, RootView: &spec.View{Name: "Rows", QueueContract: "source-row", Source: &spec.ViewSource{Table: "queue_items"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, AutoIncrement: true}, {Name: "name", Source: "name"}, {Name: "note", Source: "note"}}}}
	h, err := New(c, reflect.TypeFor[qcInput](), reflect.TypeFor[qcOutput](), operation)
	if err != nil {
		t.Fatal(err)
	}
	h.metadata.Root.HookType = reflect.TypeFor[qcHook]()
	if operation == "patch" && mode == "update" {
		h.metadata.Root.HookType = nil
	}
	one, two := 7, 8
	input := &qcInput{Rows: []*qcRow{{Name: "dup", Note: &one, Has: &qcHas{Name: true, Note: true}}, {Name: "dup", Note: &two, Has: &qcHas{Name: true, Note: true}}}}
	if mode == "update" {
		input.Rows = input.Rows[:1]
		input.Rows[0].ID = 1
		input.Rows[0].Has.ID = true
		input.CurrentRows = []*qcRow{{ID: 1, Name: "old"}}
	}
	snapshot, err := h.CaptureInput(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	calls := &[]string{}
	journal := &qcJournal{Data: data, input: input, calls: calls}
	binder := &qcBinder{sqBinder: &sqBinder{data: data}, journal: journal, mode: mode}
	return h, rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: binder}, journal, db.DB, tx, calls
}

func TestQueueContractNativeWholeGroupAllocationAndPrefix(t *testing.T) {
	for _, caller := range []bool{false, true} {
		t.Run(fmt.Sprint(caller), func(t *testing.T) {
			h, in, data, db, tx, calls := qcFixture(t, caller, "public-scalar", "post")
			out, err := h.Execute(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(*calls, ",") != "start,allocate:2,append,append" {
				t.Fatal("native phase/group order", *calls)
			}
			rows := in.Input.(*qcInput).Rows
			if out.(*qcOutput).Data[0] != rows[0] || rows[0].ID == 0 || rows[1].ID == 0 || rows[0].ID == rows[1].ID {
				t.Fatal("original native/public pointer identity lost")
			}
			if rows[0].Name != "later-public" {
				t.Fatal("later permitted public scalar was lost")
			}
			err = data.Complete(context.Background(), nil)
			if err == nil || !strings.Contains(err.Error(), "UNIQUE") {
				t.Fatal("genuine second statement failure required", err)
			}
			if caller {
				for _, table := range []string{"queue_items", "queue_effects"} {
					var n int
					if e := tx.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); e != nil || n != 1 {
						t.Fatal(table, n, e)
					}
				}
				var name string
				if e := tx.QueryRow(`SELECT name FROM queue_items`).Scan(&name); e != nil || name != "dup" {
					t.Fatal(name, e)
				}
				if _, e := tx.Exec(`INSERT INTO queue_items(name) VALUES('caller-usable')`); e != nil {
					t.Fatal(e)
				}
				if e := tx.Rollback(); e != nil {
					t.Fatal(e)
				}
			}
			for _, table := range []string{"queue_items", "queue_effects"} {
				var n int
				if e := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); e != nil || n != 0 {
					t.Fatal(table, n, e)
				}
			}
		})
	}
}

func TestQueueContractNativePublicSlotsAndSharedPayload(t *testing.T) {
	for _, mode := range []string{"pointee", "presence", "slot", "length", "retained-slot", "retained-pointee"} {
		t.Run(mode, func(t *testing.T) {
			h, in, data, db, _, _ := qcFixture(t, false, mode, "post")
			_, err := h.Execute(context.Background(), in)
			if strings.HasPrefix(mode, "retained-") {
				if err != nil {
					t.Fatal(err)
				}
				rows := in.Input.(*qcInput).Rows
				if mode == "retained-slot" {
					in.Input.(*qcInput).Rows = append([]*qcRow(nil), rows...)
				} else {
					*rows[0].Note = 33
				}
				err = data.Complete(context.Background(), nil)
			} else {
				_ = data.Complete(context.Background(), err)
			}
			if err == nil {
				t.Fatal("queue authority mutation must fail")
			}
			var n int
			if e := db.QueryRow(`SELECT COUNT(*) FROM queue_items`).Scan(&n); e != nil || n != 0 {
				t.Fatal(n, e)
			}
		})
	}
}

func TestQueueContractUpdateFailsBeforeNativeAllocation(t *testing.T) {
	h, in, data, _, _, calls := qcFixture(t, false, "update", "patch")
	_, err := h.Execute(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), "does not support update") {
		t.Fatal(err)
	}
	if len(*calls) != 0 {
		t.Fatal("unsupported action started allocation/transaction", *calls)
	}
	_ = data.Complete(context.Background(), err)
}

func TestQueueContractDirectMetadataAdmission(t *testing.T) {
	for _, tc := range []struct {
		contract, operation    string
		aux, matched, criteria bool
	}{{"source-slice", "put", false, false, false}, {"unknown", "post", false, false, false}, {"source-row", "put", false, false, false}, {"source-row", "get", false, false, false}, {"source-row", "post", true, false, false}, {"source-row", "patch", false, true, false}, {"source-row", "patch", false, false, true}} {
		r := &Record{QueueContract: tc.contract, Table: "rows", Auxiliary: tc.aux}
		if tc.matched {
			r.ConcurrencyToken = &Field{Name: "Version"}
		}
		if tc.criteria {
			v := 0
			r.MutationPredicateGroup = &v
		}
		if err := validateQueueContracts(r, tc.operation); err == nil {
			t.Fatalf("unsupported metadata admitted %+v", tc)
		}
	}
}

// Hide the optional queue capability while retaining the required native guard
// enrollment surface. Failure must precede transaction/allocation activity.
type qcWithoutQueue struct {
	xhandler.DML
	data *sqldml.Data
}

func (d *qcWithoutQueue) RegisterExecutionGuard(f func(context.Context) error) error {
	return d.data.RegisterExecutionGuard(f)
}
func (d *qcWithoutQueue) EnableCapturedExecutionGuards() error {
	return d.data.EnableCapturedExecutionGuards()
}
func (d *qcWithoutQueue) ValidateExecutionGuards(ctx context.Context) error {
	return d.data.ValidateExecutionGuards(ctx)
}
func (d *qcWithoutQueue) CloseMutationAdmission() error { return d.data.CloseMutationAdmission() }

type qcRejectedRegistration struct{ *qcJournal }

func (d *qcRejectedRegistration) RegisterExecutionGuard(func(context.Context) error) error {
	return errors.New("fixture enrollment rejected")
}

type qcServiceBinder struct {
	*qcBinder
	service any
}

func (b *qcServiceBinder) Lookup(ctx context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key == xhandler.DMLKey {
		return b.service, true, nil
	}
	return b.qcBinder.Lookup(ctx, key)
}
func TestQueueContractCapabilityAndRegistrationFailBeforeAllocation(t *testing.T) {
	for _, mode := range []string{"missing-capability", "failed-registration"} {
		t.Run(mode, func(t *testing.T) {
			h, in, data, db, _, calls := qcFixture(t, false, "", "post")
			binder := in.Binder.(*qcBinder)
			var service any = &qcWithoutQueue{DML: data.Data, data: data.Data}
			if mode == "failed-registration" {
				service = &qcRejectedRegistration{qcJournal: data}
			}
			in.Binder = &qcServiceBinder{qcBinder: binder, service: service}
			_, err := h.Execute(context.Background(), in)
			if err == nil || len(*calls) != 0 {
				t.Fatal("failed admission performed work", err, *calls)
			}
			// Retrying the same captured attempt cannot resume after failed enrollment.
			_, retry := h.Execute(context.Background(), in)
			if retry == nil || len(*calls) != 0 {
				t.Fatal("retired capture resumed", retry, *calls)
			}
			_ = data.Complete(context.Background(), err)
			var n int
			if err = db.QueryRow("SELECT COUNT(*) FROM queue_items").Scan(&n); err != nil || n != 0 {
				t.Fatal(n, err)
			}
		})
	}
}
func TestQueueContractRetainsNativeDuplicatePointerGuard(t *testing.T) {
	h, in, data, _, _, calls := qcFixture(t, false, "", "patch")
	h.metadata.Root.WriterActionPolicy = "insert-delete"
	input := in.Input.(*qcInput)
	input.Rows[0].ID = 1
	input.Rows[0].Has.ID = true
	input.Rows[1] = input.Rows[0]
	capture, err := h.CaptureInput(context.Background(), input)
	if err != nil {
		_ = data.Complete(context.Background(), err)
		return
	}
	in.Snapshot = capture
	_, err = h.Execute(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), "duplicate physical writer action") || strings.Contains(strings.Join(*calls, ","), "append") {
		t.Fatal("existing duplicate guard must reject all aliases before queue", err, *calls)
	}
	_ = data.Complete(context.Background(), err)
}

type qcAncestor struct {
	Rows  []*qcRow
	Label string
}
type qcNestedInput struct{ Parents []*qcAncestor }

func TestQueueContractImmutableAncestorDescriptor(t *testing.T) {
	for _, mode := range []string{"scalar", "parent-slot", "parent-backing", "child-slot", "frame-retarget", "relation-path"} {
		t.Run(mode, func(t *testing.T) {
			child := &qcRow{ID: 1, Name: "one"}
			parent := &qcAncestor{Rows: []*qcRow{child}}
			input := &qcNestedInput{Parents: []*qcAncestor{parent}}
			rootRecord, childRecord := &Record{Path: "Parents"}, &Record{Path: "Parents/Rows", QueueContract: "source-row"}
			relation := &Relation{Field: []int{0}, Child: childRecord}
			rootRecord.Relations = []*Relation{relation}
			parentFrame := &Frame{Record: rootRecord, Entity: reflect.ValueOf(parent), holderTracked: true, holderIndexed: true, holderPosition: 0}
			childFrame := &Frame{Record: childRecord, Parent: parentFrame, Entity: reflect.ValueOf(child), holderTracked: true, holderIndexed: true, holderPosition: 0, Location: "Parents[0]/Rows[0]"}
			p := &Program{input: input, metadata: &Metadata{InputField: 0, Root: rootRecord}}
			seal, err := p.captureQueueSlots(childFrame)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "scalar":
				parent.Label = "later"
				child.Name = "later"
			case "parent-slot":
				input.Parents[0] = &qcAncestor{Rows: parent.Rows}
			case "parent-backing":
				input.Parents = append([]*qcAncestor(nil), input.Parents...)
			case "child-slot":
				parent.Rows[0] = &qcRow{ID: 1, Name: "one"}
			case "frame-retarget":
				childFrame.Entity = reflect.ValueOf(&qcRow{ID: 1, Name: "retarget"})
				parent.Rows[0] = childFrame.Entity.Interface().(*qcRow)
			case "relation-path":
				relation.Field[0] = 1 // Descriptors retain their own original field path.
			}
			err = seal.validate(reflect.ValueOf(input))
			allowed := mode == "scalar" || mode == "relation-path"
			if (err == nil) != allowed {
				t.Fatal(mode, err)
			}
		})
	}
}

func TestSourceSliceWriterNativeAdmissionAndRollback(t *testing.T) {
	for _, caller := range []bool{false, true} {
		t.Run(fmt.Sprint(caller), func(t *testing.T) {
			h, in, data, db, tx, calls := qcFixture(t, caller, "", "post")
			h.metadata.Root.QueueContract = "source-slice"
			_, err := h.Execute(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(*calls, ","); got != "start,allocate:2,append" {
				t.Fatalf("source slice must be one native admission: %s", got)
			}
			// Duplicate unique names fail on completion, after queue admission. Neither
			// the first row nor its trigger effects may escape the transaction.
			err = data.Complete(context.Background(), nil)
			if err == nil {
				t.Fatal("expected duplicate-name failure")
			}
			query := db.QueryRow
			if tx != nil {
				query = tx.QueryRow
			}
			for _, table := range []string{"queue_items", "queue_effects"} {
				var count int
				if e := query("SELECT COUNT(*) FROM " + table).Scan(&count); e != nil || count != 0 {
					t.Fatal(table, count, e)
				}
			}
		})
	}
}

func TestSourceSliceWriterRetainedMutationGuards(t *testing.T) {
	for _, mode := range []string{"public-scalar", "pointee", "presence", "slot", "length"} {
		t.Run(mode, func(t *testing.T) {
			h, in, data, _, _, calls := qcFixture(t, false, mode, "post")
			h.metadata.Root.QueueContract = "source-slice"
			_, err := h.Execute(context.Background(), in)
			if err == nil {
				t.Fatal("source slice lost retained guard", mode)
			}
			if strings.Join(*calls, ",") != "start,allocate:2,append" {
				t.Fatal(*calls)
			}
			_ = data.Complete(context.Background(), err)
		})
	}
}

func TestSourceSlicePreservesParentAndInterveningActionBoundaries(t *testing.T) {
	childRecord := &Record{QueueContract: "source-slice"}
	ordinaryRecord := &Record{}
	parentA, parentB := &Frame{}, &Frame{}
	p := &Program{frames: &MutationFrames{}, actions: &MutationActions{}}
	add := func(record *Record, parent *Frame) {
		entity := reflect.ValueOf(&qcRow{})
		frame := &Frame{Record: record, Parent: parent, Entity: entity}
		p.frames.Rows = append(p.frames.Rows, frame)
		p.actions.Rows = append(p.actions.Rows, &Action{Kind: xhandler.WriteInsert, Entity: entity, frame: frame})
	}
	add(childRecord, parentA)
	add(childRecord, parentA)
	add(childRecord, parentB)
	add(childRecord, parentB)
	add(ordinaryRecord, parentB)
	add(childRecord, parentB)
	for start, want := range map[int]int{0: 2, 2: 4, 4: 4, 5: 6} {
		if got := p.sourceSliceEnd(start); got != want {
			t.Fatalf("start %d: end %d, want %d", start, got, want)
		}
	}
}

func TestSourceSliceUpdateRejectedBeforeAllocation(t *testing.T) {
	h, in, data, _, _, calls := qcFixture(t, false, "update", "patch")
	h.metadata.Root.QueueContract = "source-slice"
	_, err := h.Execute(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), "requires INSERT") || len(*calls) != 0 {
		t.Fatal(err, *calls)
	}
	_ = data.Complete(context.Background(), err)
}

type qcObservedHook struct{ qcHook }

func (*qcObservedHook) ObserveQueueAttempt(context.Context, xhandler.QueueAttemptEvent) {}

func TestSourceSliceRejectsRowObserverBeforeWork(t *testing.T) {
	h, in, data, _, _, calls := qcFixture(t, false, "", "post")
	h.metadata.Root.QueueContract = "source-slice"
	h.metadata.Root.HookType = reflect.TypeFor[qcObservedHook]()
	h.metadata.HookType = h.metadata.Root.HookType
	snapshot, captureErr := h.CaptureInput(context.Background(), in.Input)
	if captureErr != nil {
		t.Fatal(captureErr)
	}
	in.Snapshot = snapshot
	_, err := h.Execute(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), "per-row queue observation") || len(*calls) != 0 {
		t.Fatal(err, *calls)
	}
	_ = data.Complete(context.Background(), err)
}

type qcGroupedJournal struct {
	*qcJournal
	groups [][]*qcRow
}

func (d *qcGroupedJournal) InsertWithQueueContract(table string, rows any, contract rhandler.QueueContract) error {
	group, ok := rows.([]*qcRow)
	if !ok || contract != rhandler.SourceSlice {
		return fmt.Errorf("expected native source-slice payload")
	}
	d.groups = append(d.groups, append([]*qcRow(nil), group...))
	return d.qcJournal.InsertWithQueueContract(table, rows, contract)
}

func TestSourceSliceNativeParentGroupsSharePendingJournal(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, `CREATE TABLE queue_items(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE,note INTEGER)`); err != nil {
		t.Fatal(err)
	}
	children := []*qcRow{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}, {ID: 3, Name: "c"}, {ID: 4, Name: "d"}}
	parents := []*qcAncestor{{Rows: children[:2]}, {Rows: children[2:]}}
	root := &Record{Path: "Parents"}
	child := &Record{Path: "Parents/Rows", Table: "queue_items", QueueContract: "source-slice"}
	root.Relations = []*Relation{{Field: []int{0}, Child: child}}
	p := &Program{input: &qcNestedInput{Parents: parents}, metadata: &Metadata{Root: root, InputField: 0}, frames: &MutationFrames{}, actions: &MutationActions{}}
	for i, parent := range parents {
		pf := &Frame{Record: root, Entity: reflect.ValueOf(parent), holderTracked: true, holderIndexed: true, holderPosition: i}
		p.frames.Rows = append(p.frames.Rows, pf)
		for j, row := range parent.Rows {
			f := &Frame{Record: child, Entity: reflect.ValueOf(row), Parent: pf, holderTracked: true, holderIndexed: true, holderPosition: j}
			p.frames.Rows = append(p.frames.Rows, f)
			p.actions.Rows = append(p.actions.Rows, &Action{Kind: xhandler.WriteInsert, Entity: f.Entity, frame: f})
		}
	}
	data := sqldml.NewData(db.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	calls := []string{}
	journal := &qcGroupedJournal{qcJournal: &qcJournal{Data: data, input: &qcInput{Rows: children}, calls: &calls}}
	binder := &qcServiceBinder{qcBinder: &qcBinder{sqBinder: &sqBinder{data: data}, journal: journal.qcJournal}, service: journal}
	if err := p.queue(ctx, binder); err != nil {
		t.Fatal(err)
	}
	if len(journal.groups) != 2 || len(journal.groups[0]) != 2 || len(journal.groups[1]) != 2 || journal.groups[0][0] != children[0] || journal.groups[1][0] != children[2] {
		t.Fatal("parent group boundaries or row identities lost", journal.groups)
	}
	var count int
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM queue_items").Scan(&count); err != nil || count != 0 {
		t.Fatal("queue flushed before completion", count, err)
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM queue_items").Scan(&count); err != nil || count != 4 {
		t.Fatal(count, err)
	}
}

func TestSourceSliceHookFailureStopsRemainingHooksAndRollsBack(t *testing.T) {
	for _, caller := range []bool{false, true} {
		t.Run(fmt.Sprint(caller), func(t *testing.T) {
			h, in, data, db, tx, calls := qcFixture(t, caller, "error", "post")
			h.metadata.Root.QueueContract = "source-slice"
			_, err := h.Execute(context.Background(), in)
			if err == nil || !strings.Contains(err.Error(), "fixture after-queue rejection") {
				t.Fatal(err)
			}
			hook := in.Snapshot.(*Program).hook.Interface().(*qcHook)
			if len(hook.visited) != 1 || strings.Join(*calls, ",") != "start,allocate:2,append" {
				t.Fatal("continued after failed first hook", hook.visited, *calls)
			}
			_ = data.Complete(context.Background(), err)
			query := db.QueryRow
			if tx != nil {
				query = tx.QueryRow
			}
			for _, table := range []string{"queue_items", "queue_effects"} {
				var count int
				if e := query("SELECT COUNT(*) FROM " + table).Scan(&count); e != nil || count != 0 {
					t.Fatal(table, count, e)
				}
			}
		})
	}
}

func TestSourceSliceDeleteRejectedBeforeAllocation(t *testing.T) {
	h, in, data, _, _, calls := qcFixture(t, false, "update", "patch")
	h.metadata.Root.QueueContract = "source-slice"
	input := in.Input.(*qcInput)
	input.Rows[0].Remove = true
	input.Rows[0].Has.Remove = true
	snapshot, err := h.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	in.Snapshot = snapshot
	_, err = h.Execute(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), "requires INSERT") || len(*calls) != 0 {
		t.Fatal(err, *calls)
	}
	_ = data.Complete(context.Background(), err)
}

func TestSourceSliceSingletonHolderPreflightRejects(t *testing.T) {
	_, in, data, _, _, calls := qcFixture(t, false, "", "post")
	row := reflect.ValueOf(&qcRow{ID: 1})
	root := &Record{QueueContract: "source-slice", Table: "queue_items"}
	f := &Frame{Record: root, Entity: row, holderTracked: true}
	p := &Program{metadata: &Metadata{Root: root}, frames: &MutationFrames{Rows: []*Frame{f}}, actions: &MutationActions{Rows: []*Action{{Kind: xhandler.WriteInsert, Entity: row, frame: f}}}}
	err := p.preflightQueueContracts(context.Background(), in.Binder)
	if err == nil || !strings.Contains(err.Error(), "collection holder") || len(*calls) != 0 {
		t.Fatal(err, *calls)
	}
	_ = data.Complete(context.Background(), err)
}
