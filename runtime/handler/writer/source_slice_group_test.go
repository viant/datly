package writer

import (
	"context"
	"database/sql"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type nativeGroupFixture struct {
	p       *Program
	rows    []*qcRow
	input   *qcNestedInput
	data    *sqldml.Data
	journal *qcGroupedJournal
	binder  *qcServiceBinder
	db      *sql.DB
}

func newNativeGroupFixture(t *testing.T) *nativeGroupFixture {
	t.Helper()
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, `CREATE TABLE queue_items(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE,note INTEGER)`); err != nil {
		t.Fatal(err)
	}
	rows := []*qcRow{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}, {ID: 3, Name: "c"}, {ID: 4, Name: "d"}}
	parents := []*qcAncestor{{Rows: rows[:2]}, {Rows: rows[2:]}}
	root := &Record{Path: "Parents"}
	child := &Record{Path: "Parents/Rows", Table: "queue_items", QueueContract: "source-slice"}
	root.Relations = []*Relation{{Field: []int{0}, Child: child}}
	input := &qcNestedInput{Parents: parents}
	p := &Program{input: input, metadata: &Metadata{Root: root, InputField: 0}, frames: &MutationFrames{}, actions: &MutationActions{}}
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
	journal := &qcGroupedJournal{qcJournal: &qcJournal{Data: data, input: &qcInput{Rows: rows}, calls: &calls}}
	binder := &qcServiceBinder{qcBinder: &qcBinder{sqBinder: &sqBinder{data: data}, journal: journal.qcJournal}, service: journal}
	return &nativeGroupFixture{p: p, rows: rows, input: input, data: data, journal: journal, binder: binder, db: db.DB}
}
func TestNativeSourceSliceCrossParentGroup(t *testing.T) {
	f := newNativeGroupFixture(t)
	ctx := context.Background()
	if err := f.p.mintSourceSliceGroup(f.p.actions.Rows); err != nil {
		t.Fatal(err)
	}
	if err := f.p.queue(ctx, f.binder); err != nil {
		t.Fatal(err)
	}
	if len(f.journal.groups) != 1 || !reflect.DeepEqual(f.journal.groups[0], f.rows) {
		t.Fatal("aggregate group lost", f.journal.groups)
	}
	var count int
	if err := f.db.QueryRow("SELECT COUNT(*) FROM queue_items").Scan(&count); err != nil || count != 0 {
		t.Fatal("premature flush", count, err)
	}
	if err := f.data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRow("SELECT COUNT(*) FROM queue_items").Scan(&count); err != nil || count != 4 {
		t.Fatal(count, err)
	}
}
func TestNativeSourceSliceDistinctTokensRemainBoundaries(t *testing.T) {
	f := newNativeGroupFixture(t)
	// Two tokens deliberately split one parent; neither may merge with its peer.
	for _, group := range [][]*Action{f.p.actions.Rows[:1], f.p.actions.Rows[1:2], f.p.actions.Rows[2:]} {
		if err := f.p.mintSourceSliceGroup(group); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.p.queue(context.Background(), f.binder); err != nil {
		t.Fatal(err)
	}
	if len(f.journal.groups) != 3 || len(f.journal.groups[0]) != 1 || len(f.journal.groups[1]) != 1 || len(f.journal.groups[2]) != 2 {
		t.Fatal(f.journal.groups)
	}
	if err := f.data.Complete(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}
func TestNativeSourceSliceAuthorityRejectsBeforeAppend(t *testing.T) {
	cases := map[string]func(*nativeGroupFixture){
		"partial": func(f *nativeGroupFixture) { f.p.actions.Rows = f.p.actions.Rows[:3] },
		"order": func(f *nativeGroupFixture) {
			f.p.actions.Rows[0], f.p.actions.Rows[1] = f.p.actions.Rows[1], f.p.actions.Rows[0]
		},
		"kind":               func(f *nativeGroupFixture) { f.p.actions.Rows[2].Kind = xhandler.WriteUpdate },
		"removed-token":      func(f *nativeGroupFixture) { f.p.actions.Rows[0].sourceGroup = nil },
		"foreign-program":    func(f *nativeGroupFixture) { copy := *f.p; f.p = &copy },
		"replacement-action": func(f *nativeGroupFixture) { copy := *f.p.actions.Rows[2]; f.p.actions.Rows[2] = &copy },
		"both-action-and-frame-row": func(f *nativeGroupFixture) {
			a := f.p.actions.Rows[2]
			r := &qcRow{ID: 3, Name: "c"}
			f.input.Parents[1].Rows[0] = r
			a.Entity = reflect.ValueOf(r)
			a.frame.Entity = a.Entity
		},
		"holder-replacement": func(f *nativeGroupFixture) { f.input.Parents[1].Rows[0] = &qcRow{ID: 3, Name: "c"} },
		"parent-association": func(f *nativeGroupFixture) { f.p.actions.Rows[2].frame.Parent = f.p.actions.Rows[0].frame.Parent },
		"holder-position":    func(f *nativeGroupFixture) { f.p.actions.Rows[2].frame.holderPosition = 1 },
		"table":              func(f *nativeGroupFixture) { f.p.actions.Rows[0].frame.Record.Table = "other" },
		"interleaved-token":  func(f *nativeGroupFixture) { f.p.actions.Rows[1].sourceGroup = &sourceSliceGroup{owner: f.p} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newNativeGroupFixture(t)
			if err := f.p.mintSourceSliceGroup(f.p.actions.Rows); err != nil {
				t.Fatal(err)
			}
			mutate(f)
			err := f.p.queue(context.Background(), f.binder)
			if err == nil {
				t.Fatal("tampering accepted")
			}
			if len(f.journal.groups) != 0 {
				t.Fatal("partial group appended", f.journal.groups)
			}
			if err = f.data.Complete(context.Background(), err); err == nil {
				t.Fatal("fault swallowed by completion")
			}
			var count int
			if err = f.db.QueryRow("SELECT COUNT(*) FROM queue_items").Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
		})
	}
}
func TestNativeSourceSliceMintIsAtomic(t *testing.T) {
	f := newNativeGroupFixture(t)
	f.p.actions.Rows[3].Kind = xhandler.WriteDelete
	if err := f.p.mintSourceSliceGroup(f.p.actions.Rows); err == nil {
		t.Fatal("invalid member accepted")
	}
	for _, a := range f.p.actions.Rows {
		if a.sourceGroup != nil || f.p.sourceSliceGroups[a] != nil {
			t.Fatal("partial authority published")
		}
	}
}

func TestNativeSourceSliceWholeGroupDuplicateRejectedBeforeAppend(t *testing.T) {
	f := newNativeGroupFixture(t)
	if err := f.p.mintSourceSliceGroup(f.p.actions.Rows); err != nil {
		t.Fatal(err)
	}
	f.p.actions.Rows = append(f.p.actions.Rows, f.p.actions.Rows...)
	if err := f.p.queue(context.Background(), f.binder); err == nil {
		t.Fatal("whole-group duplicate accepted")
	}
	if len(f.journal.groups) != 0 {
		t.Fatal("duplicate group appended before rejection")
	}
}
func TestNativeSourceSliceSuccessfulGroupCannotReplay(t *testing.T) {
	f := newNativeGroupFixture(t)
	ctx := context.Background()
	if err := f.p.mintSourceSliceGroup(f.p.actions.Rows); err != nil {
		t.Fatal(err)
	}
	if err := f.p.queueSourceSlice(ctx, f.binder, f.journal, f.p.actions.Rows); err != nil {
		t.Fatal(err)
	}
	if err := f.p.queueSourceSlice(ctx, f.binder, f.journal, f.p.actions.Rows); err == nil {
		t.Fatal("direct group replay accepted")
	}
	if err := f.p.queue(ctx, f.binder); err == nil {
		t.Fatal("Program queue replay accepted")
	}
	if len(f.journal.groups) != 1 {
		t.Fatal("replay appended another group")
	}
}
func TestNativeSourceSliceRemovedMemberTokenRejectedAtDirectBoundary(t *testing.T) {
	f := newNativeGroupFixture(t)
	if err := f.p.mintSourceSliceGroup(f.p.actions.Rows[1:2]); err != nil {
		t.Fatal(err)
	}
	f.p.actions.Rows[1].sourceGroup = nil
	if err := f.p.queueSourceSlice(context.Background(), f.binder, f.journal, f.p.actions.Rows[:2]); err == nil {
		t.Fatal("ordinary group absorbed hidden minted member")
	}
	if len(f.journal.groups) != 0 {
		t.Fatal("invalid group appended")
	}
}

type nativeGroupRejectingJournal struct {
	*qcGroupedJournal
	attempts int
}

func (j *nativeGroupRejectingJournal) InsertWithQueueContract(_ string, _ any, _ rhandler.QueueContract) error {
	j.attempts++
	return errors.New("injected native append failure")
}
func TestNativeSourceSliceFailedAppendCannotRetry(t *testing.T) {
	f := newNativeGroupFixture(t)
	ctx := context.Background()
	if err := f.p.mintSourceSliceGroup(f.p.actions.Rows); err != nil {
		t.Fatal(err)
	}
	reject := &nativeGroupRejectingJournal{qcGroupedJournal: f.journal}
	first := f.p.queueSourceSlice(ctx, f.binder, reject, f.p.actions.Rows)
	if first == nil {
		t.Fatal("expected append failure")
	}
	if err := f.p.queueSourceSlice(ctx, f.binder, reject, f.p.actions.Rows); err == nil {
		t.Fatal("failed append group retried")
	}
	if reject.attempts != 1 || !f.p.actions.Rows[0].sourceGroup.attempted.Load() {
		t.Fatal("one-shot evidence was lost")
	}
	if err := f.data.Complete(ctx, first); err == nil {
		t.Fatal("failed attempt completion succeeded")
	}
}
