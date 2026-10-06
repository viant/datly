package writer

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/sequencer"
	"reflect"
	"testing"
)

type compositionAllocationLeaf struct {
	ID int `sqlx:"id,primaryKey,autoincrement"`
}
type compositionAllocationCarrier struct {
	Public []*compositionAllocationLeaf
	Writes []*compositionAllocationLeaf
	Groups []*compositionAllocationGroup
}
type compositionAllocationGroup struct {
	ID     int `sqlx:"id,primaryKey,autoincrement"`
	Public []*compositionAllocationLeaf
	Writes []*compositionAllocationLeaf
}

func TestAuxiliaryAllocationDeepSameTablePreservesPhysicalSiblings(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE associations(id INTEGER PRIMARY KEY AUTOINCREMENT)"); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	public, first, second := new(compositionAllocationLeaf), new(compositionAllocationLeaf), new(compositionAllocationLeaf)
	group := &compositionAllocationGroup{Public: []*compositionAllocationLeaf{public}, Writes: []*compositionAllocationLeaf{second}}
	roots := []*compositionAllocationCarrier{{Writes: []*compositionAllocationLeaf{first}, Groups: []*compositionAllocationGroup{group}}}
	leaf := func(aux bool, selector string) *Record {
		return &Record{Auxiliary: aux, Table: "associations", Selector: selector, Sequence: &Field{Name: "ID", AutoIncrement: true}}
	}
	// Defensive runtime metadata may be stale. Its own explicit sequence and an
	// unrelated identity policy still must not hide physical descendants.
	groupRecord := &Record{Auxiliary: true, Table: "associations", Selector: "Groups/ID", Sequence: &Field{Name: "ID", AutoIncrement: true}, WriterIdentityPolicy: assignedUpdateIdentity, Relations: []*Relation{{Child: leaf(true, "Groups/Public/ID")}, {Child: leaf(false, "Groups/Writes/ID")}}}
	record := &Record{Auxiliary: true, Relations: []*Relation{{Child: leaf(false, "Writes/ID")}, {Child: groupRecord}}}
	if err = (&Program{frames: &MutationFrames{}}).allocate(ctx, sequencer.New(h.DB, tx), record, reflect.ValueOf(roots)); err != nil {
		t.Fatal(err)
	}
	if group.ID != 0 || public.ID != 0 || first.ID != 1 || second.ID != 2 {
		t.Fatalf("carrier allocated or descendant/sibling lost: group=%d public=%d first=%d second=%d", group.ID, public.ID, first.ID, second.ID)
	}
	if roots[0].Groups[0] != group || group.Public[0] != public || group.Writes[0] != second || roots[0].Writes[0] != first {
		t.Fatal("allocation replaced graph pointers")
	}
}

func TestAuxiliaryAllocationSuppliedIdentityAndPresenceRemain(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE associations(id INTEGER PRIMARY KEY AUTOINCREMENT)"); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	public, physical := &compositionAllocationLeaf{ID: 41}, &compositionAllocationLeaf{ID: 57}
	roots := []*compositionAllocationCarrier{{Public: []*compositionAllocationLeaf{public}, Writes: []*compositionAllocationLeaf{physical}}}
	metadata := &Record{Auxiliary: true, Relations: []*Relation{{Child: &Record{Auxiliary: true, Table: "associations", Selector: "Public/ID", Sequence: &Field{Name: "ID", AutoIncrement: true}}}, {Child: &Record{Table: "associations", Selector: "Writes/ID", Sequence: &Field{Name: "ID", AutoIncrement: true}}}}}
	if err = (&Program{frames: &MutationFrames{}}).allocate(ctx, sequencer.New(h.DB, tx), metadata, reflect.ValueOf(roots)); err != nil {
		t.Fatal(err)
	}
	if public.ID != 41 || physical.ID != 57 {
		t.Fatalf("caller identities rewritten: %d %d", public.ID, physical.ID)
	}
}

// Focused existing Program.allocate slice using genuine native SQLite SQLX,
// not a full generated handler or alternative allocator. No synthetic IDs.
func TestCompositionAuxiliarySameTableMustNotAllocate(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE associations(id INTEGER PRIMARY KEY AUTOINCREMENT)"); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	public, physical := new(compositionAllocationLeaf), new(compositionAllocationLeaf)
	roots := []*compositionAllocationCarrier{{Public: []*compositionAllocationLeaf{public}, Writes: []*compositionAllocationLeaf{physical}}}
	child := func(aux bool, selector string) *Record {
		return &Record{Auxiliary: aux, Table: "associations", Selector: selector, Sequence: &Field{Name: "ID", AutoIncrement: true}}
	}
	record := &Record{Auxiliary: true, Relations: []*Relation{{Child: child(true, "Public/ID")}, {Child: child(false, "Writes/ID")}}}
	if err = (&Program{frames: &MutationFrames{}}).allocate(ctx, sequencer.New(h.DB, tx), record, reflect.ValueOf(roots)); err != nil {
		t.Fatal(err)
	}
	t.Logf("native Program.allocate exact same-table auxiliary=%d writable=%d; no DML invoked", public.ID, physical.ID)
	if public.ID != 0 || physical.ID <= 0 {
		t.Fatalf("nonpersistent auxiliary consumed native allocation: auxiliary=%d physical=%d", public.ID, physical.ID)
	}
}
