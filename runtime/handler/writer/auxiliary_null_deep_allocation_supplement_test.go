package writer

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

type deepAuxLeafHas struct{ ID, MidID, Label bool }
type deepAuxLeaf struct {
	ID    *int            `sqlx:"id,primaryKey,autoincrement"`
	MidID *int            `sqlx:"mid_id"`
	Label *string         `sqlx:"label"`
	Has   *deepAuxLeafHas `setMarker:"true" sqlx:"-" json:"-"`
}
type deepAuxMidHas struct{ ID, RootID, Name, Leaves bool }
type deepAuxMid struct {
	ID     *int           `sqlx:"id,primaryKey"`
	RootID *int           `sqlx:"root_id"`
	Name   *string        `sqlx:"name"`
	Leaves []*deepAuxLeaf `view:"Leaves,table=deep_leaves" on:"ID=MidID"`
	Has    *deepAuxMidHas `setMarker:"true" sqlx:"-" json:"-"`
}
type deepAuxRootHas struct{ ID, Name, Mids bool }
type deepAuxRoot struct {
	ID   *int            `sqlx:"id,primaryKey"`
	Name *string         `sqlx:"name"`
	Mids []*deepAuxMid   `view:"Mids,table=deep_mids,auxiliary=true,nestedNullPolicy=skip-auxiliary" on:"ID=RootID"`
	Has  *deepAuxRootHas `setMarker:"true" sqlx:"-" json:"-"`
}
type deepAuxInput struct {
	Rows          []*deepAuxRoot `parameter:"Rows,kind=body,in=data" view:"Rows,table=deep_roots,auxiliary=true,rootNullPolicy=skip-auxiliary"`
	CurrentRows   []*deepAuxRoot `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=deep_roots"`
	CurrentMids   []*deepAuxMid  `parameter:"CurrentMids,kind=view" view:"CurrentMids,table=deep_mids"`
	CurrentLeaves []*deepAuxLeaf `parameter:"CurrentLeaves,kind=view" view:"CurrentLeaves,table=deep_leaves"`
}
type deepAuxOutput struct {
	Data []*deepAuxRoot `parameter:"Data,kind=output,in=body"`
}

func TestAuxiliaryNullDeepGraphAllocatesOnlyWritableLeaves(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE deep_roots(id INTEGER PRIMARY KEY,name TEXT)", "CREATE TABLE deep_mids(id INTEGER PRIMARY KEY,root_id INTEGER NOT NULL REFERENCES deep_roots(id),name TEXT)", "CREATE TABLE deep_leaves(id INTEGER PRIMARY KEY AUTOINCREMENT,mid_id INTEGER NOT NULL REFERENCES deep_mids(id),label TEXT)", "INSERT INTO deep_roots VALUES(1,'root original')", "INSERT INTO deep_mids VALUES(10,1,'mid original')"); err != nil {
		t.Fatal(err)
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[deepAuxInput]().PkgPath(), Name: "DeepAux"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/deep-aux"}}, Settings: &spec.Settings{}, RootView: &spec.View{Name: "Rows", Auxiliary: true, RootNullPolicy: "skip-auxiliary", Source: &spec.ViewSource{Table: "deep_roots"}, Relations: []*spec.Relation{{Name: "Mids", Holder: "Mids", View: &spec.View{Name: "Mids", Auxiliary: true, NestedNullPolicy: "skip-auxiliary", Source: &spec.ViewSource{Table: "deep_mids"}, Relations: []*spec.Relation{{Name: "Leaves", Holder: "Leaves", View: &spec.View{Name: "Leaves", Source: &spec.ViewSource{Table: "deep_leaves"}}}}}}}}}
	first := &deepAuxLeaf{Label: ptr("first"), Has: &deepAuxLeafHas{Label: true}}
	second := &deepAuxLeaf{Label: ptr("second"), Has: &deepAuxLeafHas{Label: true}}
	mid := &deepAuxMid{ID: ptr(10), RootID: ptr(1), Name: ptr("must not write mid"), Leaves: []*deepAuxLeaf{first, second}, Has: &deepAuxMidHas{ID: true, RootID: true, Name: true, Leaves: true}}
	root := &deepAuxRoot{ID: ptr(1), Name: ptr("must not write root"), Mids: []*deepAuxMid{nil, mid, nil}, Has: &deepAuxRootHas{ID: true, Name: true, Mids: true}}
	in := &deepAuxInput{Rows: []*deepAuxRoot{nil, root, nil}, CurrentRows: []*deepAuxRoot{{ID: ptr(1), Name: ptr("root original")}}, CurrentMids: []*deepAuxMid{{ID: ptr(10), RootID: ptr(1), Name: ptr("mid original")}}}
	result, err := runSQLiteWriter(t, ctx, db, c, in, &deepAuxOutput{}, "patch")
	if err != nil {
		t.Fatal(err)
	}
	out := result.(*deepAuxOutput)
	if len(out.Data) != 3 || out.Data[0] != nil || out.Data[1] != root || out.Data[2] != nil || len(root.Mids) != 3 || root.Mids[0] != nil || root.Mids[1] != mid || root.Mids[2] != nil || !root.Has.Mids || !mid.Has.Leaves {
		t.Fatal("auxiliary positions/pointers/presence changed")
	}
	if first.ID == nil || second.ID == nil || *first.ID == 0 || *second.ID == 0 || *first.ID == *second.ID || first.MidID == nil || second.MidID == nil || *first.MidID != 10 || *second.MidID != 10 {
		t.Fatal("writable leaf allocation or parent links incorrect")
	}
	var rows int
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM deep_leaves WHERE mid_id=10 AND ((id=? AND label='first') OR (id=? AND label='second'))", *first.ID, *second.ID).Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("physical leaves=%d: %v", rows, err)
	}
	var name string
	for _, q := range []struct{ sql, want string }{{"SELECT name FROM deep_roots WHERE id=1", "root original"}, {"SELECT name FROM deep_mids WHERE id=10", "mid original"}} {
		if err := db.DB.QueryRow(q.sql).Scan(&name); err != nil || name != q.want {
			t.Fatalf("auxiliary table changed: %s %v", name, err)
		}
	}
	mid.Leaves = append(mid.Leaves, nil)
	handler, err := New(c, reflect.TypeFor[deepAuxInput](), reflect.TypeFor[deepAuxOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler.CaptureInput(ctx, in); err == nil {
		t.Fatal("auxiliary leniency inherited by writable grandchild")
	}
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM deep_leaves").Scan(&rows); err != nil || rows != 2 {
		t.Fatal("rejected writable null changed physical records")
	}
}
