package writer

import (
	"context"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
)

type auditGraphHas struct{ ID, Name, UpdatedBy, Children bool }
type auditGraphRow struct {
	ID        *int               `sqlx:"id,primaryKey=true"`
	Name      *string            `sqlx:"name"`
	UpdatedBy *int               `sqlx:"updated_by" writer:"audit"`
	Children  []*auditGraphChild `view:"Children,table=children" on:"ID=ParentID"`
	Has       *auditGraphHas     `setMarker:"true" sqlx:"-"`
}
type auditGraphChildHas struct{ ID, ParentID, Name, UpdatedBy, Remove bool }
type auditGraphChild struct {
	ID        *int                `sqlx:"id,primaryKey=true"`
	ParentID  *int                `sqlx:"parent_id"`
	Name      *string             `sqlx:"name"`
	UpdatedBy *int                `sqlx:"updated_by" writer:"audit"`
	Remove    bool                `sqlx:"-" writer:"delete"`
	Has       *auditGraphChildHas `setMarker:"true" sqlx:"-"`
}
type auditGraphInput struct {
	Rows            []*auditGraphRow   `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
	CurrentRows     []*auditGraphRow   `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=parents"`
	CurrentChildren []*auditGraphChild `parameter:"CurrentChildren,kind=view" view:"CurrentChildren,table=children"`
}
type auditGraphOutput struct {
	Data []*auditGraphRow `parameter:"Data,kind=output,in=body"`
}

func TestAuditEligibilityDoesNotSuppressChildWork(t *testing.T) {
	for _, change := range []string{"parent", "child", "delete"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx,
				"CREATE TABLE parents(id INTEGER PRIMARY KEY,name TEXT,updated_by INTEGER)",
				"CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER,name TEXT,updated_by INTEGER)",
				"CREATE TABLE writes(kind TEXT)",
				"INSERT INTO parents VALUES(1,'parent',3)", "INSERT INTO children VALUES(2,1,'child',3)",
				"CREATE TRIGGER parent_updated AFTER UPDATE ON parents BEGIN INSERT INTO writes VALUES('parent'); END",
				"CREATE TRIGGER child_updated AFTER UPDATE ON children BEGIN INSERT INTO writes VALUES('child'); END",
				"CREATE TRIGGER child_deleted AFTER DELETE ON children BEGIN INSERT INTO writes VALUES('delete'); END"); err != nil {
				t.Fatal(err)
			}
			child := &auditGraphChild{ID: ptr(2), Name: ptr("changed"), UpdatedBy: ptr(4), Remove: change == "delete", Has: &auditGraphChildHas{ID: true, Name: change == "child", UpdatedBy: true, Remove: change == "delete"}}
			row := &auditGraphRow{ID: ptr(1), Name: ptr("changed"), UpdatedBy: ptr(4), Children: []*auditGraphChild{child}, Has: &auditGraphHas{ID: true, Name: change == "parent", UpdatedBy: true, Children: true}}
			input := &auditGraphInput{Rows: []*auditGraphRow{row}, CurrentRows: []*auditGraphRow{{ID: ptr(1), Name: ptr("parent"), UpdatedBy: ptr(3)}}, CurrentChildren: []*auditGraphChild{{ID: ptr(2), ParentID: ptr(1), Name: ptr("child"), UpdatedBy: ptr(3)}}}
			_, err := runSQLiteWriter(t, ctx, db, unitComponent(), input, &auditGraphOutput{}, "patch")
			if err != nil {
				t.Fatal(err)
			}
			var count int
			var kind string
			if err = db.DB.QueryRow("SELECT COUNT(*),MIN(kind) FROM writes").Scan(&count, &kind); err != nil {
				t.Fatal(err)
			}
			if count != 1 || kind != change {
				t.Fatalf("expected only %s work; writes=%d kind=%s", change, count, kind)
			}
			if change != "parent" && (*row.UpdatedBy != 3 || row.Has.UpdatedBy) {
				t.Fatal("unchanged parent audit changed")
			}
			if change == "parent" && (*child.UpdatedBy != 3 || child.Has.UpdatedBy) {
				t.Fatal("unchanged child audit changed")
			}
		})
	}
}
