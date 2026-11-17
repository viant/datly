package writer

import (
	"context"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
)

type transientEligibilityHas struct{ ID, Name, Audit, Memo, Derived, Joined, Remove bool }
type transientEligibilityRow struct {
	ID      *int                     `sqlx:"ID,primaryKey=true"`
	Name    *string                  `sqlx:"NAME"`
	Audit   *int                     `sqlx:"UPDATED_USER" writer:"audit"`
	Memo    string                   `sqlx:"-"`
	Derived string                   `sqlx:"DERIVED,transient=true"`
	Joined  string                   `sqlx:"JOINED" on:"ID=ID"`
	Remove  bool                     `sqlx:"-" writer:"delete"`
	Has     *transientEligibilityHas `setMarker:"true" sqlx:"-"`
}
type transientEligibilityInput struct {
	Rows    []*transientEligibilityRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=records"`
	Current []*transientEligibilityRow `parameter:"Current,kind=view,in=Current" view:"Current,table=records"`
}
type transientEligibilityOutput struct {
	Data []*transientEligibilityRow `parameter:"Data,kind=output,in=body"`
}

// SQLX already defines transient fields. Their presence must remain available
// to hooks and control selection without independently qualifying a row UPDATE.
func TestTransientPresenceDoesNotTriggerPhysicalUpdate(t *testing.T) {
	for _, mode := range []string{"ignored", "transient", "joined", "false deletion", "business", "delete"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx,
				"CREATE TABLE records(ID INTEGER PRIMARY KEY, NAME TEXT, UPDATED_USER INTEGER)",
				"CREATE TABLE writes(kind TEXT)",
				"INSERT INTO records VALUES(1,'stored',3)",
				"CREATE TRIGGER record_updated AFTER UPDATE ON records BEGIN INSERT INTO writes VALUES('update'); END",
				"CREATE TRIGGER record_deleted AFTER DELETE ON records BEGIN INSERT INTO writes VALUES('delete'); END"); err != nil {
				t.Fatal(err)
			}
			row := &transientEligibilityRow{
				ID: ptr(1), Name: ptr("changed"), Audit: ptr(4), Memo: "input", Derived: "input", Joined: "input", Remove: mode == "delete",
				Has: &transientEligibilityHas{ID: true, Name: mode == "business", Audit: true,
					Memo: mode == "ignored" || mode == "business", Derived: mode == "transient",
					Joined: mode == "joined", Remove: mode == "false deletion" || mode == "delete"},
			}
			input := &transientEligibilityInput{Rows: []*transientEligibilityRow{row}, Current: []*transientEligibilityRow{{ID: ptr(1), Name: ptr("stored"), Audit: ptr(3)}}}
			_, err := runSQLiteWriter(t, ctx, db, unitComponent(), input, &transientEligibilityOutput{}, "patch")
			if err != nil {
				t.Fatal(err)
			}
			var count int
			var kind string
			if err = db.DB.QueryRow("SELECT COUNT(*),COALESCE(MIN(kind),'') FROM writes").Scan(&count, &kind); err != nil {
				t.Fatal(err)
			}
			wantKind := ""
			if mode == "business" {
				wantKind = "update"
			} else if mode == "delete" {
				wantKind = "delete"
			}
			wantCount := 0
			if wantKind != "" {
				wantCount = 1
			}
			if count != wantCount || kind != wantKind {
				t.Fatalf("physical writes=%d/%s; want=%d/%s", count, kind, wantCount, wantKind)
			}
			if wantKind == "" && (*row.Audit != 3 || row.Has.Audit) {
				t.Fatal("no-op retained changed audit values or markers")
			}
			if row.Memo != "input" || row.Derived != "input" || row.Joined != "input" {
				t.Fatal("transient request values were lost")
			}
		})
	}
}
