package writer

import (
	"context"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
)

// An explicitly supplied audit value must remain in a real UPDATE even when
// equal to Previous: omitting it can activate a database ON UPDATE default.
func TestGeneratedPreviousComparisonRetainsEqualAudit(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		changed, audit        bool
		wantWrites, wantAudit int
	}{
		{"business change retains supplied equal audit", true, true, 1, 1},
		{"equal business and audit remain physical noop", false, true, 0, 0},
		{"unmarked audit remains omitted", true, false, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx,
				"CREATE TABLE records(ID INTEGER PRIMARY KEY, ACCOUNT_ID INTEGER, NAME TEXT, QUANTITY INTEGER, UPDATED TEXT, UPDATED_USER INTEGER)",
				"INSERT INTO records VALUES(1,7,'stored',9,'2026-01-01T00:00:00Z',3)",
				"CREATE TABLE writes(kind TEXT)",
				"CREATE TRIGGER business_write AFTER UPDATE ON records BEGIN INSERT INTO writes VALUES('row'); END",
				"CREATE TRIGGER audit_write AFTER UPDATE OF UPDATED ON records BEGIN INSERT INTO writes VALUES('audit'); END"); err != nil {
				t.Fatal(err)
			}
			stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			name := "stored"
			if tc.changed {
				name = "changed"
			}
			row := &previousPresenceRow{ID: ptr(1), AccountID: ptr(7), Name: &name, Quantity: ptr(9), Updated: &stamp, UpdatedUser: ptr(3), Has: &previousPresenceHas{ID: true, Name: true, Updated: tc.audit}}
			previous := &previousPresenceRow{ID: ptr(1), AccountID: ptr(7), Name: ptr("stored"), Quantity: ptr(9), Updated: &stamp, UpdatedUser: ptr(3)}
			_, err := runSQLiteWriter(t, ctx, db, unitComponent(), &previousPresenceInput{Rows: []*previousPresenceRow{row}, Current: []*previousPresenceRow{previous}}, &previousPresenceOutput{}, "patch")
			if err != nil {
				t.Fatal(err)
			}
			for kind, want := range map[string]int{"row": tc.wantWrites, "audit": tc.wantAudit} {
				var got int
				if err := db.DB.QueryRow("SELECT COUNT(*) FROM writes WHERE kind=?", kind).Scan(&got); err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Errorf("%s writes: got %d want %d", kind, got, want)
				}
			}
		})
	}
}
