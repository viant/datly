package writer

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
)

type assignedHas struct{ ID, Name bool }
type assignedRow struct {
	ID   *int         `sqlx:"id,primaryKey=true"`
	Name *string      `sqlx:"name" validate:"omitempty,le(12)"`
	Has  *assignedHas `setMarker:"true" sqlx:"-"`
}
type assignedInput struct {
	Rows        []*assignedRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
	CurrentRows []*assignedRow `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=parents"`
}
type assignedOutput struct {
	Data []*assignedRow `parameter:"Data,kind=output,in=body"`
}

func assignedComponent(policy string) *spec.Component {
	return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[assignedRow]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", WriterIdentityPolicy: policy, Source: &spec.ViewSource{Table: "parents"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
}
func TestAssignedUpdateIdentitySQLite(t *testing.T) {
	for _, tc := range []struct {
		name, policy string
		id           *int
		marker       bool
		previous     bool
		value        string
		count        int
		stored       string
		failure      bool
	}{{"default missing inserts", "", ptr(999), true, false, "new", 2, "new", false}, {"opt-in missing no-op", assignedUpdateIdentity, ptr(999), true, false, "new", 1, "", false}, {"negative assigned no-op", assignedUpdateIdentity, ptr(-1), true, false, "new", 1, "", false}, {"omitted inserts", assignedUpdateIdentity, nil, false, false, "new", 2, "new", false}, {"null inserts", assignedUpdateIdentity, nil, true, false, "new", 2, "new", false}, {"zero inserts", assignedUpdateIdentity, ptr(0), true, false, "new", 2, "new", false}, {"matched updates", assignedUpdateIdentity, ptr(1), true, true, "changed", 1, "changed", false}, {"no-op still checks schema", assignedUpdateIdentity, ptr(999), true, false, strings.Repeat("x", 13), 1, "", true}} {
		t.Run(tc.name, func(t *testing.T) {
			db := sqlite.New(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO parents VALUES(1,'old')"); err != nil {
				t.Fatal(err)
			}
			in := &assignedInput{Rows: []*assignedRow{{ID: tc.id, Name: ptr(tc.value), Has: &assignedHas{ID: tc.marker, Name: true}}}}
			if tc.previous {
				in.CurrentRows = []*assignedRow{{ID: ptr(1), Name: ptr("old")}}
			}
			_, err := runSQLiteWriter(t, ctx, db, assignedComponent(tc.policy), in, &assignedOutput{}, "patch")
			if (err != nil) != tc.failure {
				t.Fatalf("err%v", err)
			}
			var count int
			if e := db.DB.QueryRow("SELECT COUNT(*) FROM parents").Scan(&count); e != nil || count != tc.count {
				t.Fatalf("count%d err%v", count, e)
			}
			if tc.stored != "" {
				var name string
				if tc.previous {
					if e := db.DB.QueryRow("SELECT name FROM parents WHERE id=1").Scan(&name); e != nil {
						t.Fatal(e)
					}
				} else {
					if e := db.DB.QueryRow("SELECT name FROM parents WHERE id<>1").Scan(&name); e != nil {
						t.Fatal(e)
					}
				}
				if name != tc.stored {
					t.Fatal(name)
				}
			}
		})
	}
}
func TestAssignedUpdateMetadataRestrictions(t *testing.T) {
	for _, operation := range []string{"post", "put"} {
		if _, err := New(assignedComponent(assignedUpdateIdentity), reflect.TypeFor[assignedInput](), reflect.TypeFor[assignedOutput](), operation); err == nil {
			t.Fatal("unsupported operation accepted", operation)
		}
	}
	component := assignedComponent("unknown")
	if _, err := New(component, reflect.TypeFor[assignedInput](), reflect.TypeFor[assignedOutput](), "patch"); err == nil {
		t.Fatal("unknown identity policy accepted")
	}
}

type assignedVersionHas struct{ ID, Name, Version bool }
type assignedVersionRow struct {
	ID      *int                `sqlx:"id,primaryKey=true"`
	Name    *string             `sqlx:"name"`
	Version *int                `sqlx:"version" writer:"concurrency"`
	Has     *assignedVersionHas `setMarker:"true" sqlx:"-"`
}
type assignedVersionInput struct {
	Rows        []*assignedVersionRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
	CurrentRows []*assignedVersionRow `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=parents"`
}
type assignedVersionOutput struct {
	Data []*assignedVersionRow `parameter:"Data,kind=output,in=body"`
}

func TestAssignedUpdatePreservesConcurrencyAndWriteErrors(t *testing.T) {
	for _, tc := range []struct {
		name             string
		id, version      int
		matched, trigger bool
		want             string
	}{{"matched stale token", 1, 2, true, false, "expected token differs"}, {"missing with supplied token", 999, 1, false, false, "unmatched identity cannot satisfy"}, {"matched actual update failure", 1, 1, true, true, "identity update failure"}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY,name TEXT,version INTEGER)", "INSERT INTO parents VALUES(1,'old',1)"); err != nil {
				t.Fatal(err)
			}
			if tc.trigger {
				if err := db.ExecStatements(ctx, "CREATE TRIGGER fail_identity AFTER UPDATE ON parents BEGIN SELECT RAISE(FAIL,'identity update failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			in := &assignedVersionInput{Rows: []*assignedVersionRow{{ID: ptr(tc.id), Name: ptr("changed"), Version: ptr(tc.version), Has: &assignedVersionHas{ID: true, Name: true, Version: true}}}}
			if tc.matched {
				in.CurrentRows = []*assignedVersionRow{{ID: ptr(1), Name: ptr("old"), Version: ptr(1)}}
			}
			_, err := runSQLiteWriter(t, ctx, db, assignedComponent(assignedUpdateIdentity), in, &assignedVersionOutput{}, "patch")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err%v", err)
			}
			var name string
			if e := db.DB.QueryRow("SELECT name FROM parents WHERE id=1").Scan(&name); e != nil || name != "old" {
				t.Fatal("failed update changed database")
			}
		})
	}
}
func TestAssignedUpdateMixedInsertDoesNotSequenceNoopIdentity(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO parents VALUES(1,'old')"); err != nil {
		t.Fatal(err)
	}
	missing := &assignedRow{ID: ptr(999), Name: ptr("none"), Has: &assignedHas{ID: true, Name: true}}
	insert := &assignedRow{Name: ptr("new"), Has: &assignedHas{Name: true}}
	in := &assignedInput{Rows: []*assignedRow{missing, insert}}
	if _, err := runSQLiteWriter(t, ctx, db, assignedComponent(assignedUpdateIdentity), in, &assignedOutput{}, "patch"); err != nil {
		t.Fatal(err)
	}
	if *missing.ID != 999 || insert.ID == nil || *insert.ID != 2 {
		t.Fatalf("missing%v insert%v", missing.ID, insert.ID)
	}
	var count int
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM parents WHERE id=999").Scan(&count); err != nil || count != 0 {
		t.Fatal("missing assigned row persisted")
	}
}
