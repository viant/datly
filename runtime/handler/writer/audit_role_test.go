package writer

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

type auditRoleHas struct{ ID, Name, Updated, UpdatedUser bool }
type auditRoleRow struct {
	ID          *int          `sqlx:"ID,primaryKey=true"`
	Name        *string       `sqlx:"NAME"`
	Updated     *time.Time    `sqlx:"UPDATED" writer:"audit"`
	UpdatedUser *int          `sqlx:"UPDATED_USER" writer:"audit"`
	Has         *auditRoleHas `setMarker:"true" sqlx:"-"`
}

func TestNoopAuditRequiresPreviousOnlyForMarkedOwnedFields(t *testing.T) {
	h, err := New(unitComponent(), reflect.TypeFor[auditRoleInput](), reflect.TypeFor[auditRoleOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	record := h.metadata.Root
	row := &auditRoleRow{ID: ptr(1), UpdatedUser: ptr(9), Has: &auditRoleHas{ID: true}}
	previous := &auditRoleRow{ID: ptr(1)}
	p := &Program{previousFields: map[*Record]fieldSet{record: {"ID": true}}}
	f := &Frame{Record: record, Entity: reflect.ValueOf(row), Previous: reflect.ValueOf(previous), Action: xhandler.WriteUpdate, Fields: livePresence(record, reflect.ValueOf(row).Elem())}
	if err := p.restoreNoopAudit(f); err != nil {
		t.Fatalf("unmarked audit required unloaded Previous: %v", err)
	}
	row.Has.UpdatedUser = true
	if err := p.restoreNoopAudit(f); err == nil {
		t.Fatal("marked audit accepted unloaded Previous")
	}
	record.Auxiliary = true
	if err := p.restoreNoopAudit(f); err != nil || *row.UpdatedUser != 9 || !row.Has.UpdatedUser {
		t.Fatalf("auxiliary lifecycle audit was altered: %v", err)
	}
}

func TestAuditRoleRejectsControlMetadata(t *testing.T) {
	for _, column := range []*spec.Column{
		{Name: "UPDATED", Source: "UPDATED", PrimaryKey: true},
		{Name: "UPDATED", Source: "UPDATED", ConcurrencyToken: true},
		{Name: "UPDATED", Source: "UPDATED", DeleteMarker: true},
	} {
		component := unitComponent()
		component.RootView.Columns = append(component.RootView.Columns, column)
		if _, err := New(component, reflect.TypeFor[auditRoleInput](), reflect.TypeFor[auditRoleOutput](), "patch"); err == nil {
			t.Fatalf("conflicting audit metadata accepted: %+v", column)
		}
	}
	for _, tag := range []reflect.StructTag{`sqlx:"-" writer:"audit"`, `sqlx:"ID,primaryKey=true" writer:"audit"`, `sqlx:"UPDATED" writer:"audit" invariant:"Window"`} {
		if err := validateAuditField(reflect.StructField{Name: "Value", Tag: tag}, nil); err == nil {
			t.Fatalf("conflicting audit tag accepted: %s", tag)
		}
	}
}

func TestUniversalWriterAuditSQLite(t *testing.T) {
	for _, tc := range []struct {
		name             string
		changed, command bool
	}{
		{"no physical update", false, false},
		{"business update includes audit", true, false},
		{"explicit command retains audit-only write", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := tc.changed
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx,
				"CREATE TABLE records(ID INTEGER PRIMARY KEY, NAME TEXT, UPDATED TEXT, UPDATED_USER INTEGER)",
				"CREATE TABLE writes(id INTEGER)",
				"INSERT INTO records VALUES(1,'stored','2026-01-01T00:00:00Z',3)",
				"CREATE TRIGGER record_updated AFTER UPDATE ON records BEGIN INSERT INTO writes VALUES(NEW.ID); END"); err != nil {
				t.Fatal(err)
			}
			oldTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			newTime := oldTime.Add(time.Hour)
			prev := &auditRoleRow{ID: ptr(1), Name: ptr("stored"), Updated: &oldTime, UpdatedUser: ptr(3)}
			row := &auditRoleRow{ID: ptr(1), Name: ptr("changed"), Updated: &newTime, UpdatedUser: ptr(4), Has: &auditRoleHas{ID: true, Name: changed, Updated: true, UpdatedUser: true}}
			component := unitComponent()
			if tc.command {
				component.RootView.WriterActionPolicy = updateActionPolicy
			}
			_, err := runSQLiteWriter(t, ctx, db, component, &auditRoleInput{Rows: []*auditRoleRow{row}, Current: []*auditRoleRow{prev}}, &auditRoleOutput{}, "patch")
			if err != nil {
				t.Fatal(err)
			}
			var writes, user int
			var name, stamp string
			if err = db.DB.QueryRow("SELECT COUNT(*) FROM writes").Scan(&writes); err != nil {
				t.Fatal(err)
			}
			if err = db.DB.QueryRow("SELECT NAME,UPDATED,UPDATED_USER FROM records WHERE ID=1").Scan(&name, &stamp, &user); err != nil {
				t.Fatal(err)
			}
			if changed || tc.command {
				wantName := "stored"
				if changed {
					wantName = "changed"
				}
				if writes != 1 || name != wantName || user != 4 || stamp == "2026-01-01T00:00:00Z" {
					t.Fatalf("business update: writes=%d name=%s stamp=%s user=%d", writes, name, stamp, user)
				}
			} else if writes != 0 || name != "stored" || user != 3 || stamp != "2026-01-01T00:00:00Z" {
				t.Fatalf("no-op changed storage: writes=%d name=%s stamp=%s user=%d", writes, name, stamp, user)
			}
		})
	}
}

type auditRoleInput struct {
	Rows    []*auditRoleRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=records"`
	Current []*auditRoleRow `parameter:"Current,kind=view,in=Current" view:"Current,table=records"`
}
type auditRoleOutput struct {
	Data []*auditRoleRow `parameter:"Data,kind=output,in=body"`
}

func TestUniversalWriterAuditDoesNotTriggerUpdate(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "audit only", true: "business and audit"}[changed], func(t *testing.T) {
			h, err := New(unitComponent(), reflect.TypeFor[auditRoleInput](), reflect.TypeFor[auditRoleOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			oldTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			newTime := oldTime.Add(time.Hour)
			prev := &auditRoleRow{ID: ptr(1), Name: ptr("stored"), Updated: &oldTime, UpdatedUser: ptr(3)}
			row := &auditRoleRow{ID: ptr(1), Name: ptr("changed"), Updated: &newTime, UpdatedUser: ptr(4), Has: &auditRoleHas{ID: true, Name: changed, Updated: true, UpdatedUser: true}}
			input := &auditRoleInput{Rows: []*auditRoleRow{row}, Current: []*auditRoleRow{prev}}
			snapshot, err := h.CaptureInput(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			caps := &fakeCapabilities{}
			if _, err = h.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: caps}); err != nil {
				t.Fatal(err)
			}
			if changed {
				if caps.updates != 1 || !row.Updated.Equal(newTime) || *row.UpdatedUser != 4 || !row.Has.Updated || !row.Has.UpdatedUser {
					t.Fatalf("real update lost audit: writes=%d row=%+v", caps.updates, row)
				}
			} else {
				if caps.updates != 0 {
					t.Fatalf("audit-only request produced %d updates", caps.updates)
				}
				if !row.Updated.Equal(oldTime) || *row.UpdatedUser != 3 || row.Has.Updated || row.Has.UpdatedUser {
					t.Fatalf("no-op response changed audit: %+v", row)
				}
			}
		})
	}
}
