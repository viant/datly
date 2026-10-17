package dml

import (
	"context"
	"errors"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

type bufferedParentRow struct {
	ID int `sqlx:"id,primaryKey"`
}
type bufferedChildRow struct {
	ID       int `sqlx:"id,primaryKey"`
	ParentID int `sqlx:"parent_id,required,refTable=parents,refColumn=id"`
}

// Separate components share one buffer. Validation must neither flush the
// earlier parent nor waive a reference supplied by a different owner or key.
func TestBufferedSiblingInsertReference(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		change                  string
		batch, invalid, guarded bool
	}{
		{name: "commit"},
		{name: "accounting update", change: "accounting"},
		{name: "composite update omits reference", change: "composite"},
		{name: "same table update omits reference", change: "sameTable"},
		{name: "batch", batch: true},
		{name: "mixed batch", batch: true, change: "mixed", invalid: true},
		{name: "unrelated allocated child ID", change: "childID"},
		{name: "wrong key", change: "wrongKey", invalid: true},
		{name: "wrong table", change: "wrongTable", invalid: true},
		{name: "later binding parent", change: "binding", invalid: true},
		{name: "other owner", change: "owner", invalid: true},
		{name: "both keys changed", change: "both", guarded: true},
		{name: "parent key changed", change: "parent", guarded: true},
		{name: "parent deleted", change: "delete", guarded: true},
		{name: "opaque SQL appended", change: "execute", guarded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE parents(id INTEGER PRIMARY KEY, name TEXT, code TEXT UNIQUE DEFAULT 'seed')", "CREATE TABLE other_parents(id INTEGER PRIMARY KEY)", "CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parents(id))", "CREATE TABLE settings(tenant INTEGER, setting INTEGER, value TEXT, PRIMARY KEY(tenant,setting))", "INSERT INTO settings VALUES(1,2,'old')"); err != nil {
				t.Fatal(err)
			}
			owner := NewData(h.DB)
			if err := owner.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			defer owner.Complete(ctx, errors.New("test rollback"))
			if err := owner.Start(ctx); err != nil {
				t.Fatal(err)
			}
			parent := &bufferedParentRow{7}
			relation := ComponentImperative
			if tc.change == "binding" {
				relation = ComponentBinding
			}
			first := owner.ComponentData(relation, "").(*Data)
			if tc.change == "owner" {
				first = NewData(h.DB)
				if err := first.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				defer first.Complete(ctx, errors.New("test rollback"))
			}
			table := "parents"
			if tc.change == "wrongTable" {
				table = "other_parents"
			}
			if err := first.Insert(table, parent); err != nil {
				t.Fatal(err)
			}
			first.SealComponent()
			second := owner.ComponentData(ComponentImperative, "").(*Data)
			if tc.change == "accounting" {
				if err := first.parent.Update("parents", &struct {
					ID   int    `sqlx:"id,primaryKey"`
					Name string `sqlx:"name"`
				}{7, "accounting"}); err != nil {
					t.Fatal(err)
				}
			}
			child := &bufferedChildRow{11, 7}
			if tc.change == "wrongKey" {
				child.ParentID = 8
			}
			var payload any = child
			var policy any = xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}
			if tc.batch {
				payload = []*bufferedChildRow{child}
				policy = []xhandler.ValidationOptions{{Action: xhandler.WriteInsert, Shallow: true}}
			}
			if tc.change == "mixed" {
				payload = []*bufferedChildRow{child, {ID: 12, ParentID: 8}}
				policy = []xhandler.ValidationOptions{{Action: xhandler.WriteInsert, Shallow: true, Location: "Rows[0]"}, {Action: xhandler.WriteInsert, Shallow: true, Location: "Rows[1]"}}
			}
			if _, err := second.FrameworkValidator().Validate(nil, payload, policy); err == nil {
				t.Fatal("nil context accepted")
			}
			result, err := second.FrameworkValidator().Validate(ctx, payload, policy)
			if err != nil {
				t.Fatal(err)
			}
			var count int
			if err := owner.tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM parents").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("validation flushed %d parents", count)
			}
			if result.Failed != tc.invalid {
				t.Fatalf("validation failed=%v want %v: %+v", result.Failed, tc.invalid, result.Violations)
			}
			if tc.change == "mixed" {
				for _, v := range result.Violations {
					if v.Location != "Rows[1].ParentID" {
						t.Fatalf("wrong candidate location: %s", v.Location)
					}
				}
			}
			if tc.invalid {
				return
			}
			switch tc.change {
			case "composite":
				if err := second.Update("settings", &struct {
					Tenant  int    `sqlx:"tenant,primaryKey"`
					Setting int    `sqlx:"setting,primaryKey"`
					Value   string `sqlx:"value"`
				}{1, 2, "changed"}); err != nil {
					t.Fatal(err)
				}
			case "sameTable":
				if err := second.Update("parents", &struct {
					Code string `sqlx:"code,primaryKey"`
					Name string `sqlx:"name"`
				}{"seed", "changed"}); err != nil {
					t.Fatal(err)
				}
			case "both":
				parent.ID = 8
				child.ParentID = 8
			case "parent":
				parent.ID = 8
			case "childID":
				child.ID = 12
			case "delete":
				if err := second.Delete("parents", parent); err != nil {
					t.Fatal(err)
				}
			case "execute":
				if err := second.Execute("DELETE FROM parents"); err != nil {
					t.Fatal(err)
				}
			}
			if err := second.Insert("children", child); err != nil {
				t.Fatal(err)
			}
			second.SealComponent()
			if tc.guarded {
				if guardErr := owner.ValidateExecutionGuards(ctx); guardErr == nil {
					t.Fatal("invalid reference guard passed")
				}
				// A caller catching the guard error cannot subsequently commit.
			}
			err = owner.Complete(ctx, nil)
			if tc.guarded {
				if err == nil {
					t.Fatal("invalidated proof committed")
				}
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM parents"}, []struct{ Total int }{{0}})
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM children"}, []struct{ Total int }{{1}})
			if tc.change == "composite" {
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT value FROM settings"}, []struct{ Value string }{{"changed"}})
			}
			if tc.change == "sameTable" {
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT name FROM parents"}, []struct{ Name string }{{"changed"}})
			}
		})
	}
}

func TestBufferedReferencePreservesOtherConstraints(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	if err := h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER, other_id INTEGER, name TEXT)", "INSERT INTO children VALUES(11,0,0,'taken')"); err != nil {
		t.Fatal(err)
	}
	owner := NewData(h.DB)
	if err := owner.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	defer owner.Complete(ctx, errors.New("test rollback"))
	if err := owner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	first := owner.ComponentData(ComponentImperative, "").(*Data)
	if err := first.Insert("parents", &bufferedParentRow{7}); err != nil {
		t.Fatal(err)
	}
	first.SealComponent()
	second := owner.ComponentData(ComponentImperative, "").(*Data)
	type constrainedChild struct {
		ID       int     `sqlx:"id,primaryKey,unique,table=children"`
		ParentID int     `sqlx:"parent_id,refTable=parents,refColumn=id"`
		OtherID  int     `sqlx:"other_id,refTable=parents,refColumn=id"`
		Name     *string `sqlx:"name,required"`
	}
	child := &constrainedChild{ID: 11, ParentID: 7, OtherID: 99}
	result, err := second.FrameworkValidator().Validate(ctx, child, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]bool{}
	for _, v := range result.Violations {
		fields[v.Field] = true
	}
	for _, field := range []string{"ID", "OtherID", "Name"} {
		if !fields[field] {
			t.Errorf("lost %s constraint: %+v", field, fields)
		}
	}
	if fields["ParentID"] {
		t.Fatal("buffered parent still rejected")
	}
}

func TestBufferedReferenceTerminalGuard(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	if err := h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	owner := NewData(h.DB)
	if err := owner.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	defer owner.Complete(ctx, errors.New("test rollback"))
	if err := owner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	parent := &bufferedParentRow{7}
	first := owner.ComponentData(ComponentImperative, "").(*Data)
	if err := first.Insert("parents", parent); err != nil {
		t.Fatal(err)
	}
	first.SealComponent()
	second := owner.ComponentData(ComponentImperative, "").(*Data)
	child := &bufferedChildRow{11, 7}
	result, err := second.FrameworkValidator().Validate(ctx, child, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
	if err != nil || result.Failed {
		t.Fatalf("validation=%+v err=%v", result, err)
	}
	parent.ID = 8
	child.ParentID = 8
	owner.executionMu.Lock()
	err = owner.validateExecutionGuardsLocked(ctx, true, true)
	owner.executionMu.Unlock()
	if err == nil {
		t.Fatal("extra terminal check omitted reference guard")
	}
}

func TestBufferedReferenceUpdateMapping(t *testing.T) {
	type mutable struct {
		ID   int    `sqlx:"id,primaryKey"`
		Code string `sqlx:"code"`
	}
	type immutable struct {
		ID   int    `sqlx:"id,primaryKey"`
		Code string `sqlx:"code,primaryKey"`
		Name string `sqlx:"name"`
	}
	if updatePreservesReference(&mutable{ID: 1, Code: "changed"}, "code") {
		t.Fatal("mutable natural reference accepted")
	}
	if !updatePreservesReference(&immutable{ID: 1, Code: "fixed", Name: "changed"}, "code") {
		t.Fatal("WHERE-only reference rejected")
	}
}

func TestBufferedReferenceRejectsDuplicateUpdateColumns(t *testing.T) {
	type duplicate struct {
		Key     string `sqlx:"code,primaryKey"`
		NewCode string `sqlx:"code"`
	}
	type alias struct {
		Key     string `sqlx:"code,primaryKey"`
		NewCode string `sqlx:"CODE"`
	}
	for _, row := range []any{&duplicate{}, &alias{}} {
		if updatePreservesReference(row, "code") {
			t.Fatal("ambiguous SET/WHERE reference accepted")
		}
	}
}
