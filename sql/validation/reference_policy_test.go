package validation_test

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/validation"
	xhandler "github.com/viant/xdatly/handler"
)

type referencePolicyRow struct {
	ID     int    `sqlx:"id,primaryKey"`
	Parent *int   `sqlx:"parent,required,unique,table=records,refTable=parents,refColumn=id"`
	Other  *int   `sqlx:"other,refTable=parents,refColumn=id"`
	Name   string `sqlx:"name" validate:"required"`
}

func TestFrameworkSatisfiedReferencesSQLite(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, tc := range []struct {
			name         string
			check, field string
			stored       bool
		}{
			{name: "exact receipt"},
			{name: "unrelated reference", check: "refKey", field: "Other"},
			{name: "null still checked", check: "notnull", field: "Parent"},
			{name: "unique still checked", check: "unique", field: "Parent", stored: true},
			{name: "Go still checked", check: "required", field: "Name"},
		} {
			mode := "scalar/"
			if batch {
				mode = "batch/"
			}
			t.Run(mode+tc.name, func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				h.DB.SetMaxOpenConns(1)
				if err := h.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY,parent INTEGER NOT NULL UNIQUE REFERENCES parents(id),other INTEGER,name TEXT)"); err != nil {
					t.Fatal(err)
				}
				if tc.stored {
					if err := h.ExecStatements(ctx, "INSERT INTO parents VALUES(77)", "INSERT INTO records VALUES(9,77,NULL,'stored')"); err != nil {
						t.Fatal(err)
					}
				}
				tx, err := h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				parent, other := 77, 88
				row := &referencePolicyRow{ID: 1, Parent: &parent, Name: "ready"}
				switch tc.field {
				case "Other":
					row.Other = &other
				case "Name":
					row.Name = ""
				case "Parent":
					if !tc.stored {
						row.Parent = nil
					}
				}
				policy := xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, Location: "Child[2]", SatisfiedReferences: []xhandler.ValidationReference{{Field: "Parent", Table: "parents", Column: "id"}}}
				var value any = row
				var options any = policy
				if batch {
					value = []*referencePolicyRow{row}
					options = []xhandler.ValidationOptions{policy}
				}
				result, err := dml.NewData(h.DB, dml.WithTx(tx)).FrameworkValidator().Validate(ctx, value, options)
				if err != nil {
					t.Fatal(err)
				}
				if tc.check == "" {
					if result.Failed {
						t.Fatal(result)
					}
				} else if !result.Failed || len(result.Violations) != 1 || result.Violations[0].Check != tc.check || result.Violations[0].Location != "Child[2]."+tc.field {
					t.Fatalf("unexpected result: %+v", result)
				}
				if tc.check == "" {
					if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(1,77,NULL,'child')"); err == nil {
						t.Fatal("validation receipt disabled the actual database foreign key")
					}
				}
				if _, err = tx.ExecContext(ctx, "INSERT INTO parents VALUES(99)"); err != nil {
					t.Fatal("validator completed the caller transaction:", err)
				}
				if err = tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestFrameworkReferencePolicyPreflight(t *testing.T) {
	valid := xhandler.ValidationReference{Field: "Parent", Table: "parents", Column: "id"}
	for _, refs := range [][]xhandler.ValidationReference{
		{{Field: "Parent", Table: "wrong", Column: "id"}},
		{{Field: "Parent", Schema: "wrong", Table: "parents", Column: "id"}},
		{{Field: "Parent", Table: "parents", Column: "wrong"}},
		{{Field: "Name", Table: "parents", Column: "id"}},
		{valid, valid},
	} {
		source := &noConnection{}
		_, err := validation.New(source).Validate(context.Background(), &referencePolicyRow{}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, SatisfiedReferences: refs})
		if err == nil || source.calls != 0 {
			t.Fatalf("invalid receipts=%v error=%v source calls=%d", refs, err, source.calls)
		}
	}
	h := sqlite.New(t)
	_, err := dml.NewData(h.DB).FrameworkValidator().Validate(context.Background(), &referencePolicyRow{}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, SatisfiedReferences: []xhandler.ValidationReference{valid}})
	if err == nil || !strings.Contains(err.Error(), "active mutation transaction") {
		t.Fatalf("receipt outside final transaction: %v", err)
	}
}
