package validation_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/validation"
	xhandler "github.com/viant/xdatly/handler"
)

type checkAssociation struct {
	ID     int    `sqlx:"id,primaryKey"`
	Name   string `sqlx:"name,unique,table=associations"`
	Parent *int   `sqlx:"parent_id,required,refTable=parents,refColumn=id"`
	Label  string `sqlx:"label" validate:"required"`
}
type checkParent struct {
	ID int `sqlx:"id,primaryKey"`
}

func checkPolicy(selected fields) xhandler.ValidationOptions {
	off := false
	return xhandler.ValidationOptions{CheckOnly: true, Fields: selected, CheckUnique: &off, Shallow: true, Location: "Data[2].Associations[1]"}
}
func TestFrameworkCheckOnlyAssociatedValuesSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "INSERT INTO parents VALUES(1)", "CREATE TABLE associations(id INTEGER PRIMARY KEY,name TEXT UNIQUE,parent_id INTEGER NOT NULL,label TEXT)", "INSERT INTO associations VALUES(9,'stored',1,'ready')"); err != nil {
		t.Fatal(err)
	}
	validator := dml.NewData(h.DB).FrameworkValidator()
	one, zero, absent := 1, 0, 404
	for _, tc := range []struct {
		name     string
		row      checkAssociation
		selected fields
		want     string
	}{
		{"existing identity and duplicate name", checkAssociation{9, "stored", &one, "ready"}, fields{"Name": true, "Parent": true, "Label": true}, ""},
		{"unread identity", checkAssociation{77, "stored", &one, "ready"}, fields{"Name": true, "Parent": true}, ""},
		{"omitted reference", checkAssociation{}, fields{}, ""},
		{"selected null", checkAssociation{}, fields{"Parent": true}, "notnull"},
		{"selected zero", checkAssociation{Parent: &zero}, fields{"Parent": true}, "refKey"},
		{"selected absent", checkAssociation{Parent: &absent}, fields{"Parent": true}, "refKey"},
		{"selected Go field", checkAssociation{}, fields{"Label": true}, "required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validator.Validate(ctx, &tc.row, checkPolicy(tc.selected))
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == "" {
				if len(result.Violations) != 0 {
					t.Fatalf("unexpected %+v", result)
				}
			} else {
				if len(result.Violations) != 1 || result.Violations[0].Check != tc.want {
					t.Fatalf("got %+v want %s", result, tc.want)
				}
				if !strings.HasPrefix(result.Violations[0].Location, "Data[2].Associations[1].") {
					t.Fatalf("location %+v", result.Violations[0])
				}
			}
		})
	}
	off := false
	policy := checkPolicy(fields{"Parent": true})
	policy.CheckRef = &off
	result, err := validator.Validate(ctx, &checkAssociation{Parent: &absent}, policy)
	if err != nil || len(result.Violations) != 0 {
		t.Fatalf("reference toggle: %+v %v", result, err)
	}
	result, err = validator.Validate(ctx, &checkAssociation{}, policy)
	if err != nil || len(result.Violations) != 1 || result.Violations[0].Check != "notnull" {
		t.Fatalf("not-null suppressed: %+v %v", result, err)
	}
	var count int
	if err := h.DB.QueryRow("SELECT COUNT(*) FROM associations").Scan(&count); err != nil || count != 1 {
		t.Fatalf("checks mutated rows: %d %v", count, err)
	}
}

type checkConnection struct {
	value  validation.Connection
	calls  int
	before func()
}

func (c *checkConnection) ValidationConnection(context.Context, string) (validation.Connection, error) {
	c.calls++
	if c.before != nil {
		c.before()
	}
	return c.value, nil
}
func TestFrameworkCheckOnlyRejectsMutationPoliciesBeforeConnection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*xhandler.ValidationOptions)
	}{
		{"action", func(p *xhandler.ValidationOptions) { p.Action = xhandler.WriteInsert }},
		{"fields", func(p *xhandler.ValidationOptions) { p.Fields = nil }},
		{"implicit unique", func(p *xhandler.ValidationOptions) { p.CheckUnique = nil }},
		{"enabled unique", func(p *xhandler.ValidationOptions) { on := true; p.CheckUnique = &on }},
		{"previous", func(p *xhandler.ValidationOptions) { p.Previous = &checkAssociation{} }},
		{"previous fields", func(p *xhandler.ValidationOptions) { p.PreviousFields = fields{} }},
		{"presence", func(p *xhandler.ValidationOptions) { p.HonorPresence = true }},
		{"deferred", func(p *xhandler.ValidationOptions) { p.DeferredFields = fields{} }},
		{"satisfied", func(p *xhandler.ValidationOptions) {
			p.SatisfiedReferences = []xhandler.ValidationReference{{Field: "Parent", Table: "parents", Column: "id"}}
		}},
		{"recursive", func(p *xhandler.ValidationOptions) { p.Shallow = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &checkConnection{}
			p := checkPolicy(fields{})
			tc.change(&p)
			_, err := validation.New(source).Validate(context.Background(), &checkAssociation{}, p)
			if err == nil || source.calls != 0 {
				t.Fatalf("invalid policy reached connection: %v calls=%d", err, source.calls)
			}
		})
	}
	for _, policies := range [][]xhandler.ValidationOptions{{checkPolicy(fields{}), {Action: xhandler.WriteInsert, Shallow: true}}} {
		source := &checkConnection{}
		rows := make([]*checkAssociation, len(policies))
		for i := range rows {
			rows[i] = &checkAssociation{}
		}
		_, err := validation.New(source).Validate(context.Background(), rows, policies)
		if err == nil || !strings.Contains(err.Error(), "cannot mix check-only") || source.calls != 0 {
			t.Fatalf("batch admitted: %v", err)
		}
	}
}
func TestFrameworkCheckOnlySnapshotsCoverageAndFlags(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	selected := fields{"Parent": true}
	p := checkPolicy(selected)
	on := true
	p.CheckRef = &on
	source := &checkConnection{value: validation.Connection{DB: h.DB}, before: func() { selected["Parent"] = false; on = false }}
	absent := 404
	result, err := validation.New(source).Validate(ctx, &checkAssociation{Parent: &absent}, p)
	if err != nil || len(result.Violations) != 1 || result.Violations[0].Check != "refKey" {
		t.Fatalf("caller changed prepared checks: %+v %v", result, err)
	}
}
func TestFrameworkCheckOnlyUsesTransactionWithoutFlushing(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "PRAGMA journal_mode=WAL", "CREATE TABLE parents(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO parents VALUES(12)"); err != nil {
		t.Fatal(err)
	}
	data := dml.NewData(h.DB, dml.WithTx(tx))
	if err := data.Insert("parents", &checkParent{ID: 13}); err != nil {
		t.Fatal(err)
	}
	validator := data.FrameworkValidator()
	p := checkPolicy(fields{"Parent": true})
	executed, queued := 12, 13
	result, err := validator.Validate(ctx, &checkAssociation{Parent: &executed}, p)
	if err != nil || len(result.Violations) != 0 {
		t.Fatalf("uncommitted transaction row unseen: %+v %v", result, err)
	}
	result, err = validator.Validate(ctx, &checkAssociation{Parent: &queued}, p)
	if err != nil || len(result.Violations) != 1 || result.Violations[0].Check != "refKey" {
		t.Fatalf("queued row flushed or invented: %+v %v", result, err)
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM parents").Scan(&count); err != nil || count != 0 {
		t.Fatalf("transaction committed or outside observer saw rows: %d %v", count, err)
	}
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM parents").Scan(&count); err != nil || count != 1 {
		t.Fatalf("validator wrote extra rows: %d %v", count, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM parents").Scan(&count); err != nil || count != 0 {
		t.Fatalf("rollback lost: %d %v", count, err)
	}
}
func TestFrameworkCheckOnlyFailuresAndTypedNil(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	validator := dml.NewData(h.DB).FrameworkValidator()
	p := checkPolicy(fields{})
	p.Previous = (*checkAssociation)(nil)
	p.PreviousFields = fields(nil)
	if _, err := validator.Validate(ctx, &checkAssociation{}, p); err != nil {
		t.Fatalf("typed nil inconsistent: %v", err)
	}
	p.Connector = "unknown"
	if _, err := validator.Validate(ctx, &checkAssociation{}, p); err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("unknown connector %v", err)
	}
	p.Connector = ""
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := validator.Validate(canceled, &checkAssociation{}, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation %v", err)
	}
	if _, err := validator.Validate(ctx, "not a row", p); err == nil {
		t.Fatal("invalid shape admitted")
	}
	absent := 404
	p = checkPolicy(fields{"Parent": true})
	if _, err := validator.Validate(ctx, &checkAssociation{Parent: &absent}, p); err == nil {
		t.Fatal("missing reference table error discarded")
	}
}
