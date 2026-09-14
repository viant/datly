package validation_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type fields map[string]bool

func (f fields) Has(name string) bool { return f[name] }

type row struct {
	Tenant  int       `sqlx:"tenant_id,primaryKey"`
	ID      int       `sqlx:"id,primaryKey"`
	Name    string    `sqlx:"name,unique,table=records"`
	Enabled *bool     `sqlx:"enabled,required"`
	Has     *presence `sqlx:"presence=true"`
}
type presence struct{ Tenant, ID, Name, Enabled bool }
type goRow struct {
	ID   int    `sqlx:"id,primaryKey"`
	Name string `sqlx:"name" validate:"required"`
}

func TestFrameworkValidationSQLite(t *testing.T) {
	f := false
	tests := []struct {
		name         string
		value        *row
		options      xhandler.ValidationOptions
		check, error string
	}{
		{"insert explicit zero false", &row{Name: "new", Enabled: &f}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}, "", ""},
		{"insert null", &row{Name: "new"}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}, "notnull", ""},
		{"insert duplicate", &row{Name: "stored", Enabled: &f}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}, "unique", ""},
		{"update existing zero original key", &row{Tenant: 9, ID: 99, Name: "stored"}, xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Previous: &row{Tenant: 1, ID: 0}, PreviousFields: fields{"Tenant": true, "ID": true}, Fields: fields{"Name": true}, Shallow: true}, "", ""},
		{"backfilled null covered without persistence mark", &row{Has: &presence{}}, xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Previous: &row{Tenant: 1}, PreviousFields: fields{"Tenant": true, "ID": true}, Fields: fields{"Enabled": true}, Shallow: true}, "notnull", ""},
		{"missing prior key evidence", &row{}, xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Previous: &row{Tenant: 1}, PreviousFields: fields{"Tenant": true}, Fields: fields{}, Shallow: true}, "", "was not loaded"},
		{"missing update coverage", &row{}, xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Previous: &row{Tenant: 1}, PreviousFields: fields{"Tenant": true, "ID": true}, Shallow: true}, "", "explicit coverage"},
		{"unknown connector", &row{Enabled: &f}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Connector: "unknown", Shallow: true}, "", "not registered"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(tenant_id INTEGER,id INTEGER,name TEXT UNIQUE,enabled BOOL NOT NULL,PRIMARY KEY(tenant_id,id))", "INSERT INTO records VALUES(1,0,'stored',0)"); err != nil {
				t.Fatal(err)
			}
			data := dml.NewData(h.DB)
			validator := data.FrameworkValidator()
			marker := tc.value.Has
			result, err := validator.Validate(ctx, tc.value, tc.options)
			if tc.error != "" {
				if err == nil || !strings.Contains(err.Error(), tc.error) {
					t.Fatalf("got %v want %s", err, tc.error)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.check == "" {
				if result.Err() != nil {
					t.Fatal(result)
				}
			} else {
				if len(result.Violations) != 1 || result.Violations[0].Check != tc.check {
					t.Fatalf("got %+v", result)
				}
				var typed *xhandler.Validation
				if !errors.As(result.Err(), &typed) {
					t.Fatal("typed violation lost")
				}
				if strings.Contains(result.Error(), "stored") {
					t.Fatal("raw database value exposed")
				}
			}
			if tc.value.Has != marker {
				t.Fatal("marker replaced")
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT tenant_id,id,name,enabled FROM records"}, []row{{Tenant: 1, ID: 0, Name: "stored", Enabled: &f}})
		})
	}
}

func TestFrameworkGoValidationCoverage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields fields
		action xhandler.WriteAction
		want   string
		failed bool
	}{
		{"full required", nil, xhandler.WriteInsert, "", true},
		{"covered update tag", fields{"Name": true}, xhandler.WriteUpdate, "", true},
		{"omitted Go rule", fields{}, xhandler.WriteUpdate, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t)
			data := dml.NewData(h.DB)
			options := xhandler.ValidationOptions{Action: tc.action, Fields: tc.fields, Shallow: true, Location: "Rows[0]"}
			if tc.action == xhandler.WriteUpdate {
				options.Previous = &goRow{ID: 1}
				options.PreviousFields = fields{"ID": true}
			}
			result, err := data.FrameworkValidator().Validate(context.Background(), &goRow{}, options)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Failed != tc.failed || (tc.failed && (len(result.Violations) != 1 || result.Violations[0].Location != "Rows[0].Name")) || (!tc.failed && len(result.Violations) != 0) {
				t.Fatalf("got %+v", result)
			}
		})
	}
}

func TestFrameworkValidationCallerTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(tenant_id INTEGER,id INTEGER,name TEXT UNIQUE,enabled BOOL NOT NULL,PRIMARY KEY(tenant_id,id))"); err != nil {
		t.Fatal(err)
	}
	h.DB.SetMaxOpenConns(1)
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(1,0,'pending',0)"); err != nil {
		t.Fatal(err)
	}
	data := dml.NewData(h.DB, dml.WithTx(tx))
	f := false
	result, err := data.FrameworkValidator().Validate(ctx, &row{Name: "pending", Enabled: &f}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
	if err != nil || result.Err() == nil {
		t.Fatalf("got %v %v", result, err)
	}
	batchResult, err := data.FrameworkValidator().Validate(ctx, []*row{{Name: "available", Enabled: &f}, {Name: "pending", Enabled: &f}}, []xhandler.ValidationOptions{
		{Action: xhandler.WriteInsert, Shallow: true, Location: "Rows[0]"},
		{Action: xhandler.WriteInsert, Shallow: true, Location: "Rows[1]"},
	})
	if err != nil || !batchResult.Failed || len(batchResult.Violations) != 1 || batchResult.Violations[0].Location != "Rows[1].Name" {
		t.Fatalf("batch transaction visibility: %v %v", batchResult, err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(2,0,'still-owned',0)"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT tenant_id,id,name,enabled FROM records"}, []row{})
}
