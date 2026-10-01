package validation_test

import (
	"context"
	"database/sql"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/validation"
	xhandler "github.com/viant/xdatly/handler"
	"strings"
	"testing"
)

type selectableChecksRow struct {
	ID       int    `sqlx:"id,primaryKey"`
	Name     string `sqlx:"name,unique,table=records"`
	Parent   int    `sqlx:"parent,refTable=parents,refColumn=id"`
	Required *int   `sqlx:"required,required"`
}

func TestValidationDatabaseCheckOptionsScalarAndBatch(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT UNIQUE,parent INTEGER,required INTEGER)", "INSERT INTO records VALUES(1,'stored',1,1)"); err != nil {
		t.Fatal(err)
	}
	service := dml.NewData(h.DB).FrameworkValidator()
	off, on := false, true
	for _, tc := range []struct {
		name         string
		unique, refs *bool
		want         int
	}{{"defaults", nil, nil, 3}, {"only refs", &off, &on, 2}, {"only unique", &on, &off, 2}, {"required retained", &off, &off, 1}} {
		for _, batch := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "scalar", true: "batch"}[batch], func(t *testing.T) {
				row := &selectableChecksRow{Name: "stored", Parent: 404}
				options := xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, CheckUnique: tc.unique, CheckRef: tc.refs}
				var value any = row
				var policy any = options
				if batch {
					value = []*selectableChecksRow{row}
					policy = []xhandler.ValidationOptions{options}
				}
				result, err := service.Validate(context.Background(), value, policy)
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Violations) != tc.want {
					t.Fatalf("got %+v", result)
				}
			})
		}
	}
	_, err := service.Validate(context.Background(), []*selectableChecksRow{{}, {}}, []xhandler.ValidationOptions{{Action: xhandler.WriteInsert, Shallow: true}, {Action: xhandler.WriteInsert, Shallow: true, CheckUnique: &off}})
	if err == nil || !strings.Contains(err.Error(), "same database checks") {
		t.Fatalf("mixed checks got %v", err)
	}
}

type mutatingCheckSource struct {
	db   *sql.DB
	flag *bool
}

func (s mutatingCheckSource) ValidationConnection(context.Context, string) (validation.Connection, error) {
	*s.flag = true
	return validation.Connection{DB: s.db}, nil
}
func TestValidationCheckOptionsSnapshotCallerPointers(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT UNIQUE,parent INTEGER,required INTEGER)", "INSERT INTO records VALUES(1,'stored',1,1)"); err != nil {
		t.Fatal(err)
	}
	for _, batch := range []bool{false, true} {
		off := false
		service := validation.New(mutatingCheckSource{db: h.DB, flag: &off})
		row := &selectableChecksRow{Name: "stored", Parent: 404}
		options := xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, CheckUnique: &off}
		var value any = row
		var policy any = options
		if batch {
			value = []*selectableChecksRow{row}
			policy = []xhandler.ValidationOptions{options}
		}
		result, err := service.Validate(context.Background(), value, policy)
		if err != nil {
			t.Fatal(err)
		}
		if !off || len(result.Violations) != 2 {
			t.Fatalf("caller pointer influenced native checks %+v", result)
		}
	}
}
