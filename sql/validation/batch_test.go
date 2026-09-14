package validation_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/validation"
	xhandler "github.com/viant/xdatly/handler"
)

func TestFrameworkBatchValidationSQLite(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "mixed valid", true: "mixed duplicate"}[duplicate], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(tenant_id INTEGER,id INTEGER,name TEXT UNIQUE,enabled BOOL NOT NULL,PRIMARY KEY(tenant_id,id))", "INSERT INTO records VALUES(1,0,'stored',0)"); err != nil {
				t.Fatal(err)
			}
			f := false
			marker := &presence{}
			rows := []*row{{Tenant: 99, ID: 99, Name: "changed", Has: marker}, {Name: "new", Enabled: &f}}
			if duplicate {
				rows[1].Name = rows[0].Name
			}
			policies := []xhandler.ValidationOptions{
				{Action: xhandler.WriteUpdate, Previous: &row{Tenant: 1, ID: 0}, PreviousFields: fields{"Tenant": true, "ID": true}, Fields: fields{"Name": true}, Shallow: true, Location: "Input.Left[3]"},
				{Action: xhandler.WriteInsert, Shallow: true, Location: "Input.Right[1]"},
			}
			result, err := dml.NewData(h.DB).FrameworkValidator().Validate(ctx, rows, policies)
			if err != nil {
				t.Fatal(err)
			}
			if result.Failed != duplicate || len(result.Violations) != map[bool]int{false: 0, true: 2}[duplicate] {
				t.Fatalf("unexpected result: %+v", result)
			}
			if duplicate {
				locations := map[string]bool{}
				for _, v := range result.Violations {
					locations[v.Location] = true
					if v.Check != "unique" || strings.Contains(v.Message, "changed") {
						t.Fatalf("unexpected violation: %+v", v)
					}
				}
				if !reflect.DeepEqual(locations, map[string]bool{"Input.Left[3].Name": true, "Input.Right[1].Name": true}) {
					t.Fatalf("lost candidate locations: %v", locations)
				}
			}
			if rows[0].Has != marker || *marker != (presence{}) {
				t.Fatal("validation changed persistence presence")
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT tenant_id,id,name,enabled FROM records"}, []row{{Tenant: 1, ID: 0, Name: "stored", Enabled: &f}})
		})
	}
}

type noConnection struct{ calls int }

func (s *noConnection) ValidationConnection(context.Context, string) (validation.Connection, error) {
	s.calls++
	return validation.Connection{}, nil
}

func TestFrameworkBatchPreflight(t *testing.T) {
	insert := xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}
	for _, tc := range []struct {
		name     string
		rows     any
		policies []xhandler.ValidationOptions
		want     string
	}{
		{"non slice", &row{}, []xhandler.ValidationOptions{insert}, "typed entity slice"},
		{"interface slice", []any{&row{}}, []xhandler.ValidationOptions{insert}, "typed entity slice"},
		{"empty wrong count", []*row{}, []xhandler.ValidationOptions{insert}, "count"},
		{"empty wrong type", []int{}, nil, "typed entity slice"},
		{"nil candidate", []*row{{}, nil}, []xhandler.ValidationOptions{insert, insert}, "entity is required"},
		{"different connectors", []*row{{}, {}}, []xhandler.ValidationOptions{insert, {Action: xhandler.WriteInsert, Shallow: true, Connector: "other"}}, "same connector"},
		{"missing previous evidence", []*row{{}, {}}, []xhandler.ValidationOptions{insert, {Action: xhandler.WriteUpdate, Shallow: true, Previous: &row{}, Fields: fields{}}}, "read provenance"},
		{"unloaded previous keys", []*row{{}, {}}, []xhandler.ValidationOptions{insert, {Action: xhandler.WriteUpdate, Shallow: true, Previous: &row{}, PreviousFields: fields{}, Fields: fields{}}}, "was not loaded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &noConnection{}
			_, err := validation.New(source).Validate(context.Background(), tc.rows, tc.policies)
			if err == nil || !strings.Contains(err.Error(), tc.want) || source.calls != 0 {
				t.Fatalf("error=%v connection calls=%d", err, source.calls)
			}
		})
	}
	source := &noConnection{}
	result, err := validation.New(source).Validate(context.Background(), []*row(nil), []xhandler.ValidationOptions(nil))
	if err != nil || result.Failed || source.calls != 0 {
		t.Fatalf("empty batch=%v error=%v calls=%d", result, err, source.calls)
	}
}

func TestFrameworkBatchGoLocations(t *testing.T) {
	h := sqlite.New(t)
	result, err := dml.NewData(h.DB).FrameworkValidator().Validate(context.Background(), []*goRow{{}, {}}, []xhandler.ValidationOptions{
		{Action: xhandler.WriteInsert, Shallow: true, Location: "Root[7]"},
		{Action: xhandler.WriteInsert, Shallow: true, Location: "Child[2]"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Failed || len(result.Violations) != 2 || result.Violations[0].Location != "Root[7].Name" || result.Violations[1].Location != "Child[2].Name" {
		t.Fatalf("lost Go violation locations: %+v", result)
	}
}

type referenceRow struct {
	ID     int  `sqlx:"id,primaryKey"`
	Parent *int `sqlx:"parent,refTable=parents,refColumn=id"`
}

func TestFrameworkBatchRepeatedReferenceLocations(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	missing := 404
	result, err := dml.NewData(h.DB).FrameworkValidator().Validate(ctx,
		[]*referenceRow{{ID: 1, Parent: &missing}, {ID: 2, Parent: &missing}, {ID: 3, Parent: &missing}},
		[]xhandler.ValidationOptions{
			{Action: xhandler.WriteInsert, Shallow: true, Location: "Left[2]"},
			{Action: xhandler.WriteInsert, Shallow: true, Location: "Right[8]"},
			{Action: xhandler.WriteUpdate, Shallow: true, Location: "Omitted[0]", Previous: &referenceRow{ID: 3}, PreviousFields: fields{"ID": true}, Fields: fields{}},
		})
	if err != nil {
		t.Fatal(err)
	}
	locations := map[string]bool{}
	for _, violation := range result.Violations {
		locations[violation.Location] = true
		if violation.Check != "refKey" {
			t.Fatalf("unexpected violation: %+v", violation)
		}
	}
	if !result.Failed || len(result.Violations) != 2 || !reflect.DeepEqual(locations, map[string]bool{"Left[2].Parent": true, "Right[8].Parent": true}) {
		t.Fatalf("lost repeated-reference diagnostics: %v", locations)
	}
}
