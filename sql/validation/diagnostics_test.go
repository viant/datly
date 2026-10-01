package validation_test

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"strings"
	"testing"
)

func TestFrameworkCheckedFactsRemainPrivate(t *testing.T) {
	for _, testCase := range []struct {
		description string
		input       any
		expected    any
		location    string
		check       string
		field       string
	}{
		{description: "Go zero check", input: &goRow{}, expected: 0, location: "Payload[1]", field: "Value", check: "required"},
		{description: "SQL unique sensitive string", input: &row{Name: "private-store"}, expected: "private-store", location: "Payload[1].Children[0]", field: "Name", check: "unique"},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(context.Background(), "CREATE TABLE records(tenant_id INTEGER,id INTEGER,name TEXT UNIQUE,enabled BOOL,PRIMARY KEY(tenant_id,id))", "INSERT INTO records VALUES(1,1,'private-store',NULL)"); err != nil {
				t.Fatal(err)
			}
			if testCase.description == "Go zero check" {
				testCase.input = &struct {
					Value int `sqlx:"value" validate:"required"`
				}{}
			}
			result, err := dml.NewData(h.DB).FrameworkValidator().Validate(context.Background(), testCase.input, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, Location: testCase.location})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, violation := range result.Violations {
				if violation.Check != testCase.check {
					continue
				}
				found = true
				if !violation.HasCheckedValue || violation.CheckedValue != testCase.expected || violation.Location != testCase.location+"."+testCase.field {
					t.Fatalf("checked fact=%+v", violation)
				}
			}
			if !found {
				t.Fatalf("missing %q diagnostic:%+v", testCase.check, result)
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "private-store") || strings.Contains(string(encoded), "CheckedValue") {
				t.Fatalf("private fact exposed:%s", encoded)
			}
		})
	}
}
