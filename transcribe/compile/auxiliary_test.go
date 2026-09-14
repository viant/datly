package compile

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestParenthesizedTableAuxiliaryIntent(t *testing.T) {
	for _, test := range []struct {
		source    string
		auxiliary bool
		table     string
	}{{"SELECT * FROM records", false, "records"}, {"SELECT * FROM (records)", true, "records"}, {"SELECT * FROM (schema.records)", true, "schema.records"}, {"SELECT * FROM (SELECT * FROM records) r", false, ""}} {
		t.Run(test.source, func(t *testing.T) {
			input := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: test.source}}
			actual, err := NewReader().Compile(ReadInput{View: input, SQL: test.source})
			if err != nil {
				t.Fatal(err)
			}
			if actual.Auxiliary != test.auxiliary || actual.Source.Table != test.table {
				t.Fatalf("view=%+v source=%+v", actual, actual.Source)
			}
			if input.Auxiliary || input.Source.SQL != test.source {
				t.Fatal("canonical source input mutated")
			}
			if actual.Clone().Auxiliary != actual.Auxiliary {
				t.Fatal("clone lost intent")
			}
		})
	}
	if _, found := reflect.TypeFor[data.View]().FieldByName("Auxiliary"); found {
		t.Fatal("mutation intent leaked into runtime view shape")
	}
}
