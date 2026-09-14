package tag

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestSelectorSQLMethodsRoundTrip(t *testing.T) {
	for _, methods := range [][]spec.SQLMethod{{{Name: "lower", Args: []string{"string"}}}, {{Name: "abs", Args: []string{"float64"}}, {Name: "current_date"}}} {
		view := View{Name: "records", Selector: &spec.Selector{AllowCriteria: true, Filterable: []spec.FieldPath{"*"}, SQLMethods: methods}}
		encoded, err := view.Value()
		if err != nil {
			t.Fatal(err)
		}
		actual, err := ParseView(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual.Selector.SQLMethods, methods) {
			t.Fatalf("got=%+v want=%+v", actual.Selector.SQLMethods, methods)
		}
	}
}

func TestSelectorSQLMethodsRejectsMalformedDefinitions(t *testing.T) {
	for _, value := range []string{`[{"name":"x","unknown":true}]`, `[{"name":""}]`, `[{"name":"x"},{"name":"X"}]`, `[{"name":"x","args":[""]}]`, `[] []`} {
		if _, err := parseSQLMethods(value); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
}
