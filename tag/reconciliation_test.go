package tag

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestFiniteReconciliationTagRoundTrip(t *testing.T) {
	config := &spec.Reconciliation{Mode: "same-parent-root-first", RootFields: []string{"Total"}, Roles: []spec.ReconciliationRole{{Holder: "Entries", Fields: []string{"Title"}, AdoptIdentity: true}}}
	value, e := (View{Name: "Rows", Reconciliation: config}).Value()
	if e != nil {
		t.Fatal(e)
	}
	parsed, e := ParseView(value)
	if e != nil || !reflect.DeepEqual(parsed.Reconciliation, config) {
		t.Fatal(parsed, e)
	}
	if _, e = ParseView(value + ",finiteReconciliation=bad"); e == nil {
		t.Fatal("duplicate admitted")
	}
	if _, e = ParseView("Rows,finiteReconciliation=bad"); e == nil {
		t.Fatal("malformed admitted")
	}
}
