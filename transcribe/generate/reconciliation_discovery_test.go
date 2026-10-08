package generate

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestFiniteReconciliationDefersPartialWildcardFields(t *testing.T) {
	child := &spec.View{Name: "children", Columns: []*spec.Column{{Name: "NAME"}}}
	root := &spec.View{Name: "root", EntityHooks: "Lifecycle", Reconciliation: &spec.Reconciliation{Mode: "same-parent-root-first", Roles: []spec.ReconciliationRole{{Holder: "Children", Fields: []string{"ParentId"}}}}, Relations: []*spec.Relation{{Holder: "Children", View: child}}}
	input := &Input{Component: &spec.Component{Settings: &spec.Settings{Mutation: "patch"}, RootView: root}}
	if err := input.validateLifecycleTarget(true, true); err != nil {
		t.Fatal("partial wildcard discovery rejected", err)
	}
	if err := input.validateLifecycleTarget(true, false); err == nil {
		t.Fatal("final missing field admitted")
	}
	child.Columns = append(child.Columns, &spec.Column{Name: "PARENT_ID"})
	if err := input.validateLifecycleTarget(true, false); err != nil {
		t.Fatal("discovered parent field rejected", err)
	}
	child.Columns[1].PrimaryKey = true
	if err := input.validateLifecycleTarget(true, false); err == nil {
		t.Fatal("identity assignment admitted")
	}
	if err := input.validateLifecycleTarget(false, true); err == nil {
		t.Fatal("nonmutation target admitted")
	}
}
