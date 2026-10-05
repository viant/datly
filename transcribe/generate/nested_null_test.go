package generate

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestNestedNullPolicyLifecycleTargetRestrictions(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		mutation, root, auxiliary, one bool
		method, policy                 string
		fail                           bool
	}{
		{name: "writer child", mutation: true, method: "PATCH", policy: "initial-validation"},
		{name: "reader", method: "GET", policy: "initial-validation", fail: true},
		{name: "root", mutation: true, root: true, method: "PATCH", policy: "initial-validation", fail: true},
		{name: "auxiliary", mutation: true, auxiliary: true, method: "PATCH", policy: "initial-validation", fail: true},
		{name: "to-one", mutation: true, one: true, method: "PATCH", policy: "initial-validation", fail: true},
		{name: "invalid", mutation: true, method: "PATCH", policy: "invalid", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			child := &spec.View{Name: "Children", NestedNullPolicy: tc.policy, Auxiliary: tc.auxiliary}
			root := &spec.View{Name: "Rows", Relations: []*spec.Relation{{View: child}}}
			if tc.one {
				child.Cardinality = spec.CardinalityOne
			}
			if tc.root {
				root.NestedNullPolicy = tc.policy
				child.NestedNullPolicy = ""
			}
			input := &Input{Component: &spec.Component{RootView: root, Routes: []*spec.Route{{Method: tc.method}}}}
			if err := input.ValidateLifecycleTarget(tc.mutation); (err != nil) != tc.fail {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
