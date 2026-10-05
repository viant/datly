package generate

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestRootNullPolicyLifecycleTargetRestrictions(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		mutation, auxiliary, one, child bool
		policy, method                  string
		fail                            bool
	}{
		{name: "writer", mutation: true, policy: "initial-validation", method: "PATCH"},
		{name: "reader", policy: "initial-validation", method: "GET", fail: true},
		{name: "unsupported route", mutation: true, policy: "initial-validation", method: "GET", fail: true},
		{name: "auxiliary", mutation: true, auxiliary: true, policy: "initial-validation", method: "PATCH", fail: true},
		{name: "to-one root", mutation: true, one: true, policy: "initial-validation", method: "PATCH", fail: true},
		{name: "nested", mutation: true, child: true, policy: "initial-validation", method: "PATCH", fail: true},
		{name: "unknown policy", mutation: true, policy: "invalid", method: "PATCH", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := &spec.View{Name: "Rows", RootNullPolicy: tc.policy, Auxiliary: tc.auxiliary}
			if tc.one {
				root.Cardinality = spec.CardinalityOne
			}
			if tc.child {
				root.RootNullPolicy = ""
				root.Relations = []*spec.Relation{{View: &spec.View{Name: "Children", RootNullPolicy: tc.policy}}}
			}
			input := &Input{Component: &spec.Component{RootView: root, Routes: []*spec.Route{{Method: tc.method}}}}
			err := input.ValidateLifecycleTarget(tc.mutation)
			if (err != nil) != tc.fail {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
