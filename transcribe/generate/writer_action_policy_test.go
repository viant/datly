package generate

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestWriterActionPolicyGenerationAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		mutate                  func(*spec.Component)
		mutation, pending, fail bool
	}{
		{"PATCH leaf", func(c *spec.Component) {}, true, false, false},
		{"pending discovery defers marker", func(c *spec.Component) { c.RootView.Columns = nil }, true, true, false},
		{"resolved requires marker", func(c *spec.Component) { c.RootView.Columns = nil }, true, false, true},
		{"reader", func(c *spec.Component) {}, false, false, true},
		{"unknown", func(c *spec.Component) { c.RootView.WriterActionPolicy = "unknown" }, true, false, true},
		{"POST", func(c *spec.Component) { c.Settings.Mutation = "post"; c.Routes[0].Method = "POST" }, true, false, true},
		{"PUT", func(c *spec.Component) { c.Settings.Mutation = "put"; c.Routes[0].Method = "PUT" }, true, false, true},
		{"auxiliary", func(c *spec.Component) { c.RootView.Auxiliary = true }, true, false, true},
		{"descendant", func(c *spec.Component) { c.RootView.Relations = []*spec.Relation{{Name: "Child"}} }, true, false, true},
		{"identity", func(c *spec.Component) { c.RootView.WriterIdentityPolicy = "assigned-update" }, true, false, true},
		{"predicate", func(c *spec.Component) { v := 1; c.RootView.MutationPredicateGroup = &v }, true, false, true},
		{"token", func(c *spec.Component) {
			c.RootView.Columns = append(c.RootView.Columns, &spec.Column{Name: "Version", ConcurrencyToken: true})
		}, true, false, true},
		{"independent", func(c *spec.Component) {
			c.RootView.WriterActionPolicy = ""
			c.Views = []*spec.View{{Name: "Side", WriterActionPolicy: "insert-delete", Columns: []*spec.Column{{Name: "Remove", DeleteMarker: true}}}}
		}, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &spec.Component{Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH"}}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: "insert-delete", Columns: []*spec.Column{{Name: "Remove", DeleteMarker: true}}}}
			tc.mutate(c)
			in := &Input{Component: c}
			err := in.validateLifecycleTarget(tc.mutation, tc.pending)
			if (err != nil) != tc.fail {
				t.Fatalf("error=%v expected failure=%v", err, tc.fail)
			}
		})
	}
}
