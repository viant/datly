package generate

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestWriterUpdateCommandGenerationAdmission(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*spec.Component)
		fail   bool
	}{
		{"leaf", func(*spec.Component) {}, false},
		{"auxiliary", func(c *spec.Component) { c.RootView.Auxiliary = true }, true},
		{"reader", func(c *spec.Component) { c.Settings.Mutation = "get"; c.Routes[0].Method = "GET" }, true},
		{"PUT", func(c *spec.Component) { c.Settings.Mutation = "put"; c.Routes[0].Method = "PUT" }, true},
		{"relation", func(c *spec.Component) { c.RootView.Relations = []*spec.Relation{{Name: "Child"}} }, true},
		{"delete", func(c *spec.Component) { c.RootView.Columns = []*spec.Column{{Name: "Remove", DeleteMarker: true}} }, true},
		{"token", func(c *spec.Component) {
			c.RootView.Columns = []*spec.Column{{Name: "Version", ConcurrencyToken: true}}
		}, true},
		{"identity", func(c *spec.Component) { c.RootView.WriterIdentityPolicy = "assigned-update" }, true},
		{"predicate", func(c *spec.Component) { c.RootView.MutationPredicateGroup = new(int) }, true},
		{"queue", func(c *spec.Component) { c.RootView.QueueContract = "source-row" }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := &spec.Component{Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH"}}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: "update"}}
			test.change(c)
			err := (&Input{Component: c}).validateLifecycleTarget(true, false)
			if (err != nil) != test.fail {
				t.Fatalf("admission: %v", err)
			}
		})
	}
}
