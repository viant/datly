package transcribe

import (
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"testing"
)

func TestExplicitDeleteWriterRouteCompatibility(t *testing.T) {
	marked := &spec.View{Columns: []*spec.Column{{Name: "Remove", DeleteMarker: true}}}
	auxiliary := &spec.View{Auxiliary: true, Columns: marked.Columns}
	for _, tc := range []struct {
		operation, method string
		view              *spec.View
		valid             bool
	}{
		{"patch", "PATCH", nil, true}, {"put", "PUT", nil, true}, {"patch", "DELETE", marked, true}, {"put", "DELETE", marked, true},
		{"post", "DELETE", marked, false}, {"patch", "DELETE", nil, false}, {"patch", "DELETE", auxiliary, false}, {"put", "POST", marked, false},
	} {
		if err := validateWriterRoute(tc.operation, tc.method, tc.view); (err == nil) != tc.valid {
			t.Errorf("%s/%s valid%v err%v", tc.operation, tc.method, tc.valid, err)
		}
	}
}

func TestDeleteTransportPreservesDeclaredWriterLifecyclePolicy(t *testing.T) {
	for _, operation := range []string{"patch", "put"} {
		component := &spec.Component{Settings: &spec.Settings{Mutation: operation}, Routes: []*spec.Route{{Method: "PATCH"}, {Method: "DELETE"}}, RootView: &spec.View{Name: "Records", EntityHooks: "RecordLifecycle", Columns: []*spec.Column{{Name: "Remove", DeleteMarker: true}}}}
		if operation == "put" {
			component.Routes[0].Method = "PUT"
		}
		input := gen.Input{Component: component}
		if err := input.ValidateLifecycleTarget(true); err != nil {
			t.Fatalf("valid %s deletion lifecycle rejected:%v", operation, err)
		}
		component.RootView.Columns[0].DeleteMarker = false
		if err := input.ValidateLifecycleTarget(true); err == nil {
			t.Fatal("DELETE with no deletion policy retained lifecycle")
		}
	}
}
