package transcribe

import (
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"strings"
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

func TestDeleteStagingRejectsAuxiliaryOnlyAndExplicitInconsistentPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, policy string
		aux          bool
	}{
		{name: "auxiliary marker only", aux: true}, {name: "explicit post", policy: "post"}, {name: "explicit get", policy: "get"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{Settings: &spec.Settings{Mutation: tc.policy}, Routes: []*spec.Route{{Method: "DELETE"}}, RootView: &spec.View{Name: "Records", Auxiliary: tc.aux, EntityHooks: "Lifecycle", Columns: []*spec.Column{{Name: "Remove", DeleteMarker: true}}}}
			if err := (&gen.Input{Component: component}).ValidateLifecycleTarget(true); err == nil {
				t.Fatal("inconsistent staged DELETE policy accepted")
			}
		})
	}
}

func TestWriterRouteCompatibilityMatrix(t *testing.T) {
	marked := &spec.View{Columns: []*spec.Column{{Name: "Remove", DeleteMarker: true}}}
	auxiliary := &spec.View{Auxiliary: true, Columns: marked.Columns}
	for _, operation := range []string{"get", "post", "patch", "put"} {
		for _, method := range []string{"", "GET", "POST", "PATCH", "PUT", "DELETE", "HEAD", "OPTIONS", "patch"} {
			for _, view := range []*spec.View{nil, marked, auxiliary} {
				expected := operation == "get" || method == "" || strings.EqualFold(operation, method) || (operation == "post" && strings.EqualFold(method, "PATCH")) || (operation == "patch" && strings.EqualFold(method, "PUT")) || ((operation == "patch" || operation == "put") && method == "DELETE" && view == marked)
				if err := validateWriterRoute(operation, method, view); (err == nil) != expected {
					t.Errorf("operation=%s method=%s view=%p expected=%v err=%v", operation, method, view, expected, err)
				}
			}
		}
	}
}
