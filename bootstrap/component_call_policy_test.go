package bootstrap

import (
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestPackageRoutePreservesComponentCallPolicy(t *testing.T) {
	for _, policy := range []string{"", "imperative", "buffered"} {
		holder := dtag.Component{Name: "Orders", Method: "POST", Path: "/orders", Settings: dtag.SettingsFromSpec(&spec.Settings{ComponentCallPolicy: policy})}
		rendered, err := holder.StructTag()
		if err != nil {
			t.Fatal(err)
		}
		parsed, found, err := dtag.ParseComponent(reflect.StructTag(rendered))
		if err != nil || !found {
			t.Fatalf("parse generated holder: %v %v", found, err)
		}
		source := &RouteSource{FieldName: "Orders", PackagePath: "example.com/orders", Tag: parsed}
		component, err := source.Resolve(reflect.TypeOf(struct{}{}), reflect.TypeOf(struct{}{}))
		if err != nil {
			t.Fatal(err)
		}
		actual := ""
		if component.Settings != nil {
			actual = component.Settings.ComponentCallPolicy
		}
		if actual != policy {
			t.Fatalf("bootstrap lost %q: %+v", policy, component.Settings)
		}
	}
}

func TestCanonicalComponentRejectsInvalidCallPolicy(t *testing.T) {
	for _, settings := range []*spec.Settings{{ComponentCallPolicy: "unknown"}, {ComponentCallPolicy: "buffered", IndependentChildTransactions: true}} {
		_, err := (ContractResolver{Component: &spec.Component{Settings: settings}}).Resolve()
		if err == nil {
			t.Fatalf("accepted canonical invalid policy %+v", settings)
		}
		source := &RouteSource{FieldName: "Orders", PackagePath: "example.com/orders", Tag: dtag.Component{Name: "Orders", Method: "POST", Path: "/orders", Settings: dtag.SettingsFromSpec(settings)}}
		if _, err = source.canonicalComponent(); err == nil {
			t.Fatalf("accepted authored invalid policy %+v", settings)
		}
	}
}
