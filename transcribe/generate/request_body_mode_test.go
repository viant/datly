package generate

import (
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestRequestBodyModeGenerationReload(t *testing.T) {
	for _, mode := range []string{"", "eager", "on_demand"} {
		t.Run(mode, func(t *testing.T) {
			routes, _, err := resolveRoutes([]*spec.Route{{Method: "DELETE", Path: "/views/{id}", RequestBodyMode: mode}})
			if err != nil {
				t.Fatal(err)
			}
			plan := &Plan{ComponentName: "Delete", Package: "example/delete"}
			raw, err := plan.componentTag(routes[0]).StructTag()
			if err != nil {
				t.Fatal(err)
			}
			tag, present, err := dtag.ParseComponent(reflect.StructTag(raw))
			if err != nil || !present {
				t.Fatalf("tag: %v %v", present, err)
			}
			source := &bootstrap.RouteSource{PackagePath: "example/delete", Tag: tag}
			component, err := source.Resolve(reflect.TypeOf(struct{}{}), reflect.TypeOf(struct{}{}))
			if err != nil {
				t.Fatal(err)
			}
			if len(component.Routes) != 1 || component.Routes[0].RequestBodyMode != mode {
				t.Fatalf("lost route mode: %+v", component.Routes)
			}
		})
	}
}
