package bootstrap

import (
	"fmt"
	"reflect"
	"testing"

	dtag "github.com/viant/datly/tag"
	"github.com/viant/x"
)

func TestGroupedRouteHoldersExpandWithURIOnce(t *testing.T) {
	type input struct {
		ID *int `parameter:"ID,kind=path,in=id,uri=/{id},required=true"`
	}
	type output struct{}
	for _, paths := range [][]string{{"/things"}, {"/things", "/things/{id}"}, {"/things/{id}", "/things"}} {
		t.Run(fmt.Sprint(paths), func(t *testing.T) {
			source := &PackageComponentSource{InputType: &x.Type{Type: reflect.TypeFor[input]()}, OutputType: &x.Type{Type: reflect.TypeFor[output]()}}
			for _, path := range paths {
				name := "things"
				if path != "/things" {
					name += "ById"
				}
				text := fmt.Sprintf("component:%q mcp:%q", "Things,path="+path+",method=GET", fmt.Sprintf(`[{"kind":"tool","name":%q}]`, name))
				tag, present, err := dtag.ParseComponent(reflect.StructTag(text))
				if err != nil || !present {
					t.Fatalf("component tag: %v", err)
				}
				source.Routes = append(source.Routes, &RouteSource{PackagePath: "example.com/things", FieldName: name, Tag: tag})
			}
			for attempt := 0; attempt < 2; attempt++ {
				component, err := source.ResolveDescriptors(nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(component.Routes) != 2 {
					t.Fatalf("resolved %d routes from %d holders", len(component.Routes), len(paths))
				}
				seen := map[string]bool{}
				for i, route := range component.Routes {
					if seen[route.Path] {
						t.Fatalf("duplicate route %s", route.Path)
					}
					seen[route.Path] = true
					if i < len(paths) && route.Path != paths[i] {
						t.Fatalf("holder order changed: %s != %s", route.Path, paths[i])
					}
					want := "things"
					if route.Path != "/things" {
						want += "ById"
					}
					if len(route.MCP) != 1 || route.MCP[0].Name != want {
						t.Fatalf("route exposure lost: %+v", route)
					}
				}
			}
		})
	}
}
