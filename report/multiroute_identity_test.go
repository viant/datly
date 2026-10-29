package report

import (
	"reflect"
	"testing"

	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestCubeFacadesDisambiguateRepeatedRouteNames(t *testing.T) {
	for _, names := range [][2]string{{"Events", "Events"}, {"event-total", "event_total"}} {
		t.Run(names[0]+"/"+names[1], func(t *testing.T) {
			source := reportSource(t, &spec.ReportSettings{Enabled: true})
			source.Component.Routes = []*spec.Route{{Method: "GET", Path: "/records/101", Name: names[0], Internal: true}, {Method: "GET", Path: "/records/202", Name: names[1], Internal: true}}
			compile := func() map[string]string {
				t.Helper()
				input, err := handlercompiler.New(handlercompiler.Input{Component: source.Component, InputType: reflect.TypeFor[reportSourceInput]()}).Compile()
				if err != nil {
					t.Fatal(err)
				}
				source.Input = input.Input
				project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
				if err != nil {
					t.Fatal(err)
				}
				if len(project.Derived()) != 2 {
					t.Fatalf("facade count %d", len(project.Derived()))
				}
				identities := map[string]string{}
				seen := map[string]bool{}
				for _, facade := range project.Derived() {
					target := facade.Plan.Target().Route.Path
					if facade.Component.Routes[0].Path != target+"/cube" || !facade.Component.Routes[0].Internal {
						t.Fatalf("route authority changed: %+v", facade.Component.Routes[0])
					}
					key := facade.Component.Key.String()
					if seen[key] {
						t.Fatal("duplicate facade key", key)
					}
					seen[key] = true
					identities[target] = key
				}
				return identities
			}
			first := compile()
			source.Component.Routes[0], source.Component.Routes[1] = source.Component.Routes[1], source.Component.Routes[0]
			if reversed := compile(); !reflect.DeepEqual(first, reversed) {
				t.Fatalf("route order changes identity: %v vs %v", first, reversed)
			}
		})
	}
}

func TestCubeDistinctRouteNamesRetainExistingIdentity(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "Records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records/101", Name: "Daily"}, {Method: "GET", Path: "/records/202", Name: "Monthly"}}}
	d := newReportDeriver(nil)
	for i, want := range []string{"RecordsDailyCube", "RecordsMonthlyCube"} {
		got, err := d.derivedIdentity(component, component.Routes[i], 2)
		if err != nil || got.key.Name != want {
			t.Fatalf("identity %+v error %v want %s", got, err, want)
		}
	}
}
