package bootstrap

import (
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestGoShapeComposeAndWithURIWithoutDQL(t *testing.T) {
	type input struct {
		ID *int `parameter:"ID,kind=path,in=id,uri=/{id},required=true" mcpEnabled:"true" pathMcpEnabled:"true"`
	}
	type output struct{}
	tag, present, err := dtag.ParseComponent(reflect.StructTag(`component:"Things,path=/things,method=GET,report=true,reportCompose=true,reportComposeMaxCubes=12" mcp:"[{\"kind\":\"tool\",\"name\":\"Things\"}]"`))
	if err != nil || !present {
		t.Fatalf("component tag: %v", err)
	}
	source := &RouteSource{PackagePath: "example.com/things", FieldName: "Things", Tag: tag}
	component, err := source.Resolve(reflect.TypeFor[input](), reflect.TypeFor[output]())
	if err != nil {
		t.Fatal(err)
	}
	if component.Settings.Report.Compose == nil || !component.Settings.Report.Compose.Enabled || component.Settings.Report.Compose.MaxCubes != 12 {
		t.Fatal("compose settings lost")
	}
	if len(component.Routes) != 2 || component.Routes[1].MCP[0].Name != "ThingsById" {
		t.Fatalf("routes=%+v", component.Routes)
	}
	artifact, err := BuildArtifact(ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output]()})
	if err != nil {
		t.Fatal(err)
	}
	base, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/things"})
	if !ok {
		t.Fatal("base contract missing")
	}
	alternate, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/things/{id}"})
	if !ok {
		t.Fatal("alternate contract missing")
	}
	if len(base.Fields()) != 0 || len(alternate.Fields()) != 1 {
		t.Fatalf("base=%d alternate=%d", len(base.Fields()), len(alternate.Fields()))
	}
	if len(artifact.Component.Routes) != 2 {
		t.Fatal("repeated compilation duplicated routes")
	}
}
