package bootstrap

import (
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	x "github.com/viant/x"
)

func TestArtifactCompilerUsesPackageTypeAuthority(t *testing.T) {
	type packageType struct{ Package bool }
	type dqlType struct{ DQL bool }
	catalog := typecatalog.NewCatalog()
	for _, registration := range []struct {
		origin typecatalog.TypeOrigin
		typ    reflect.Type
	}{
		{typecatalog.TypeOriginPackage, reflect.TypeOf(packageType{})},
		{typecatalog.TypeOriginDQL, reflect.TypeOf(dqlType{})},
	} {
		if err := catalog.Register(registration.origin, x.NewType(
			registration.typ, x.WithPkgPath("example.com/model"), x.WithName("Shared"),
		)); err != nil {
			t.Fatalf("Register(%s) error = %v", registration.origin, err)
		}
	}
	compiler := artifactCompiler{input: ArtifactInput{Types: catalog}}
	actual, err := compiler.lookupType("example.com/model.Shared")
	if err != nil || actual != reflect.TypeOf(packageType{}) {
		t.Fatalf("lookupType() = %v, %v; want package authority", actual, err)
	}
}

func TestBuildArtifactResolvesSQLFromSharedBindlyResourceStore(t *testing.T) {
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct {
		Data []row
	}
	resources := resource.New()
	if err := resources.Register("component", fstest.MapFS{
		"sql/users.sql": {Data: []byte("SELECT id FROM users")},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	artifact, err := BuildArtifact(ArtifactInput{
		Component: &spec.Component{
			RootView:   &spec.View{Name: "Users", Source: &spec.ViewSource{URI: "component:sql/users.sql"}},
			Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
		},
		OutputType: reflect.TypeOf(output{}), Resources: resources,
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	if artifact.Reader == nil || artifact.Reader.Root.View == nil || artifact.Reader.Root.View.Spec.Source == nil ||
		artifact.Reader.Root.View.Spec.Source.SQL != "SELECT id FROM users" || artifact.Reader.Root.View.Spec.Source.URI != "component:sql/users.sql" {
		t.Fatalf("reader source = %+v", artifact.Reader)
	}
}

func TestBuildArtifactCarriesCanonicalRouteInputContract(t *testing.T) {
	type input struct{ ID int }
	component := &spec.Component{
		Routes:     []*spec.Route{{Method: "GET", Path: "/items/{id}"}},
		Parameters: []*spec.Parameter{{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}}},
	}
	artifact, err := BuildArtifact(ArtifactInput{Component: component, InputType: reflect.TypeOf(input{})})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Input == nil || artifact.Input.Type() != reflect.TypeOf(input{}) {
		t.Fatalf("input contract = %+v", artifact.Input)
	}
	route, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/items/{id}"})
	if !ok || route.Plan() == nil || len(route.Fields()) != 1 || route.Fields()[0].Path() != "ID" {
		t.Fatalf("route input contract = (%+v, %v)", route, ok)
	}
}
