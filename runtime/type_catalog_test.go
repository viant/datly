package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type catalogContextInput struct{}
type catalogContextOutput struct{ Found bool }
type catalogContextPredicate struct{}
type catalogContextOther struct{}

func TestNativeHandlerReceivesDetachedGenerationTypeCatalog(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[catalogContextPredicate](), x.WithPkgPath("example.com/linked"))); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/catalog", Name: "Catalog"}, Routes: []*spec.Route{{Method: "GET", Path: "/catalog"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[catalogContextInput](), OutputType: reflect.TypeFor[catalogContextOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	handler := rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
		current, ok, err := TypeCatalog(ctx)
		if err != nil || !ok {
			t.Fatalf("native handler catalog available=%v error=%v", ok, err)
		}
		_, found, err := current.ResolveRuntimeType(typecatalog.PackageAuthority, "example.com/linked.catalogContextPredicate")
		if err != nil || !found {
			t.Fatalf("linked predicate found=%v error=%v", found, err)
		}
		_, unwanted, err := current.ResolveRuntimeType(typecatalog.PackageAuthority, "example.com/linked.catalogContextOther")
		if err != nil || unwanted {
			t.Fatalf("request mutated runtime type catalog: found=%v error=%v", unwanted, err)
		}
		if err = current.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[catalogContextOther](), x.WithPkgPath("example.com/linked"))); err != nil {
			t.Fatal(err)
		}
		return &catalogContextOutput{Found: true}, nil
	})
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[catalogContextOutput](), Handler: handler}}, WithTypeCatalog(catalog))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())
	// Source changes after construction cannot alter the generation snapshot.
	if err = catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[catalogContextOther](), x.WithPkgPath("example.com/linked"))); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		request := testharness.NewRequest("GET", "/catalog")
		scope, scopeErr := request.Scope()
		if scopeErr != nil {
			t.Fatal(scopeErr)
		}
		actual, invokeErr := runtime.ExecuteRoute(context.Background(), "GET", "/catalog", scope)
		if invokeErr != nil || !actual.(*catalogContextOutput).Found {
			t.Fatalf("actual=%v error=%v", actual, invokeErr)
		}
	}
	if _, ok, err := TypeCatalog(context.Background()); err != nil || ok {
		t.Fatalf("bare context exposed catalog: available=%v error=%v", ok, err)
	}
}
