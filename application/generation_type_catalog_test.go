package application

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type generationCatalogInput struct{}
type generationCatalogOutput struct{ Linked bool }
type generationCatalogPredicate struct{}

func TestPublishedGenerationSuppliesItsTypeAuthorityToNativeHandler(t *testing.T) {
	ctx := context.Background()
	manager, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(ctx)
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/generation", Name: "Catalog"}, Routes: []*spec.Route{{Method: "GET", Path: "/generation/catalog"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[generationCatalogInput](), OutputType: reflect.TypeFor[generationCatalogOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	handler := rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
		catalog, ok, err := druntime.TypeCatalog(ctx)
		if err != nil || !ok {
			t.Fatalf("published catalog available=%v error=%v", ok, err)
		}
		_, linked, err := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, "example.com/generation.generationCatalogPredicate")
		return &generationCatalogOutput{Linked: linked}, err
	})
	if err = manager.Reload(ctx, Request{Revision: 1, Compile: func(_ context.Context, types *typecatalog.Catalog) (*Build, error) {
		if err := types.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[generationCatalogPredicate](), x.WithPkgPath("example.com/generation"))); err != nil {
			return nil, err
		}
		return &Build{Components: []*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[generationCatalogOutput](), Handler: handler}}}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	actual, err := manager.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/generation/catalog"}}})
	if err != nil || !actual.(*generationCatalogOutput).Linked {
		t.Fatalf("actual=%v error=%v", actual, err)
	}
}
