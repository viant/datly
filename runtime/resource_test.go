package runtime

import (
	"embed"
	"reflect"
	"testing"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
)

//go:embed testdata/query.sql
var testResources embed.FS

func TestRuntimePreservesPackageResourceFS(t *testing.T) {
	runtime, err := NewRuntime(nil, WithResourceFS("components", testResources))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	registered, ok := runtime.ResourceFS("components")
	if !ok {
		t.Fatal("ResourceFS(components) was not found")
	}
	if _, ok := registered.(embed.FS); !ok {
		t.Fatalf("ResourceFS(components) type = %T, want embed.FS", registered)
	}
	data, err := runtime.ReadResource("components:testdata/query.sql")
	if err != nil {
		t.Fatalf("ReadResource() error = %v", err)
	}
	if string(data) != "SELECT 1\n" {
		t.Fatalf("ReadResource() = %q", data)
	}
}

func TestArtifactAndRuntimeShareOneBindlyResourceStore(t *testing.T) {
	type row struct {
		Value int
	}
	type output struct {
		Data []row
	}
	resources := resource.New()
	if err := resources.Register("component", testResources); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: &spec.Component{
			RootView:   &spec.View{Name: "Query", Source: &spec.ViewSource{URI: "component:testdata/query.sql"}},
			Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
		},
		OutputType: reflect.TypeOf(output{}), Resources: resources,
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	runtime, err := NewRuntime(nil, WithResources(resources))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	if runtime.Resources() != resources || artifact.Reader.Root.View.Spec.Source.SQL != "SELECT 1\n" {
		t.Fatalf("resource authority diverged: runtime=%p source=%q", runtime.Resources(), artifact.Reader.Root.View.Spec.Source.SQL)
	}
}

func TestRuntimeUsesSuppliedSharedResourceStore(t *testing.T) {
	resources := resource.New()
	if err := resources.Register("components", testResources); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	runtime, err := NewRuntime(nil, WithResources(resources))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	if runtime.Resources() != resources {
		t.Fatal("runtime replaced the supplied resource store")
	}
	registered, ok := runtime.ResourceFS("components")
	if !ok {
		t.Fatal("ResourceFS(components) was not found")
	}
	if _, ok := registered.(embed.FS); !ok {
		t.Fatalf("ResourceFS(components) type = %T, want embed.FS", registered)
	}
}
