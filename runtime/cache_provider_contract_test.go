package runtime

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/handler/custom"
	cacheprovider "github.com/viant/datly/runtime/handler/provider/cache"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xcache "github.com/viant/xdatly/cache"
	xhandler "github.com/viant/xdatly/handler"
)

type cacheExampleInput struct {
	Name  string          `parameter:"Name,kind=const,in=CacheName"`
	Cache xcache.Provider `bind:"kind=cache,required"`
}
type cacheExampleOutput struct{ Value string }
type cacheExampleHandler struct{}

func (cacheExampleHandler) Exec(ctx context.Context, _ xhandler.Session, input *cacheExampleInput, output *cacheExampleOutput) error {
	backend, err := input.Cache.Cache(ctx, input.Name)
	if err != nil {
		return err
	}
	value, found, err := backend.Get(ctx, "key")
	if err != nil {
		return err
	}
	if !found {
		value = []byte("cached")
		if err := backend.Put(ctx, "key", value, time.Minute); err != nil {
			return err
		}
	}
	output.Value = string(value)
	return nil
}

func TestCacheProviderUsesHandlerDI(t *testing.T) {
	backend, err := cacheprovider.NewMemory(0)
	if err != nil {
		t.Fatal(err)
	}
	providers, err := cacheprovider.New(map[string]xcache.Cache{"context": backend})
	if err != nil {
		t.Fatal(err)
	}
	name := "context"
	component := componentSpec("CacheExample", "GET", "/cached", []*spec.Parameter{{Name: "Name", Source: spec.BindSource{Kind: "const", Name: "CacheName"}, Value: &name}})
	artifact := componentArtifact(t, component, reflect.TypeFor[cacheExampleInput](), reflect.TypeFor[cacheExampleOutput]())
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[cacheExampleOutput](), Handler: custom.New[cacheExampleInput, cacheExampleOutput](cacheExampleHandler{}), Providers: cacheprovider.Providers(providers)}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	result, err := executeTestRoute(t, rt, context.Background(), testharness.NewRequest("GET", "/cached"))
	if err != nil {
		t.Fatal(err)
	}
	if result.(*cacheExampleOutput).Value != "cached" {
		t.Fatalf("output=%#v", result)
	}
	value, found, err := backend.Get(context.Background(), "key")
	if err != nil || !found || string(value) != "cached" {
		t.Fatalf("injected backend did not receive write: %q %v %v", value, found, err)
	}
}
