package runtime

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/remote"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

type remoteFamilyLoader struct{ component *registry.RegisteredComponent }

func (l remoteFamilyLoader) LoadComponent(_ context.Context, key spec.Key) (*registry.RegisteredComponent, error) {
	if l.component.Component.Key == key {
		return l.component, nil
	}
	return nil, nil
}

func (l remoteFamilyLoader) LoadComponents(_ context.Context, _ spec.Key) ([]*registry.RegisteredComponent, error) {
	return []*registry.RegisteredComponent{l.component}, nil
}

func TestLazyFamilyBindsStaticRemoteHandlerOnce(t *testing.T) {
	configuration := remoteAuthConfig("https://example.test")
	component := componentSpec("LazyRemote", http.MethodGet, "/lazy-remote", []*spec.Parameter{
		{Name: "Token", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: boolValue(true)},
		{Name: "Remote", Source: spec.BindSource{Kind: "const", Name: "Remote"}, Value: &configuration},
	})
	artifact := componentArtifact(t, component, reflect.TypeFor[remote.Input](), reflect.TypeFor[remoteAuthOutput]())
	contract := &remote.Handler[remoteAuthOutput]{}
	registered := &registry.RegisteredComponent{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[remoteAuthOutput](),
		Handler: custom.New(contract),
	}
	rt, err := NewIndexedRuntime([]*spec.Component{artifact.Component}, nil, remoteFamilyLoader{component: registered})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	if contract.Mapper != nil {
		t.Fatal("lazy handler was bound before its registration loaded")
	}
	family, err := rt.LoadComponents(context.Background(), component.Key)
	if err != nil {
		t.Fatal(err)
	}
	if len(family) != 1 || contract.Mapper == nil || contract.HTTP == nil || contract.MCP == nil {
		t.Fatalf("lazy static binding: family=%d mapper=%v http=%v mcp=%v", len(family), contract.Mapper != nil, contract.HTTP != nil, contract.MCP != nil)
	}
	if len(registered.Providers) != 0 || len(family[0].Providers) == 0 {
		t.Fatal("lazy preparation mutated the loader registration or omitted defaults")
	}
	initialMapper := contract.Mapper
	if _, err := rt.LoadComponent(context.Background(), component.Key); err != nil || contract.Mapper != initialMapper {
		t.Fatalf("second lazy lookup changed static mapper: %v", err)
	}
}
