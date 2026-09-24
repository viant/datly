package cache

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/viant/bindly/locator"
	xcache "github.com/viant/xdatly/cache"
)

type customCache struct{}

func (*customCache) Get(context.Context, string) ([]byte, bool, error)        { return nil, false, nil }
func (*customCache) Put(context.Context, string, []byte, time.Duration) error { return nil }
func (*customCache) Delete(context.Context, string) error                     { return nil }

func TestExplicitRegistry(t *testing.T) {
	var typedNil *customCache
	for _, backends := range []map[string]xcache.Cache{
		{"": &customCache{}},
		{"nil": nil},
		{"typed": typedNil},
	} {
		if _, err := New(backends); err == nil {
			t.Fatalf("invalid registration accepted: %v", backends)
		}
	}
	backend := &customCache{}
	source := map[string]xcache.Cache{"main": backend}
	registry, err := New(source)
	if err != nil {
		t.Fatal(err)
	}
	delete(source, "main")
	ctx := context.Background()
	got, err := registry.Cache(ctx, "main")
	if err != nil || got != backend {
		t.Fatalf("registered cache=%v err=%v", got, err)
	}
	for _, name := range []string{"", "MAIN", "missing"} {
		if got, err := registry.Cache(ctx, name); err == nil || got != nil {
			t.Fatalf("unregistered %q returned %v, %v", name, got, err)
		}
	}
	var nilRegistry *Registry
	if got, err := nilRegistry.Cache(ctx, "main"); err == nil || got != nil {
		t.Fatalf("nil registry returned %v, %v", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := registry.Cache(canceled, "main"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lookup err=%v", err)
	}
}

func TestNativeProviderBinding(t *testing.T) {
	var typedNil *Registry
	if len(Providers(nil)) != 0 || len(Providers(typedNil)) != 0 {
		t.Fatal("nil cache provider created a binding")
	}
	registry, _ := New(map[string]xcache.Cache{"main": &customCache{}})
	bindings := Providers(registry)
	if len(bindings) != 1 || bindings[0].Kind() != Kind || bindings[0].Priority() != locator.PriorityDefault {
		t.Fatalf("invalid cache binding: %v", bindings)
	}
	if policy, ok := bindings[0].(locator.CachePolicy); !ok || !policy.DefaultCacheable() {
		t.Fatal("static binding must be cacheable")
	}
	value := bindings[0].Locate(nil)
	got, found, err := value.Value(context.Background(), reflect.TypeFor[xcache.Provider](), "")
	if err != nil || !found || got != registry {
		t.Fatalf("provider binding got=%v found=%v err=%v", got, found, err)
	}
	if _, found, err := value.Value(context.Background(), reflect.TypeFor[xcache.Provider](), "named"); err != nil || found {
		t.Fatalf("named binding found=%v err=%v", found, err)
	}
	if _, _, err := value.Value(context.Background(), reflect.TypeFor[string](), ""); err == nil {
		t.Fatal("wrong target type accepted")
	}
}
