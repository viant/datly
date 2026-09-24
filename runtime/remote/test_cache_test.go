package remote

import (
	"context"
	"testing"

	cacheprovider "github.com/viant/datly/runtime/handler/provider/cache"
	xcache "github.com/viant/xdatly/cache"
)

const testCacheName = "remote-test"

func newTestCache(t *testing.T, capacity int) xcache.Provider {
	t.Helper()
	backend, err := cacheprovider.NewMemory(capacity)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := cacheprovider.New(map[string]xcache.Cache{testCacheName: backend})
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func testEntries(t *testing.T, mapper *Mapper, config *Config, provider xcache.Provider) int {
	t.Helper()
	count, err := mapper.CachedEntries(context.Background(), config, provider)
	if err != nil {
		t.Fatal(err)
	}
	return count
}
