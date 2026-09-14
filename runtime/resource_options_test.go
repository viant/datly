package runtime

import (
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
)

func TestResourceStoreAndFileOptionsCannotSilentlyOverride(t *testing.T) {
	store := resource.New()
	files := fstest.MapFS{"query.sql": {Data: []byte("SELECT 1")}}
	for _, options := range [][]Option{{WithResources(store), WithResourceFS("extra", files)}, {WithResourceFS("extra", files), WithResources(store)}} {
		if _, err := NewRuntime(nil, options...); err == nil {
			t.Fatal("order-dependent resource options accepted")
		}
		if _, found := store.Lookup("extra"); found {
			t.Fatal("invalid registration mutated explicit store")
		}
	}
	if _, err := NewRuntime(nil, WithResources(store), WithResources(store)); err != nil {
		t.Fatalf("identical store selection failed: %v", err)
	}
}
