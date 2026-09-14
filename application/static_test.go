package application

import (
	"context"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestStaticGenerationPinsPolicyAndBytes(t *testing.T) {
	ctx := context.Background()
	manager, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(ctx)
	source := fstest.MapFS{"public/index.html": &fstest.MapFile{Data: []byte("one")}}
	content := &spec.StaticContent{Path: "/ui", Namespace: "site", Root: "public", APIKeyHeader: "X-Key", APIKeyValue: "one"}
	compile := func(context.Context, *typecatalog.Catalog) (*Build, error) {
		store := resource.New()
		if err := store.Register("site", source); err != nil {
			return nil, err
		}
		return &Build{Resources: store, HTTP: gateway.Config{StaticContent: []*spec.StaticContent{content}}}, nil
	}
	if err = manager.Reload(ctx, Request{Revision: 1, Compile: compile}); err != nil {
		t.Fatal(err)
	}
	pinned, _, err := manager.pin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	source["public/index.html"].Data = []byte("two")
	content.APIKeyValue = "two"
	if err = manager.Reload(ctx, Request{Revision: 2, Compile: compile}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		ctx       context.Context
		key, body string
		status    int
	}{{pinned, "one", "one", 200}, {ctx, "two", "two", 200}, {ctx, "one", "", 403}, {pinned, "two", "", 403}} {
		req := httptest.NewRequest("GET", "/ui/", nil).WithContext(tc.ctx)
		req.Header.Set("X-Key", tc.key)
		w := httptest.NewRecorder()
		manager.ServeHTTP(w, req)
		if w.Code != tc.status || w.Body.String() != tc.body {
			t.Fatalf("%d %q", w.Code, w.Body.String())
		}
	}
	content.Root = "missing"
	if err = manager.Reload(ctx, Request{Revision: 3, Compile: compile}); err == nil {
		t.Fatal("missing root accepted")
	}
	if manager.Revision() != 2 {
		t.Fatal("failed generation was published")
	}
	req := httptest.NewRequest("GET", "/ui/", nil)
	req.Header.Set("X-Key", "two")
	w := httptest.NewRecorder()
	manager.ServeHTTP(w, req)
	if w.Body.String() != "two" {
		t.Fatal("old generation lost")
	}
}
