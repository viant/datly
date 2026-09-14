package http

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/scy"
)

func TestAPIKeysOriginalPrefixAndSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key.txt")
	if err := os.WriteFile(path, []byte("resolved-key"), 0600); err != nil {
		t.Fatal(err)
	}
	source := APIKeys{{URI: "", Header: "X-Default", Value: "default"}, {URI: "/records", Header: "X-Key", Value: "old", Secret: &scy.Resource{URL: path}}, {URI: "/records/private", Header: "X-Private", Value: "private"}}
	keys, err := source.Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Routes: []*spec.Route{{Path: "/records/item"}, {Path: "/records/private/item"}, {Path: "/else"}, {Path: "/recordsSuffix"}}}
	keys.Apply(component)
	want := []string{"resolved-key", "private", "default", "resolved-key"}
	for i, route := range component.Routes {
		if route.APIKeyValue != want[i] {
			t.Fatalf("route %s value mismatch", route.Path)
		}
	}
	if source[1].Value != "old" || source[1].Secret == nil {
		t.Fatal("source policy mutated")
	}
	access := &DocumentAccess{APIKeyHeader: "X-Key", APIKeyValue: "resolved-key"}
	req := httptest.NewRequest("POST", "/warm", nil)
	if access.Authorize(req) == nil {
		t.Fatal("empty credential authorized")
	}
	req.Header.Set("X-Key", "resolved-key")
	if err := access.Authorize(req); err != nil {
		t.Fatal(err)
	}
}

func TestAPIKeysInvalidConfiguration(t *testing.T) {
	for _, keys := range []APIKeys{{{URI: "/x", Header: "X", Value: "a"}, {URI: "/x", Header: "X", Value: "b"}}, {{Header: "", Value: "secret"}}, {{Header: "X", Value: ""}}, {{Header: "X\nBad", Value: "secret"}}, {{URI: "relative", Header: "X", Value: "secret"}}} {
		if _, err := keys.Resolve(context.Background()); err == nil {
			t.Fatal("invalid keys accepted")
		}
	}
}
