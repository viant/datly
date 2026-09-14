package reader

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/typecatalog"
	x "github.com/viant/x"
)

func testTypeCatalog(t *testing.T, entries map[string]reflect.Type) *typecatalog.Catalog {
	t.Helper()
	catalog := typecatalog.NewCatalog()
	for key, rType := range entries {
		separator := strings.LastIndex(key, ".")
		if separator <= 0 || separator == len(key)-1 {
			t.Fatalf("type key %q must be absolute", key)
		}
		typ := x.NewType(rType, x.WithPkgPath(key[:separator]), x.WithName(key[separator+1:]))
		if err := catalog.Register(typecatalog.TypeOriginPackage, typ); err != nil {
			t.Fatalf("register type %q: %v", key, err)
		}
	}
	return catalog
}
