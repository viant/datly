package bootstrap

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/mcp/tool/testdata/customjson"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestWireTypeAuthoredOwnerReconciliation(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeFor[customjson.RequestWire](), reflect.TypeFor[customjson.OtherWire]()} {
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
	}
	const pkg = "github.com/viant/datly/mcp/tool/testdata/customjson"
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "owned", Package: pkg}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, expression := range []string{"owned.RequestWire", "owned.OtherWire"} {
		component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Body", Source: spec.BindSource{Kind: "body"}, WireTypeExpr: expression}}}
		resolved, err := (ContractResolver{Component: component, InputType: linkedContractType(reflect.TypeFor[customjson.Input]()), Types: resolver}).Resolve()
		if expression == "owned.OtherWire" {
			if err == nil || !strings.Contains(err.Error(), "conflicting wireType") || !strings.Contains(err.Error(), "Body") {
				t.Fatalf("conflict: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if resolved.Parameters[0].WireTypeExpr != expression || component.Parameters[0].WireTypeExpr != expression {
			t.Fatal("authored owner metadata changed")
		}
	}
}
