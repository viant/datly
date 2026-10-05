package resource

import (
	"context"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestResourceEmptyPlaceholderAndSlashDefaults(t *testing.T) {
	contract := newResourceContract(t, "/orders/{id}/details", []bindly.BindingSpec{{Path: "ID", Location: bindstate.Location{Kind: "path", In: "id"}}})
	c, e := NewCompiler("datly://localhost")
	if e != nil {
		t.Fatal(e)
	}
	p, e := c.Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "Order"}, Route: &spec.Route{Method: "GET", Path: "/orders/{id}/details"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: "orders"}, Contract: contract})
	if e != nil {
		t.Fatal(e)
	}
	catalog, e := NewCatalog([]*Plan{p})
	if e != nil {
		t.Fatal(e)
	}
	for _, uri := range []string{"datly://localhost/orders//details", "datly://localhost/orders/details"} {
		t.Run(uri, func(t *testing.T) {
			if _, e := catalog.Resolve(uri); e == nil {
				t.Fatal("empty or missing placeholder resolved")
			}
		})
	}
	for _, tc := range []struct{ encoded, want string }{{"a%2Fb", "a/b"}, {"%2F", "/"}, {"%20", " "}, {"a%252Fb", "a%2Fb"}} {
		t.Run(tc.encoded, func(t *testing.T) {
			r, e := catalog.Resolve("datly://localhost/orders/" + tc.encoded + "/details")
			if e != nil {
				t.Fatal(e)
			}
			v, found, e := r.Scope().Path().Locate(nil).Value(context.Background(), reflect.TypeOf(""), "id")
			if e != nil || !found || v != tc.want {
				t.Fatalf("value=%v found=%v err=%v", v, found, e)
			}
		})
	}
}
