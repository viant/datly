package typecatalog

import (
	"reflect"
	"testing"

	xresponse "github.com/viant/xdatly/response"
)

func TestResolverProvidesStandardResponseMetrics(t *testing.T) {
	const publicPackage = "github.com/viant/xdatly/response"
	for _, authority := range []Authority{PackageAuthority, TranscribeAuthority} {
		t.Run(string(authority), func(t *testing.T) {
			catalog := NewCatalog()
			resolver, err := NewResolver(catalog, authority, &ResolutionContext{
				Imports: []PackageImport{{Alias: "wire", Package: publicPackage}},
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, expression := range []string{"wire.Metrics", publicPackage + ".Metrics"} {
				descriptor, err := resolver.Descriptor(expression)
				if err != nil || descriptor == nil || descriptor.Type != reflect.TypeFor[xresponse.Metrics]() {
					t.Fatalf("%s resolved to %+v: %v", expression, descriptor, err)
				}
			}
			other, err := NewResolver(catalog, authority, &ResolutionContext{
				Imports: []PackageImport{{Alias: "wire", Package: "example.com/response"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if descriptor, err := other.Descriptor("wire.Metrics"); err != nil || descriptor != nil {
				t.Fatalf("unrelated response package acquired built-in metrics: %+v %v", descriptor, err)
			}
		})
	}
}
