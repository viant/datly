package typecatalog

import (
	"encoding/json"
	"reflect"
	"testing"

	xshape "github.com/viant/x/shape"
)

func TestResolverProvidesStandardJSONRawMessage(t *testing.T) {
	resolver, err := NewResolver(NewCatalog(), TranscribeAuthority, &ResolutionContext{
		Imports: []PackageImport{{Alias: "json", Package: "encoding/json"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	jsonType := reflect.TypeFor[json.RawMessage]()
	jsonKey := "encoding/json.RawMessage"
	if descriptor := resolver.types[jsonKey]; descriptor == nil || descriptor.Type != jsonType {
		t.Fatalf("standard json key=%q type=%v descriptor=%#v available=%v", jsonKey, jsonType, descriptor, resolver.types)
	}
	named, err := (xshape.Resolver{}).Named("encoding/json.RawMessage")
	if err != nil || named != jsonKey {
		t.Fatalf("standard json named=%q want=%q err=%v", named, jsonKey, err)
	}
	lookup, err := resolver.lookupCanonical("encoding/json.RawMessage")
	if err != nil || lookup == nil || lookup.Type != reflect.TypeFor[json.RawMessage]() {
		t.Fatalf("standard json lookup=%#v err=%v", lookup, err)
	}
	resolved, err := resolver.ResolveShape("json.RawMessage")
	if err != nil || resolved == nil || resolved.Descriptor == nil || resolved.Descriptor.Type != reflect.TypeFor[json.RawMessage]() {
		t.Fatalf("json.RawMessage resolution=%+v err=%v", resolved, err)
	}
}
