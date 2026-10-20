package input

import (
	"net/url"
	"reflect"
	"testing"

	requestprovider "github.com/viant/bindly/provider/request"
	bindstate "github.com/viant/bindly/state"
)

func TestScopeRetainsDeclaredEmptyQuery(t *testing.T) {
	for _, tc := range []struct {
		description string
		source      Source
	}{
		{"empty tool arguments", Arguments{}},
		{"empty resource query", URI{Query: url.Values{}}},
	} {
		t.Run(tc.description, func(t *testing.T) {
			plan, err := NewCompiler().Compile([]Argument{{PublicName: "Page", Source: bindstate.Location{Kind: "query", In: "page"}, SourceType: reflect.TypeFor[int]()}})
			if err != nil {
				t.Fatal(err)
			}
			scope, err := plan.Scope(tc.source)
			if err != nil {
				t.Fatal(err)
			}
			providers := scope.Providers()
			if len(providers) != 1 || providers[0].Kind() != "query" || reflect.TypeOf(providers[0]) != reflect.TypeOf(requestprovider.NewValues().Query()) {
				t.Fatalf("declared query lost its native provider: %v", providers)
			}
			if _, present, err := providers[0].Locate(nil).Value(t.Context(), nil, "page"); err != nil || present {
				t.Fatalf("omitted Page became present: present=%v err=%v", present, err)
			}
		})
	}
}
