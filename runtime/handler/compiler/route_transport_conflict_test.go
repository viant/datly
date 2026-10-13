package compiler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestEffectiveRouteTransportDestinations(t *testing.T) {
	type input struct{ A, B string }
	for _, tc := range []struct {
		name, kindA, kindB, sourceA, sourceB, routeA, routeB string
		conflict                                             bool
	}{
		{name: "duplicate query", kindA: "query", kindB: "query", sourceA: "q", sourceB: "q", conflict: true},
		{name: "duplicate cookie", kindA: "cookie", kindB: "cookie", sourceA: "session", sourceB: "session", conflict: true},
		{name: "duplicate path", kindA: "path", kindB: "path", sourceA: "id", sourceB: "id", conflict: true},
		{name: "duplicate form", kindA: "form", kindB: "form", sourceA: "value", sourceB: "value", conflict: true},
		{name: "shared immutable header", kindA: "header", kindB: "header", sourceA: "X-Name", sourceB: "x-name"},
		{name: "distinct sources", kindA: "query", kindB: "form", sourceA: "value", sourceB: "value"},
		{name: "query case preserved", kindA: "query", kindB: "query", sourceA: "Name", sourceB: "name"},
		{name: "distinct routes", kindA: "query", kindB: "query", sourceA: "q", sourceB: "q", routeA: "/a", routeB: "/b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Example"}, Routes: []*spec.Route{{Method: "GET", Path: "/a"}, {Method: "GET", Path: "/b"}}, Parameters: []*spec.Parameter{
				{Name: "A", Source: spec.BindSource{Kind: tc.kindA, Name: tc.sourceA}, Activation: &spec.RouteActivation{URI: tc.routeA}},
				{Name: "B", Source: spec.BindSource{Kind: tc.kindB, Name: tc.sourceB}, Activation: &spec.RouteActivation{URI: tc.routeB}},
			}}
			_, err := New(Input{Component: component, InputType: reflect.TypeFor[input]()}).Compile()
			if tc.conflict {
				if err == nil || !strings.Contains(err.Error(), "targets both A and B") {
					t.Fatalf("conflict error = %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
