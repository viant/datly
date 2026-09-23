package data

import (
	"fmt"
	"testing"

	"github.com/viant/datly/spec"
)

func TestFromViewPreservesBatchConcurrency(t *testing.T) {
	for _, concurrency := range []int{0, 1, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			child := &spec.View{Name: "items", BatchSize: 2, BatchConcurrency: concurrency, RelationalConcurrency: 3}
			root := &spec.View{Name: "records", Relations: []*spec.Relation{{Name: "Items", View: child}}}
			actual := FromView(nil, root).Relations[0].Of.View
			if actual.Spec.BatchSize != 2 || actual.Spec.BatchConcurrency != concurrency || actual.Spec.RelationalConcurrency != 3 {
				t.Fatalf("runtime batch settings = %+v", actual)
			}
			actual.Spec.BatchConcurrency = 9
			if child.BatchConcurrency != concurrency {
				t.Fatal("runtime batch settings mutated canonical view")
			}
		})
	}
}

func TestFromViewPreservesViewCacheAndClonesCanonicalMetadata(t *testing.T) {
	limit := 25
	component := &spec.Component{
		Description: "users",
		Settings: &spec.Settings{Cache: &spec.CacheSettings{
			Name:   "component-cache",
			Warmup: &spec.CacheWarmupSettings{Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "tenant", Values: []string{"1"}}}}}},
		}},
	}
	canonical := &spec.View{
		Name: "users",
		Source: &spec.ViewSource{
			Controls: &spec.ViewControls{Limit: &limit},
			Bindings: &spec.ViewBindings{Connector: "analytics", CacheName: "view-cache"},
		},
		Selector:     &spec.Selector{Filterable: []spec.FieldPath{"ID"}, OrderAliases: map[string]spec.FieldPath{"created": "CreatedAt"}},
		Partitioning: &spec.Partitioning{Type: "Tenant", Arguments: []string{"TenantID"}},
	}

	actual := FromView(component, canonical)
	if actual.Cache == nil || actual.Cache.Name != "view-cache" || actual.Cache.Warmup == nil || actual.Connector != "analytics" {
		t.Fatalf("resolved cache/bindings = %+v, connector=%q", actual.Cache, actual.Connector)
	}
	actual.Spec.Selector.Filterable[0] = "Changed"
	actual.Spec.Selector.OrderAliases["created"] = "Changed"
	actual.Spec.Partitioning.Arguments[0] = "Changed"
	actual.Spec.Source.Controls.Limit = nil
	actual.Cache.Warmup.Cases[0].Set[0].Values[0] = "changed"
	if canonical.Selector.Filterable[0] != "ID" || canonical.Selector.OrderAliases["created"] != "CreatedAt" ||
		canonical.Partitioning.Arguments[0] != "TenantID" || canonical.Source.Controls.Limit == nil ||
		component.Settings.Cache.Warmup.Cases[0].Set[0].Values[0] != "1" {
		t.Fatal("resolved view aliases canonical or component metadata")
	}
}

func TestFromViewUsesComponentCacheWhenViewHasNoCacheBinding(t *testing.T) {
	component := &spec.Component{Settings: &spec.Settings{Cache: &spec.CacheSettings{Name: "component-cache"}}}
	actual := FromView(component, &spec.View{Name: "users", Source: &spec.ViewSource{}})
	if actual.Cache == nil || actual.Cache.Name != "component-cache" {
		t.Fatalf("cache = %+v", actual.Cache)
	}
}

func TestFromViewPreservesCanonicalNullableScalarFallback(t *testing.T) {
	actual := FromView(nil, &spec.View{Columns: []*spec.Column{
		{Name: "name", Type: spec.TypeRef{Name: "string"}, Nullable: true},
		{Name: "payload", Type: spec.TypeRef{Name: "Payload"}, Nullable: true},
	}})
	if len(actual.Columns) != 2 || !actual.Columns[0].Nullable || actual.Columns[0].NullFallback != "''" {
		t.Fatalf("scalar column = %+v", actual.Columns)
	}
	if !actual.Columns[1].Nullable || actual.Columns[1].NullFallback != "" {
		t.Fatalf("custom column = %+v", actual.Columns[1])
	}
}

func TestFromViewRequiredSuppressesNullableInference(t *testing.T) {
	source := &spec.View{Columns: []*spec.Column{{Name: "value", Type: spec.TypeRef{Name: "string"}, Nullable: true, Required: true}}}
	actual := FromView(nil, source)
	if actual.Columns[0].Nullable || actual.Columns[0].NullFallback != "" || !actual.Spec.Columns[0].Required {
		t.Fatalf("required output column = %+v", actual.Columns[0])
	}
	if !source.Columns[0].Nullable {
		t.Fatal("runtime conversion mutated authored metadata")
	}
}

func TestFromViewResolvesCanonicalRelationMatchStrategy(t *testing.T) {
	tests := []struct {
		name     string
		source   spec.MatchStrategy
		expected MatchStrategy
	}{
		{name: "default", expected: MatchSequential},
		{name: "read all", source: spec.MatchReadAll, expected: MatchReadAll},
		{name: "read matched", source: spec.MatchReadMatched, expected: MatchSequential},
		{name: "read derived", source: spec.MatchReadDerived, expected: MatchSequential},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := FromView(nil, &spec.View{Name: "orders", Relations: []*spec.Relation{{
				Name: "items", MatchStrategy: test.source, View: &spec.View{Name: "items"},
			}}})
			if len(actual.Relations) != 1 || actual.Relations[0].Of == nil ||
				actual.Relations[0].Of.MatchStrategy != test.expected {
				t.Fatalf("relation = %+v", actual.Relations)
			}
		})
	}
}

func TestFromViewOwnsOneSpecSnapshotPerGraphNode(t *testing.T) {
	child := &spec.View{
		Key: spec.Key{Kind: spec.KindView, Name: "rows"}, Name: "rows", Namespace: "child",
		Source:  &spec.ViewSource{SQL: "SELECT id FROM children"},
		Columns: []*spec.Column{{Name: "ID", Type: spec.TypeRef{Name: "int"}, Codec: &spec.Codec{Body: "decode", Args: []string{"arg"}}}},
	}
	root := &spec.View{Name: "rows", Namespace: "root", Relations: []*spec.Relation{
		{Name: "Left", View: child}, {Name: "Right", View: child},
	}}
	// Conversion preserves graph identity; executable dependency cycles are
	// still rejected by the reader plan owner.
	child.Relations = []*spec.Relation{{Name: "Parent", View: root}}
	actual := FromView(nil, root)
	left, right := actual.Relations[0].Of.View, actual.Relations[1].Of.View
	if left != right || left.Relations[0].Of.View != actual {
		t.Fatal("shared targets or cycle identity changed")
	}
	if actual.Spec.Relations[0].View != &left.Spec || actual.Spec.Relations[1].View != &right.Spec || left.Spec.Relations[0].View != &actual.Spec {
		t.Fatal("spec snapshot and reader graph reference different metadata nodes")
	}
	wantID, err := child.Identity()
	if err != nil {
		t.Fatal(err)
	}
	actualID, err := left.Spec.Identity()
	if err != nil || actualID != wantID || actual.Spec.Namespace != "root" {
		t.Fatalf("canonical identity = %q, %v; want %q", actualID, err, wantID)
	}
	left.Spec.Namespace = "compiled"
	left.Spec.Source.SQL = "SELECT id FROM resolved_children"
	left.Spec.Columns[0].Codec.Args[0] = "compiled"
	if actual.Spec.Relations[1].View.Namespace != "compiled" || left.Columns[0].Codec != left.Spec.Columns[0].Codec {
		t.Fatal("compiled metadata does not use the owned spec snapshot")
	}
	if child.Namespace != "child" || child.Source.SQL != "SELECT id FROM children" || child.Columns[0].Codec.Args[0] != "arg" {
		t.Fatal("reader enrichment mutated caller source")
	}
	child.Name = "changed"
	if left.Spec.Name != "rows" {
		t.Fatal("caller source mutation reached reader snapshot")
	}
	second := FromView(nil, root)
	second.Relations[0].Of.View.Spec.Source.SQL = "another compilation"
	if left.Spec.Source.SQL != "SELECT id FROM resolved_children" {
		t.Fatal("independent compilations share mutable metadata")
	}
}

func TestFromViewPreservesAbsentAndExplicitBooleanSettings(t *testing.T) {
	enabled := true
	for _, test := range []struct {
		name  string
		value *bool
	}{{name: "absent"}, {name: "false", value: new(bool)}, {name: "true", value: &enabled}} {
		t.Run(test.name, func(t *testing.T) {
			source := &spec.View{AllowNulls: test.value, Groupable: test.value}
			actual := FromView(nil, source)
			want := test.value != nil && *test.value
			if actual.NullsAllowed() != want || actual.IsGroupable() != want ||
				(actual.Spec.AllowNulls == nil) != (test.value == nil) || (actual.Spec.Groupable == nil) != (test.value == nil) {
				t.Fatalf("boolean settings lost presence or defaults: %+v", actual.Spec)
			}
			if test.value != nil {
				*actual.Spec.AllowNulls = !want
				*actual.Spec.Groupable = !want
				if *source.AllowNulls != want || *source.Groupable != want {
					t.Fatal("boolean settings alias caller source")
				}
			}
		})
	}
}
