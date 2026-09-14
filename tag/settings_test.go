package tag

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestSettingsRoundTripPreservesPackageAuthorityFacets(t *testing.T) {
	source := &spec.Settings{
		Generation: &spec.GenerationSettings{
			Template: "patch", DescriptionResource: "docs/orders.md", ViewFile: "orders.go", InputFile: "input.go",
		},
		InputType: "Input", Const: map[string]string{"Tenant": "acme"},
		CaseFormat: "lc", JSONMarshalType: "codec.JSON", JSONUnmarshalType: "codec.Input",
		XMLUnmarshalType: "codec.XML", Format: "tabular_json", DateFormat: "2006-01-02",
		Cache: &spec.CacheSettings{
			Enabled: true, Name: "orders", TTL: "5m", Provider: "aerospike://cache", Location: "orders", TimeToLiveMs: 300000,
			Warmup: &spec.CacheWarmupSettings{
				IndexColumn: "order_id", IndexParameter: "OrderID", Connector: "warehouse",
				Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "Period", Values: []string{"today", "yesterday"}}}}},
			},
		},
	}
	settings := SettingsFromSpec(source)
	source.Cache.Warmup.Cases[0].Set[0].Values[0] = "changed"
	tagValue, err := settings.StructTag()
	if err != nil {
		t.Fatalf("StructTag() error = %v", err)
	}
	actual, err := ParseSettings(reflect.StructTag(tagValue))
	if err != nil {
		t.Fatalf("ParseSettings() error = %v", err)
	}
	if actual.CaseFormat != "lc" || actual.JSONMarshalType != "codec.JSON" || actual.JSONUnmarshalType != "codec.Input" ||
		actual.XMLUnmarshalType != "codec.XML" || actual.Format != "tabular_json" || actual.DateFormat != "2006-01-02" ||
		actual.Cache == nil || actual.Cache.Warmup == nil || actual.Cache.Warmup.Cases[0].Set[0].Values[0] != "today" {
		t.Fatalf("settings round trip = %+v", actual)
	}
	target := &spec.Settings{
		Generation: &spec.GenerationSettings{Template: "keep", ViewFile: "keep.go"},
		Const:      map[string]string{"Keep": "yes"},
	}
	actual.Apply(target)
	if target.Generation == nil || target.Generation.Template != "keep" || target.Generation.ViewFile != "keep.go" || target.Const["Keep"] != "yes" ||
		target.CaseFormat != "lc" || target.Cache == nil {
		t.Fatalf("applied settings = %+v", target)
	}
}

func TestParseSettingsRejectsMalformedTypedFacets(t *testing.T) {
	for _, value := range []reflect.StructTag{`cache:"not-json"`} {
		if _, err := ParseSettings(value); err == nil {
			t.Fatalf("expected malformed settings error for %q", value)
		}
	}
}
