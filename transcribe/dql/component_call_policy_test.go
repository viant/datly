package dql

import (
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestComponentCallPolicyAuthoredMetadata(t *testing.T) {
	for _, policy := range []string{"imperative", "buffered"} {
		source := `#package('example.com/orders')
#setting($_ = $route('/orders', 'POST'))
#setting($_ = $component_call_policy('` + policy + `'))
SELECT 1 AS ID`
		compiled, err := parseComponentSource("example.com/orders", "Orders", source)
		if err != nil {
			t.Fatal(err)
		}
		if compiled.Settings == nil || compiled.Settings.ComponentCallPolicy != policy {
			t.Fatalf("lost authored policy %+v", compiled.Settings)
		}
		rendered, err := dtag.SettingsFromSpec(compiled.Settings).StructTag()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := dtag.ParseSettings(reflect.StructTag(rendered))
		if err != nil {
			t.Fatal(err)
		}
		restored := &spec.Settings{}
		decoded.Apply(restored)
		if restored.Clone().ComponentCallPolicy != policy {
			t.Fatalf("lost reconstructed policy %+v", restored)
		}
		exported := (Serializer{}).Export(compiled, source)
		if !exported.Original || !exported.Supported || exported.Source != source {
			t.Fatal("authored source changed")
		}
		if (Serializer{}).Export(compiled, "").Supported {
			t.Fatal("lossy SQL-only export discarded caller policy")
		}
	}
	ordinary, err := parseComponentSource("example.com/orders", "Orders", `#package('example.com/orders')
#setting($_ = $route('/orders','POST'))
SELECT 1 AS ID`)
	if err != nil {
		t.Fatal(err)
	}
	if ordinary.Settings != nil && ordinary.Settings.ComponentCallPolicy != "" {
		t.Fatal("ordinary calls opted in")
	}
}

func TestComponentCallPolicyRejectsInvalidAuthorship(t *testing.T) {
	for _, declarations := range []string{
		`$component_call_policy('')`, `$component_call_policy('unknown')`, `$component_call_policy('BUFFERED')`,
		`$component_call_policy()`, `$component_call_policy('buffered','imperative')`,
		`$component_call_policy('buffered').When(false)`, `$component_call_policy('buffered').Optional()`, `$component_call_policy('buffered')garbage`,
		"$component_call_policy('buffered'))\n#setting($_ = $component_call_policy('imperative')",
		"$component_call_policy('buffered'))\n#setting($_ = $independent_child_transactions(true)",
		"$independent_child_transactions(true))\n#setting($_ = $component_call_policy('buffered')",
	} {
		source := `#package('example.com/orders')
#setting($_ = $route('/orders','POST'))
#setting($_ = ` + declarations + `)
SELECT 1 AS ID`
		if _, err := parseComponentSource("example.com/orders", "Orders", source); err == nil {
			t.Fatalf("accepted invalid declarations %s", declarations)
		}
	}
}

func TestComponentCallPolicyRejectsParsedModifiers(t *testing.T) {
	for _, body := range []string{"$component_call_policy('buffered').When(false)", "$component_call_policy('buffered').Optional()", "$component_call_policy('buffered')garbage"} {
		if _, err := parseComponentSettings([]directiveBlock{{kind: directiveKindSetting, body: "$_ = " + body}}); err == nil {
			t.Fatalf("parsed setting accepted unsupported tail: %s", body)
		}
	}
}
