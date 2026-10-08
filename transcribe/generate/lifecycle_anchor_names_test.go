package generate

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestLocalLifecycleAnchorNames(t *testing.T) {
	plan := &Plan{Package: "example.com/components", Holder: "Writer_Component", LifecycleTypes: []string{"Lifecycle", "same.Lifecycle", "Other", "Lifecycle"}, Imports: []spec.ImportSpec{{Alias: "same", Package: "example.com/components"}}}
	want := []lifecycleAnchor{{expression: "Lifecycle", symbol: "_anchorLifecycle_Writer_Component"}, {expression: "Other", symbol: "_anchorOther_Writer_Component"}}
	for i := 0; i < 2; i++ {
		if got := localLifecycleAnchors(plan); !reflect.DeepEqual(got, want) {
			t.Fatalf("anchors = %#v, want %#v", got, want)
		}
	}
	plan.Holder = "Writer_Component_0"
	for _, other := range localLifecycleAnchors(plan) {
		for _, original := range want {
			if other.symbol == original.symbol {
				t.Fatalf("holder names collided: %s", other.symbol)
			}
		}
	}
}
