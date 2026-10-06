package tag

import (
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
)

func TestComponentCallPolicyTags(t *testing.T) {
	for _, policy := range []string{"", "imperative", "buffered"} {
		source := &spec.Settings{ComponentCallPolicy: policy}
		rendered, err := SettingsFromSpec(source).StructTag()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(rendered, "componentCallPolicy") != (policy != "") {
			t.Fatalf("default/tag contract: %q", rendered)
		}
		decoded, err := ParseSettings(reflect.StructTag(rendered))
		if err != nil {
			t.Fatal(err)
		}
		target := &spec.Settings{}
		decoded.Apply(target)
		if target.ComponentCallPolicy != policy {
			t.Fatalf("tag lost policy %q: %+v", policy, target)
		}
	}
	for _, value := range []reflect.StructTag{
		`componentCallPolicy:""`, `componentCallPolicy:"unknown"`, `componentCallPolicy:"BUFFERED"`,
		`componentCallPolicy:"buffered" independentChildTransactions:"true"`,
	} {
		if _, err := ParseSettings(value); err == nil {
			t.Fatalf("accepted malformed/conflicting tag %q", value)
		}
	}
	for _, value := range []Settings{{ComponentCallPolicy: "unknown"}, {ComponentCallPolicy: "buffered", IndependentChildTransactions: true}} {
		if _, err := value.StructTag(); err == nil {
			t.Fatalf("emitted invalid metadata %+v", value)
		}
	}
}
