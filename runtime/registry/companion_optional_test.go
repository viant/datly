package registry

import (
	"github.com/viant/bindly"
	"github.com/viant/datly/internal/selectorcompanion"
	"testing"
)

func TestCompanionOptionalNoopRequiresExplicitFalseAndUniqueAlias(t *testing.T) {
	no := false
	yes := true
	for _, tc := range []struct {
		name       string
		required   *bool
		duplicates bool
		want       bool
	}{
		{"explicit optional", &no, false, true}, {"unspecified", nil, false, false}, {"required", &yes, false, false}, {"ambiguous", &no, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &RouteInputContract{contract: &InputContract{}, selectors: &selectorcompanion.BindingPlan{}, fields: []InputField{{path: "ID", binding: bindly.BindingSpec{Name: "ID", Required: tc.required}}}}
			if tc.duplicates {
				c.fields = append(c.fields, c.fields[0])
			}
			got, err := c.WithOptionalBindings("id")
			if tc.want {
				if err != nil || got != c {
					t.Fatalf("not exact no-op: %v", err)
				}
			} else if err == nil {
				t.Fatal("unsupported omission admitted")
			}
		})
	}
}
