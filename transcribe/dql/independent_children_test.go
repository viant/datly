package dql

import (
	dtag "github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestIndependentChildrenMetadataRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		description, input string
		expected           bool
	}{
		{"explicit independent orchestration", "true", true},
		{"default shared orchestration", "false", false},
	} {
		t.Run(tc.description, func(t *testing.T) {
			source := `#package('example.com/orchestrate')
#setting($_ = $route('/orchestrate','POST'))
#setting($_ = $independent_child_transactions(` + tc.input + `))
SELECT 1 AS ID`
			compiled, err := parseComponentSource("example.com/orchestrate", "Orchestrate", source)
			if err != nil {
				t.Fatal(err)
			}
			if (compiled.Settings == nil && tc.expected) || (compiled.Settings != nil && compiled.Settings.IndependentChildTransactions != tc.expected) {
				t.Fatalf("policy metadata=%+v", compiled.Settings)
			}
			rendered, err := dtag.SettingsFromSpec(compiled.Settings).StructTag()
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := dtag.ParseSettings(reflect.StructTag(rendered))
			if err != nil {
				t.Fatal(err)
			}
			if decoded.IndependentChildTransactions != tc.expected {
				t.Fatalf("tag round trip=%+v", decoded)
			}
		})
	}
}
