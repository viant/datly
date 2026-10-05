package dql

import (
	"encoding/json"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestNilSlicePolicyRoundTrip(t *testing.T) {
	for _, policy := range []string{"empty_array", "null"} {
		t.Run(policy, func(t *testing.T) {
			component, err := parseComponentSource("example.com/output", "Records", "#setting($_ = $route('/records', 'GET'))\n#setting($_ = $nil_slice_policy('"+policy+"'))\nSELECT id FROM records")
			if err != nil {
				t.Fatal(err)
			}
			clone := component.Settings.Clone()
			if clone.Output == nil || clone.Output.NilSlicePolicy != policy {
				t.Fatalf("clone: %+v", clone)
			}
			clone.Output.NilSlicePolicy = "changed"
			if component.Settings.Output.NilSlicePolicy != policy {
				t.Fatal("clone aliases authored settings")
			}
			raw, err := json.Marshal(component.Settings)
			if err != nil {
				t.Fatal(err)
			}
			var decoded spec.Settings
			if err = json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.Output.NilSlicePolicy != policy {
				t.Fatalf("JSON: %s", raw)
			}
			tag, err := dtag.SettingsFromSpec(&decoded).StructTag()
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := dtag.ParseSettings(reflect.StructTag(tag))
			if err != nil {
				t.Fatal(err)
			}
			var loaded spec.Settings
			parsed.Apply(&loaded)
			if loaded.Output.NilSlicePolicy != policy {
				t.Fatalf("tag: %s", tag)
			}
		})
	}
}

func TestNilSlicePolicyRejectsMalformedDirectives(t *testing.T) {
	for _, directive := range []string{
		"$nil_slice_policy()", "$nil_slice_policy('empty_array','null')", "$nil_slice_policy(true)", "$nil_slice_policy(empty_array)", "$nil_slice_policy('')", "$nil_slice_policy('EMPTY_ARRAY')", "$nil_slice_policy('typo')", "$nil_slice_policy('null').Unexpected()",
		"$nil_slice_policy('null'))\n#setting($_ = $nil_slice_policy('null')", "$nil_slice_policy('empty_array'))\n#setting($_ = $nil_slice_policy('null')",
	} {
		t.Run(directive, func(t *testing.T) {
			source := "#setting($_ = $route('/records', 'GET'))\n#setting($_ = " + directive + ")\nSELECT id FROM records"
			_, first := parseComponentSource("example.com/output", "Records", source)
			_, second := parseComponentSource("example.com/output", "Records", source)
			if first == nil || second == nil {
				t.Fatal("invalid directive accepted")
			}
			if first.Error() != second.Error() {
				t.Fatalf("nondeterministic diagnostics: %v / %v", first, second)
			}
		})
	}
}
