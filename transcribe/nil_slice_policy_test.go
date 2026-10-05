package transcribe

import (
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"testing"
)

func TestNilSlicePolicyAuthoredOverlay(t *testing.T) {
	for _, policy := range []string{"empty_array", "null", ""} {
		t.Run(policy, func(t *testing.T) {
			base := &spec.Settings{Output: &spec.OutputSettings{NilSlicePolicy: "empty_array", Exclude: []string{"Secret"}, OmitEmpty: true, Title: "export"}}
			var authored *spec.Settings
			if policy != "" {
				plan := dql.PrepareSource("#setting($_ = $nil_slice_policy('" + policy + "'))\nSELECT id FROM records")
				if err := plan.Err(); err != nil {
					t.Fatal(err)
				}
				authored = plan.Directives.Settings
			}
			result := (&settingsLoader{base: base, authored: authored}).Load()
			want := policy
			if want == "" {
				want = "empty_array"
			}
			if result.Output.NilSlicePolicy != want || !result.Output.OmitEmpty || result.Output.Title != "export" || result.Output.Exclude[0] != "Secret" {
				t.Fatalf("overlay %+v", result.Output)
			}
			if base.Output.NilSlicePolicy != "empty_array" {
				t.Fatal("overlay mutated base")
			}
		})
	}
}
