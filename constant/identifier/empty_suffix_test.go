package identifier

import (
	"github.com/viant/datly/constant"
	"testing"
)

func TestImmutableEmptyIdentifierSuffix(t *testing.T) {
	source := "SELECT ID FROM `CI_TARGETING_RULE${mdp_table_suffix}` WHERE FEATURE_TYPE = 'ad.pmp.deal.id'"
	for _, suffix := range []string{"", "_SOAK01"} {
		values, err := constant.New(map[string]string{"mdp_table_suffix": suffix})
		if err != nil {
			t.Fatal(err)
		}
		renderer := Renderer{Values: values}
		got, err := renderer.Render(source)
		want := "SELECT ID FROM `CI_TARGETING_RULE" + suffix + "` WHERE FEATURE_TYPE = 'ad.pmp.deal.id'"
		if err != nil || got != want {
			t.Fatalf("suffix %q: got %q, want %q, error %v", suffix, got, want, err)
		}
		got, err = renderer.Identifier("`CI_TARGETING_RULE${mdp_table_suffix}`")
		if err != nil || got != "`CI_TARGETING_RULE"+suffix+"`" {
			t.Fatalf("table suffix %q: %q %v", suffix, got, err)
		}
	}
	values, err := constant.New(map[string]string{"mdp_table_suffix": ""})
	if err != nil {
		t.Fatal(err)
	}
	renderer := Renderer{Values: values}
	for _, table := range []string{"`${mdp_table_suffix}`", "`${mdp_table_suffix}.records`", "`records.${mdp_table_suffix}`", "`bad name${mdp_table_suffix}`", "`records${mdp_table_suffix}${other}`"} {
		if _, err := renderer.Identifier(table); err == nil {
			t.Fatalf("empty suffix admitted incomplete/unsupported identifier %q", table)
		}
	}
}
