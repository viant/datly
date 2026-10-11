package generate

import (
	"reflect"
	"testing"

	dtag "github.com/viant/datly/tag"
)

func TestGeneratedOutputConstantRetainsBindingSourceAndTypedDefault(t *testing.T) {
	source := `#setting($_ = $route('/constants','GET'))
#define($_ = $MergeConfig<string>(const/dbRuleMergeConfig).Value('null').Optional().Output())
SELECT 1`
	c, err := parseTestComponentSource("example.test/constants", "Constants", source)
	if err != nil {
		t.Fatal(err)
	}
	plan := testPlan(t, c)
	for _, field := range plan.Output.Fields {
		if field.Name != "MergeConfig" {
			continue
		}
		parsed, err := dtag.ParseField(reflect.StructField{Name: field.Name, Type: reflect.TypeFor[string](), Tag: reflect.StructTag(field.Tag)})
		if err != nil {
			t.Fatal(err)
		}
		if parsed.Binding == nil || parsed.Binding.Location.Kind != "const" || parsed.Binding.Location.In != "dbRuleMergeConfig" || parsed.Binding.DefaultValue != "null" {
			t.Fatalf("generated constant binding: %+v tag=%s", parsed.Binding, field.Tag)
		}
		return
	}
	t.Fatal("generated constant output missing")
}
