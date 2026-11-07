package docs_test

import (
	docs "github.com/viant/datly/documentation"
	"reflect"
	"testing"
)

func TestEffectiveFieldAnnotationOverridesLegacyDescription(t *testing.T) {
	var snapshot *docs.Snapshot
	for _, tc := range []struct {
		tag  reflect.StructTag
		want string
	}{
		{`desc:"legacy"`, "legacy"},
		{`desc:"legacy" description:"effective"`, "effective"},
		{`desc:"legacy" description:""`, ""},
	} {
		result := snapshot.StructField("Body.Name", reflect.StructField{Name: "Name", Type: reflect.TypeFor[string](), Tag: tc.tag})
		if result.Description != tc.want {
			t.Fatalf("%s: %q", tc.tag, result.Description)
		}
	}
}
