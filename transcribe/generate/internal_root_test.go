package generate

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestInternalMutationRootTagsAndClientExclusion(t *testing.T) {
	p := &spec.Parameter{Name: "Delete", TypeExpr: "[]*DeleteView", Source: spec.BindSource{Kind: "internal"}}
	tag := reflect.StructTag(fieldTag(p, "Delete", nil))
	if tag.Get("json") != "-" || tag.Get("internal") != "true" {
		t.Fatalf("internal root exposed: %s", tag)
	}
	if publicClientParameter(p) {
		t.Fatal("internal root included in client contract")
	}
}
