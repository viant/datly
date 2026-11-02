package writer

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestGeneratedRelationMutationPredicateMetadata(t *testing.T) {
	ptr := func(v int) *int { return &v }
	for _, tc := range []struct {
		name, tag string
		view      *spec.View
		want      *int
		invalid   bool
	}{
		{name: "absent", tag: "children"},
		{name: "generated-zero", tag: "children,mutationPredicate=0", want: ptr(0)},
		{name: "generated-eight", tag: "children,mutationPredicate=8", want: ptr(8)},
		{name: "explicit", tag: "children", view: &spec.View{MutationPredicateGroup: ptr(8)}, want: ptr(8)},
		{name: "matching", tag: "children,mutationPredicate=8", view: &spec.View{MutationPredicateGroup: ptr(8)}, want: ptr(8)},
		{name: "conflicting", tag: "children,mutationPredicate=8", view: &spec.View{MutationPredicateGroup: ptr(7)}, invalid: true},
		{name: "negative-tag", tag: "children,mutationPredicate=-1", invalid: true},
		{name: "negative-view", tag: "children", view: &spec.View{MutationPredicateGroup: ptr(-1)}, invalid: true},
		{name: "duplicate", tag: "children,mutationPredicate=8,mutationPredicate=8", invalid: true},
		{name: "malformed", tag: "children,mutationPredicate=x", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := recordMutationPredicate(tc.tag, tc.view)
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid mutation metadata accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == nil {
				if got != nil {
					t.Fatalf("unexpected group %v", got)
				}
				return
			}
			if got == nil || *got != *tc.want {
				t.Fatalf("group %v want %d", got, *tc.want)
			}
			if tc.view != nil && got == tc.view.MutationPredicateGroup {
				t.Fatal("group aliases mutable View")
			}
		})
	}
}
