package tool

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

type singleSliceBody struct{ Values []int }

func (*singleSliceBody) UnmarshalJSON([]byte) error { return nil }

type optionalSliceBody struct {
	Values []int `json:"values,omitempty"`
}

func (*optionalSliceBody) UnmarshalJSON([]byte) error { return nil }

type renamedSliceBody struct {
	Values []int `json:"values"`
}

func (*renamedSliceBody) UnmarshalJSON([]byte) error { return nil }

type namedInts []int
type twoSliceBody struct {
	Values []int
	Other  int
}

func (*twoSliceBody) UnmarshalJSON([]byte) error { return nil }

type hiddenSliceBody struct {
	Values []int `json:"-"`
}

func (*hiddenSliceBody) UnmarshalJSON([]byte) error { return nil }

type embeddedSliceBody struct{ singleSliceBody }

func (*embeddedSliceBody) UnmarshalJSON([]byte) error { return nil }

type internalSliceBody struct {
	Values []int `internal:"true"`
}

func (*internalSliceBody) UnmarshalJSON([]byte) error { return nil }

type inlineSliceBody struct {
	Values []int `json:",inline"`
}

func (*inlineSliceBody) UnmarshalJSON([]byte) error { return nil }

type plainSliceBody struct{ Values []int }
type pointerSliceBody struct{ Values *[]int }

func (*pointerSliceBody) UnmarshalJSON([]byte) error { return nil }
func TestWholeBodyMemberSchemaRequired(t *testing.T) {
	base := Argument{sourceKind: "body", publicName: "Values", sourceType: reflect.TypeFor[*singleSliceBody](), destinationType: reflect.TypeFor[*singleSliceBody](), wireType: reflect.TypeFor[namedInts]()}
	for _, tc := range []struct {
		name                 string
		change               func(*Argument)
		required, associated bool
	}{
		{"exact", func(*Argument) {}, true, true},
		{"internal", func(a *Argument) {
			a.sourceType = reflect.TypeFor[*internalSliceBody]()
			a.destinationType = a.sourceType
		}, false, false},
		{"inline", func(a *Argument) {
			a.sourceType = reflect.TypeFor[*inlineSliceBody]()
			a.destinationType = a.sourceType
		}, false, false},
		{"no custom codec", func(a *Argument) { a.sourceType = reflect.TypeFor[*plainSliceBody](); a.destinationType = a.sourceType }, false, false},
		{"pointer member", func(a *Argument) {
			a.sourceType = reflect.TypeFor[*pointerSliceBody]()
			a.destinationType = a.sourceType
		}, false, false},

		{"optional", func(a *Argument) {
			a.sourceType = reflect.TypeFor[*optionalSliceBody]()
			a.destinationType = a.sourceType
			a.publicName = "values"
		}, false, true},
		{"renamed", func(a *Argument) {
			a.sourceType = reflect.TypeFor[*renamedSliceBody]()
			a.destinationType = a.sourceType
			a.publicName = "values"
		}, true, true},
		{"case mismatch", func(a *Argument) { a.publicName = "VALUES" }, false, false},
		{"named body", func(a *Argument) { a.sourceName = "data" }, false, false},
		{"query", func(a *Argument) { a.sourceKind = "query" }, false, false},
		{"no wire", func(a *Argument) { a.wireType = nil }, false, false},
		{"conversion", func(a *Argument) { a.destinationType = reflect.TypeFor[*optionalSliceBody]() }, false, false},
		{"different element", func(a *Argument) { a.wireType = reflect.TypeFor[[]int64]() }, false, false},
		{"schema override", func(a *Argument) { a.wireSchema = &spec.WireSchema{Type: "array"} }, false, false},
		{"nested override", func(a *Argument) { a.wireSchemas = map[string]*spec.WireSchema{"Values": {Type: "array"}} }, false, false},
		{"multiple", func(a *Argument) { a.sourceType = reflect.TypeFor[*twoSliceBody](); a.destinationType = a.sourceType }, false, false},
		{"hidden", func(a *Argument) {
			a.sourceType = reflect.TypeFor[*hiddenSliceBody]()
			a.destinationType = a.sourceType
		}, false, false},
		{"embedded", func(a *Argument) {
			a.sourceType = reflect.TypeFor[*embeddedSliceBody]()
			a.destinationType = a.sourceType
		}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := base
			tc.change(&a)
			required, associated := wholeBodyMemberSchemaRequired(a)
			if required != tc.required || associated != tc.associated {
				t.Fatalf("got %t,%t want %t,%t", required, associated, tc.required, tc.associated)
			}
			if a.required {
				t.Fatal("runtime required changed")
			}
		})
	}
}
