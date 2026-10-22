package readergraph

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

func TestParameterFieldBindingAlias(t *testing.T) {
	type output struct {
		Data []int `parameter:"view,kind=output,in=view"`
	}
	type ambiguous struct {
		Data  []int `parameter:"view,kind=output,in=view"`
		Other []int `parameter:"view,kind=output,in=view"`
	}
	type useCase struct {
		description, input, expected string
		shape                        reflect.Type
		wantError                    bool
	}
	for _, tc := range []useCase{
		{"logical name", "view", "Data", reflect.TypeFor[output](), false},
		{"Go field identity", "Data", "Data", reflect.TypeFor[output](), false},
		{"missing field", "Missing", "", reflect.TypeFor[output](), false},
		{"ambiguous alias", "view", "", reflect.TypeFor[ambiguous](), true},
	} {
		t.Run(tc.description, func(t *testing.T) {
			fields, err := xshape.Linked(tc.shape).Fields()
			if err != nil {
				t.Fatal(err)
			}
			field, err := ParameterField(fields, &spec.Parameter{Name: tc.input})
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v", err)
			}
			if tc.wantError {
				return
			}
			actual := ""
			if field != nil {
				actual = field.Name
			}
			if actual != tc.expected {
				t.Fatalf("field = %q, want %q", actual, tc.expected)
			}
		})
	}
}
