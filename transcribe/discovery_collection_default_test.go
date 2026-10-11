package transcribe

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestDiscoveryInputCompilerCollectionDefaults(t *testing.T) {
	for _, item := range []struct {
		name, expr, value string
		want              any
	}{
		{"singleton", "[]int", "[1]", []int{1}},
		{"ordered signed duplicates", "[]int", "[4,-3,4,0]", []int{4, -3, 4, 0}},
		{"empty remains non nil", "[]int", "[]", []int{}},
		{"null remains nil", "[]int", "null", []int(nil)},
		{"pointer collection", "*[]int", "[1]", slicePointer([]int{1})},
	} {
		t.Run(item.name, func(t *testing.T) {
			compiled, err := (&discoveryInputCompiler{component: &spec.Component{Name: "Status", Parameters: []*spec.Parameter{{Name: "IDs", TypeExpr: item.expr, Value: &item.value, Source: spec.BindSource{Kind: "query", Name: "ids"}}}}}).compile()
			if err != nil {
				t.Fatal(err)
			}
			if got := compiled.Value.FieldByName("IDs").Interface(); !reflect.DeepEqual(got, item.want) {
				t.Fatalf("got %#v, want %#v", got, item.want)
			}
		})
	}
}

func slicePointer(value []int) *[]int { return &value }

func TestDiscoveryInputCompilerCollectionDefaultsRejectInvalid(t *testing.T) {
	for _, value := range []string{`["bad"]`, `[1,]`, `1`} {
		_, err := (&discoveryInputCompiler{component: &spec.Component{Name: "Status", Parameters: []*spec.Parameter{{Name: "IDs", TypeExpr: "[]int", Value: &value, Source: spec.BindSource{Kind: "query", Name: "ids"}}}}}).compile()
		if err == nil {
			t.Fatalf("invalid default %q accepted", value)
		}
	}
}
