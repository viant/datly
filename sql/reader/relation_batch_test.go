package reader

import (
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestRelationRead_BatchesScalarAndCompositeKeys(t *testing.T) {
	reader := &relationRead{plan: &ViewPlan{View: &data.View{Spec: spec.View{BatchSize: 2}}}}
	tests := []struct {
		name      string
		scalar    []interface{}
		composite [][]interface{}
		lengths   []int
	}{
		{name: "scalar", scalar: []interface{}{1, 2, 3, 4, 5}, lengths: []int{2, 2, 1}},
		{name: "composite", composite: [][]interface{}{{"a", 1}, {"a", 2}, {"b", 1}, {"b", 2}, {"c", 1}}, lengths: []int{2, 2, 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batches := reader.batches(test.scalar, test.composite)
			if len(batches) != len(test.lengths) {
				t.Fatalf("unexpected batches: %#v", batches)
			}
			for i, expected := range test.lengths {
				actual := len(batches[i].placeholders)
				if len(test.composite) > 0 {
					actual = len(batches[i].composite)
				}
				if actual != expected {
					t.Fatalf("batch %d length: got %d, want %d", i, actual, expected)
				}
			}
		})
	}
}
