package reader

import (
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestNewPlanValidatesExecutableDependencyGraph(t *testing.T) {
	for _, test := range []struct {
		name            string
		cycle, negative bool
	}{{"shared_dag", false, false}, {"dependency_cycle", true, false}, {"negative_batch_concurrency", false, true}} {
		t.Run(test.name, func(t *testing.T) {
			root := &data.View{Spec: spec.View{Name: "root"}}
			child := &data.View{Spec: spec.View{Name: "child"}}
			root.Relations = []*data.Relation{{Name: "first", Of: &data.RelationRef{View: child}}, {Name: "second", Of: &data.RelationRef{View: child}}}
			if test.cycle {
				child.Relations = []*data.Relation{{Name: "back", Of: &data.RelationRef{View: root}}}
			}
			if test.negative {
				child.Spec.BatchConcurrency = -1
			}
			_, err := NewPlan(PlanConfig{RootView: root})
			if test.cycle {
				if err == nil || !strings.Contains(err.Error(), "dependency cycle") {
					t.Fatalf("cycle error %v", err)
				}
				return
			}
			if test.negative {
				if err == nil || !strings.Contains(err.Error(), "batch concurrency") {
					t.Fatalf("concurrency error %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
