package generate

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestNativeWriterPresentationPhysicalGraphScope(t *testing.T) {
	for _, auxiliary := range []bool{false, true} {
		root := &spec.View{Name: "body", Auxiliary: auxiliary}
		child := &spec.View{Name: "child"}
		derived := &spec.View{Name: "derived"}
		memory := &spec.View{Name: "memory", InMemory: true}
		current := &spec.View{Name: "Current"}
		root.Relations = []*spec.Relation{{View: child}, {View: derived, Kind: spec.RelationKindDerived}, {View: memory}, nil}
		child.Relations = []*spec.Relation{{View: root}}
		graph := nativeWriterPresentationGraph(root)
		require.Len(t, graph, 2)
		require.True(t, graph[root])
		require.True(t, graph[child])
		require.False(t, graph[derived])
		require.False(t, graph[memory])
		require.False(t, graph[current])
		if !auxiliary {
			require.Nil(t, ordinaryBodyGraph(root), "presentation must not expand ownership")
		}
		plan := &Plan{Settings: dtag.Settings{Mutation: "patch", CaseFormat: "lc"}, Generation: &spec.GenerationSettings{}}
		for _, v := range []*spec.View{root, child, current, derived} {
			fs := []Field{{Name: "RowId"}, {Name: "Alias", Tag: `json:"legacy_name,omitempty"`}, {Name: "Optional", Tag: `json:",omitempty"`}, {Name: "Present", Tag: `json:""`}}
			if graph[v] {
				require.NoError(t, applyBodyJSONTags(plan, fs, v))
				require.Empty(t, reflect.StructTag(fs[0].Tag).Get("json"))
			} else {
				require.NoError(t, applyInferredJSONTags(plan, fs, v))
				require.Equal(t, "rowId", reflect.StructTag(fs[0].Tag).Get("json"))
			}
			require.Equal(t, "legacy_name,omitempty", reflect.StructTag(fs[1].Tag).Get("json"))
			if graph[v] {
				require.Equal(t, ",omitempty", reflect.StructTag(fs[2].Tag).Get("json"))
				_, ok := reflect.StructTag(fs[3].Tag).Lookup("json")
				require.True(t, ok)
				require.Empty(t, reflect.StructTag(fs[3].Tag).Get("json"))
			}
		}
	}
}
