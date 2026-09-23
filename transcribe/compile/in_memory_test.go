package compile

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestReaderCompileInMemoryRelation(t *testing.T) {
	const query = `SELECT p.*, signals.*, perf.* %s
FROM parents p
JOIN (SELECT 0 AS parent_id, '' AS feature_type, '' AS feature_value) signals ON p.id = signals.parent_id
JOIN performance perf ON signals.feature_type = perf.feature_type AND signals.feature_value = perf.feature_value`
	for _, enabled := range []bool{false, true} {
		directive := ""
		if enabled {
			directive = ", in_memory(signals)"
		}
		root, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Parents"}, SQL: fmt.Sprintf(query, directive)})
		require.NoError(t, err)
		require.Len(t, root.Relations, 1)
		child := root.Relations[0].View
		require.Equal(t, enabled, child.InMemory)
		require.NotEmpty(t, child.Source.SQL, "discovery source must survive compilation")
		require.Len(t, child.Relations, 1)
		require.Len(t, child.Relations[0].On, 2)
		require.False(t, child.Relations[0].View.InMemory)
		require.Equal(t, enabled, root.Clone().Relations[0].View.InMemory)
		require.NotContains(t, root.Source.SQL, "in_memory(")
	}
}

func TestReaderCompileRejectsInvalidInMemory(t *testing.T) {
	for _, directive := range []string{
		"in_memory(p)", "in_memory(missing)", "in_memory(c, true)",
		"in_memory(c), in_memory(c)", "COALESCE(in_memory(c), 0)",
	} {
		t.Run(directive, func(t *testing.T) {
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Parents"}, SQL: "SELECT p.*, c.*, " + directive + " FROM parents p JOIN children c ON p.id = c.parent_id"})
			require.Error(t, err)
		})
	}
}
