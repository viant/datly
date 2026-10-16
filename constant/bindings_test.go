package constant_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
)

func TestInputConstantBindingPreservesExternalNamespace(t *testing.T) {
	defaultTable := "default-project.mdp.history"
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "HistoryTable", Source: spec.BindSource{Kind: "const", Name: "frequencyTargetHistoryTable"}, Value: &defaultTable}}}
	for _, override := range []bool{false, true} {
		instance := map[string]string{"HistoryTable": "unrelated-project.mdp.history"}
		want := defaultTable
		if override {
			want = "production-project.mdp.history"
			instance["frequencyTargetHistoryTable"] = want
		}
		values, err := constant.New(instance)
		require.NoError(t, err)
		effective, err := values.For(component)
		require.NoError(t, err)
		key, declared := effective.Binding("HISTORYTABLE")
		require.True(t, declared)
		require.Equal(t, "frequencyTargetHistoryTable", key)
		value, found := effective.LookupBound("historytable")
		require.True(t, found)
		require.Equal(t, want, value)
		value, found = effective.Lookup("HistoryTable")
		require.True(t, found)
		require.Equal(t, instance["HistoryTable"], value, "binding must not overwrite an external key")
		_, declared = values.Binding("HistoryTable")
		require.False(t, declared, "caller-owned values changed")
		require.Equal(t, defaultTable, *component.Parameters[0].Value)
		child, err := effective.For(&spec.Component{})
		require.NoError(t, err)
		_, declared = child.Binding("HistoryTable")
		require.False(t, declared, "parent declarations must not leak into children")
		require.NotEqual(t, effective.Identity(), child.Identity())
	}
}

func TestInputConstantBindingWithoutAuthoredDefault(t *testing.T) {
	values, err := constant.New(map[string]string{"physicalTable": "records"})
	require.NoError(t, err)
	effective, err := values.For(&spec.Component{Parameters: []*spec.Parameter{{Name: "Table", Source: spec.BindSource{Kind: "const", Name: "physicalTable"}}}})
	require.NoError(t, err)
	value, found := effective.LookupBound("Table")
	require.True(t, found)
	require.Equal(t, "records", value)
}
