package compile

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestMySQLExecutionDirective(t *testing.T) {
	base := &spec.View{Name: "audience", Source: &spec.ViewSource{}}
	for _, tc := range []struct{ controls, failure string }{
		{"mysql_condition_fanout_filter(a, false)", ""},
		{"mysql_condition_fanout_filter(a, true)", "only false"},
		{"mysql_condition_fanout_filter(a, 'false')", "literals"},
		{"mysql_condition_fanout_filter(a, :Flag)", "literals"},
		{"mysql_condition_fanout_filter(missing, false)", "does not match"},
		{"mysql_condition_fanout_filter(a, false), mysql_condition_fanout_filter(a, false)", "more than once"},
		{"mysql_condition_fanout_filter(a)", "arguments"},
	} {
		t.Run(tc.controls, func(t *testing.T) {
			view, err := NewReader().Compile(ReadInput{View: base, SQL: "SELECT a.*, " + tc.controls + " FROM audience a"})
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, view.ExecutionOptions)
			require.False(t, *view.ExecutionOptions.MySQL.ConditionFanoutFilter)
			require.NotContains(t, view.Source.SQL, "mysql_condition_fanout_filter")
			require.NotContains(t, view.Source.SQL, "SET_VAR")
			require.Nil(t, base.ExecutionOptions)
			clone := view.Clone()
			*clone.ExecutionOptions.MySQL.ConditionFanoutFilter = true
			require.False(t, *view.ExecutionOptions.MySQL.ConditionFanoutFilter)
		})
	}
}

func TestMySQLExecutionDirectiveRejectsAuthoredConflict(t *testing.T) {
	_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "audience"}, SQL: "SELECT /*+ SET_VAR(optimizer_switch='condition_fanout_filter=on') */ a.*, mysql_condition_fanout_filter(a, false) FROM audience a"})
	require.ErrorContains(t, err, "conflicts")
}
