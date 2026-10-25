package tag

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestMySQLExecutionOptionTagRoundTrip(t *testing.T) {
	flag := false
	options := &spec.ExecutionOptions{MySQL: &spec.MySQLExecutionOptions{ConditionFanoutFilter: &flag}}
	text, err := (View{Name: "audience", ExecutionOptions: options}).Value()
	require.NoError(t, err)
	require.Equal(t, "audience,mysqlConditionFanoutFilter=false", text)
	parsed, err := ParseView(text)
	require.NoError(t, err)
	require.Equal(t, options, parsed.ExecutionOptions)
	for _, invalid := range []string{"audience,mysqlConditionFanoutFilter=true", "audience,mysqlConditionFanoutFilter=false,mysqlConditionFanoutFilter=false"} {
		_, err := ParseView(invalid)
		require.Error(t, err)
	}
	flag = true
	_, err = (View{Name: "audience", ExecutionOptions: options}).Value()
	require.ErrorContains(t, err, "only false")
}
