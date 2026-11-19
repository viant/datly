package sanitize

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/velty"
)

type criteriaContextMock struct{}

type criteriaContextCollector struct {
	Args []interface{}
}

func (c *criteriaContextCollector) AppendBinding(value interface{}) string {
	c.Args = append(c.Args, value)
	return "?"
}

type predicateContextMock struct{}

func (p predicateContextMock) Builder() *predicateBuilderContextMock {
	return &predicateBuilderContextMock{}
}

func (p predicateContextMock) FilterGroup(group int, op string) string {
	return fmt.Sprintf("P%d:%s", group, op)
}

type predicateBuilderContextMock struct {
	value string
}

func (b *predicateBuilderContextMock) CombineOr(group string) *predicateBuilderContextMock {
	b.value = group
	return b
}

func (b *predicateBuilderContextMock) Build(kind string) string {
	switch kind {
	case "AND":
		return " AND (" + b.value + ") "
	case "WHERE":
		return " WHERE (" + b.value + ") "
	default:
		return ""
	}
}

type sqlContextMock struct{}

func (s sqlContextMock) Eq(column string, value interface{}) string {
	return fmt.Sprintf("%s = %v", column, value)
}

type unsafeContextMock struct {
	VendorID int
}

func TestRenderVelty_WithShapeContext_DataDriven(t *testing.T) {
	testCases := []struct {
		name     string
		template string
		expect   string
		args     []interface{}
	}{
		{
			name:     "criteria append binding",
			template: "SELECT * FROM VENDOR t WHERE t.ID = $criteria.AppendBinding($Unsafe.VendorID)",
			expect:   "t.ID = ?",
			args:     []interface{}{101},
		},
		{
			name:     "predicate builder chain",
			template: "SELECT * FROM PRODUCT t WHERE 1=1 ${predicate.Builder().CombineOr($predicate.FilterGroup(0, \"AND\")).Build(\"AND\")}",
			expect:   " AND (P0:AND) ",
		},
		{
			name:     "sql helper",
			template: "SELECT * FROM VENDOR t WHERE $sql.Eq(\"ID\", $Unsafe.VendorID)",
			expect:   "ID = 101",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual, args := renderVeltyWithShapeContext(t, testCase.template)
			assert.Contains(t, actual, testCase.expect)
			if len(testCase.args) > 0 {
				assert.Equal(t, testCase.args, args)
			}
		})
	}
}

func renderVeltyWithShapeContext(t *testing.T, template string) (string, []interface{}) {
	t.Helper()
	planner := velty.New()
	require.NoError(t, planner.DefineVariable("criteria", &criteriaContextCollector{}))
	require.NoError(t, planner.DefineVariable("predicate", predicateContextMock{}))
	require.NoError(t, planner.DefineVariable("sql", sqlContextMock{}))
	require.NoError(t, planner.DefineVariable("Unsafe", unsafeContextMock{}))

	exec, newState, err := planner.Compile([]byte(template))
	require.NoError(t, err)

	state := newState()
	criteria := &criteriaContextCollector{}
	require.NoError(t, state.SetValue("criteria", criteria))
	require.NoError(t, state.SetValue("predicate", predicateContextMock{}))
	require.NoError(t, state.SetValue("sql", sqlContextMock{}))
	require.NoError(t, state.SetValue("Unsafe", unsafeContextMock{VendorID: 101}))
	require.NoError(t, exec.Exec(state))
	return state.Buffer.String(), criteria.Args
}
