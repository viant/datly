package options

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testutil/sqlnormalizer"
	"github.com/viant/sqlparser"
)

func parserOption() sqlparser.Option {
	return sqlparser.WithErrorHandler(nil)
}

func TestRule_NormalizeSQL(t *testing.T) {
	for _, testCase := range sqlnormalizer.Cases() {
		t.Run(testCase.Name, func(t *testing.T) {
			rule := &Rule{Generated: testCase.Generated}
			actual := rule.NormalizeSQL(testCase.SQL, parserOption)
			require.Equal(t, testCase.Expect, actual)
		})
	}
}

func TestMapper_Map(t *testing.T) {
	m := mapper{"a": "A"}
	require.Equal(t, "A", m.Map("a"))
	require.Equal(t, "b", m.Map("b"))
}

func TestNormalizeName(t *testing.T) {
	require.Equal(t, "UserAlias", normalizeName("user_alias"))
	require.Equal(t, "UserAlias", normalizeName("UserAlias"))
}
