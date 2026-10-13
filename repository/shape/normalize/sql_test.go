package normalize

import (
	"testing"

	"github.com/stretchr/testify/require"
	legacy "github.com/viant/datly/cmd/options"
	"github.com/viant/sqlparser"
)

func parserOption() sqlparser.Option {
	return sqlparser.WithErrorHandler(nil)
}

func TestSQL_ParityWithLegacyNormalizer(t *testing.T) {
	type normalizeCase struct {
		Name      string
		Generated bool
		SQL       string
	}
	cases := []normalizeCase{
		{
			Name:      "skip normalization when not generated",
			Generated: false,
			SQL:       "SELECT a.id FROM users a JOIN orders b ON a.id = b.user_id",
		},
		{
			Name:      "invalid sql returns input",
			Generated: true,
			SQL:       "SELECT * FROM (",
		},
		{
			Name:      "normalize from and join aliases in selectors and alias nodes",
			Generated: true,
			SQL:       "SELECT a.id, b.user_id FROM users a JOIN orders b ON a.id = b.user_id",
		},
		{
			Name:      "keep alias that is already normalized",
			Generated: true,
			SQL:       "SELECT UserAlias.id FROM users UserAlias",
		},
		{
			Name:      "normalize snake_case alias",
			Generated: true,
			SQL:       "SELECT order_item.id FROM users order_item",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.Name, func(t *testing.T) {
			expected := (&legacy.Rule{Generated: testCase.Generated}).NormalizeSQL(testCase.SQL, parserOption)
			actual := SQL(testCase.SQL, testCase.Generated, parserOption)
			require.Equal(t, expected, actual)
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
