package inference

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlparser"
)

func TestJoinRelationExtraction(t *testing.T) {
	testCases := []struct {
		description string
		sql         string
		wantParent  string
		wantRelCol  string
		wantRefCol  string
	}{
		{
			description: "simple join",
			sql:         "SELECT * FROM a a JOIN b b ON a.brand = b.b_brand",
			wantParent:  "a",
			wantRelCol:  "brand",
			wantRefCol:  "b_brand",
		},
		{
			description: "join with function on parent",
			sql:         "SELECT * FROM a a JOIN b b ON lower(a.brand) = b.b_brand",
			wantParent:  "a",
			wantRelCol:  "brand",
			wantRefCol:  "b_brand",
		},
		{
			description: "join with collate and multiple conditions",
			sql: "SELECT * FROM a a JOIN b b ON " +
				"a.brand COLLATE utf8mb4_bin = b.b_brand COLLATE utf8mb4_bin AND " +
				"a.model COLLATE utf8mb4_bin = b.b_model COLLATE utf8mb4_bin",
			wantParent: "a",
			wantRelCol: "brand",
			wantRefCol: "b_brand",
		},
	}

	for _, testCase := range testCases {
		q, err := sqlparser.ParseQuery(testCase.sql)
		require.NoError(t, err, testCase.description)
		require.NotEmpty(t, q.Joins, testCase.description)

		join := q.Joins[0]
		parent := ParentAlias(join)
		require.Equal(t, testCase.wantParent, parent, testCase.description)

		relCol, refCol := ExtractRelationColumns(join)
		require.Equal(t, testCase.wantRelCol, relCol, testCase.description)
		require.Equal(t, testCase.wantRefCol, refCol, testCase.description)
	}
}
