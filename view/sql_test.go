package view

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectColumnsSQL(t *testing.T) {
	testcases := []struct {
		description string
		view        *View
		sql         string
	}{
		{
			description: `From`,
			view: &View{
				From:  "SELECT * FROM FOOS",
				Alias: "t",
			},
			sql: `SELECT * FROM FOOS 

 WHERE 1=0 `,
		},
		{
			description: `Criteria`,
			view: &View{
				From:  "SELECT * FROM FOOS $WHERE_CRITERIA",
				Alias: "t",
			},
			sql: `SELECT * FROM FOOS 

 WHERE 1=0 `,
		},
		{
			description: `Criteria with where`,
			view: &View{
				From:  "SELECT * FROM FOOS  WHERE Id = 10 $CRITERIA",
				Alias: "t",
			},
			sql: `SELECT * FROM FOOS  WHERE Id = 10  

 AND 1=0 `,
		},
		{
			description: `Criteria with where`,
			view: &View{
				From: `SELECT * FROM FOOS  WHERE Id = 10
--- this is comment
GROUP BY 1
`,
				Alias: "t",
			},
			sql: `SELECT * FROM FOOS  WHERE Id = 10
--- this is comment 

 AND 1=0 
GROUP BY 1`,
		},
	}

	for _ = range testcases {
		//sql, err := DetectColumns(testcase.View.Source(), testcase.View)
		//if !assert.Nil(t, err, testcase.description) {
		//	continue
		//}
		//
		//assert.Equal(t, testcase.sql, sql, testcase.description)
	}
}

func TestNeutralizePredicateBuilderForDiscovery(t *testing.T) {
	input := `SELECT * FROM FOO t WHERE 1=1 ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("AND")}`
	actual := neutralizePredicateBuilderForDiscovery(input)
	require.NotContains(t, actual, `${predicate.Builder()`)
	require.Contains(t, actual, `AND 1=0`)
}
