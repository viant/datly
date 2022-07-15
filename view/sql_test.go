package view

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
			sql: `SELECT * FROM FOOS  WHERE 1=0 `,
		},
		{
			description: `Criteria`,
			view: &View{
				From:  "SELECT * FROM FOOS $WHERE_CRITERIA",
				Alias: "t",
			},
			sql: `SELECT * FROM FOOS  WHERE 1=0 `,
		},
		{
			description: `Criteria with where`,
			view: &View{
				From:  "SELECT * FROM FOOS  WHERE id = 10 $CRITERIA",
				Alias: "t",
			},
			sql: `SELECT * FROM FOOS  WHERE id = 10   AND 1=0 `,
		},
	}

	for _, testcase := range testcases {
		sql, _ := DetectColumnsSQL(testcase.view.Source(), testcase.view)
		assert.Equal(t, testcase.sql, sql, testcase.description)
	}
}
