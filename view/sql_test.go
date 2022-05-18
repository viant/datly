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
			sql: `SELECT t.* FROM (SELECT * FROM FOOS) t WHERE 1=0`,
		},
		{
			description: `Criteria`,
			view: &View{
				From:  "SELECT * FROM FOOS $CRITERIA",
				Alias: "t",
			},
			sql: `SELECT t.* FROM (SELECT * FROM FOOS  WHERE 1 = 0) t WHERE 1=0`,
		},
		{
			description: `Criteria with where`,
			view: &View{
				From:  "SELECT * FROM FOOS  WHERE id = 10 $CRITERIA",
				Alias: "t",
			},
			sql: `SELECT t.* FROM (SELECT * FROM FOOS  WHERE id = 10  AND 1 = 0) t WHERE 1=0`,
		},
	}

	for _, testcase := range testcases {
		assert.Equal(t, testcase.sql, detectColumnsSQL(testcase.view.Source(), testcase.view), testcase.description)
	}
}
