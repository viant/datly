package cmd

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseSQLx(t *testing.T) {

	var testCases = []struct {
		description string
		SQL         string
	}{
		{
			SQL: `SELECT ad.*, au.* 
					FROM
(
)
		T1 ad 
					JOIN T2 au ON au.X_ID = ad.ID
					WHERE ad.id = 10
`,
		},
	}

	for _, testCase := range testCases {
		table, _, err := ParseSQLx(&Options{}, testCase.SQL)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.NotNil(t, table)
	}

}
