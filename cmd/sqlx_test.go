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
					FROM T1 ad 
					JOIN T2 au ON au.X_ID = ad.ID`,
		},
	}

	for _, testCase := range testCases {
		table, err := ParseSQLx(testCase.SQL)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.NotNil(t, table)
	}

}
