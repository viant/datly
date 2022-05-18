package ast

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHasWhere(t *testing.T) {
	testcases := []struct {
		description string
		input       string
		contains    bool
	}{
		{
			description: `contains where`,
			input:       `SELECT * FROM FOOS WHERE 1=2`,
			contains:    true,
		},
		{
			description: `parentheses`,
			input:       `((SELECT * FROM FOOS WHERE 1=2))`,
			contains:    true,
		},
		{
			description: `parentheses #2`,
			input:       `SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) () () () () WHERE 1=1`,
			contains:    true,
		},
		{
			description: `parentheses #3`,
			input:       `SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) () () () ()`,
			contains:    false,
		},
		{
			description: `without where`,
			input:       `((SELECT * FROM FOOS))`,
			contains:    false,
		},
		{
			description: `inner select, without where`,
			input:       `SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10)`,
			contains:    false,
		},
		{
			description: `inner select, with where #1`,
			input:       `SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) WHERE 1=1`,
			contains:    true,
		},
		{
			description: `inner select, with where #2`,
			input: `SELECT * FROM (
SELECT * FROM EVENTS
) WHERE (id = 1 OR id = 2) $CRITERIA`,
			contains: true,
		},
	}

	for _, testcase := range testcases {
		assert.Equal(t, testcase.contains, ContainsWhereClause([]byte(testcase.input)), testcase.description)
	}
}
