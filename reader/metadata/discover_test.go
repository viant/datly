package metadata

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEnrichWithDiscover(t *testing.T) {
	testcases := []struct {
		description string
		input       string
		output      string
	}{
		{
			description: `contains where`,
			input:       `SELECT * FROM FOOS WHERE 1=2`,
			output:      `(SELECT * FROM FOOS WHERE 1=2 $AND_CRITERIA $PAGINATION)`,
		},

		{
			description: `parentheses #2`,
			input:       `SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) () () () () WHERE 1=1`,
			output:      `(SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) () () () () WHERE 1=1 $AND_CRITERIA $PAGINATION)`,
		},
		{
			description: `parentheses #3`,
			input:       `SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) () () () ()`,
			output:      `(SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) () () () () $WHERE_CRITERIA $PAGINATION)`,
		},
		{
			description: `without where`,
			input:       `(SELECT * FROM FOOS)`,
			output:      `(SELECT * FROM FOOS $WHERE_CRITERIA $PAGINATION)`,
		},
		{
			description: `inner select, without where`,
			input:       `SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10)`,
			output:      `(SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) $WHERE_CRITERIA $PAGINATION)`,
		},
		{
			description: `inner select, without where`,
			input:       `( (     (SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10))))`,
			output:      `(SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) $WHERE_CRITERIA $PAGINATION)`,
		},
		{
			description: `inner select, without where`,
			input:       `SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) WHERE 1 = 1 GROUP BY EVENT_NAME `,
			output:      `(SELECT * FROM (SELECT * FROM EVENTS WHERE ID = 10) WHERE 1 = 1 $AND_CRITERIA GROUP BY EVENT_NAME $PAGINATION)`,
		},
	}

	//for _, testcase := range testcases[len(testcases)-1:] {
	for _, testcase := range testcases {
		discover := EnrichWithDiscover(testcase.input, true)
		assert.Equal(t, testcase.output, discover, testcase.description)
	}
}
