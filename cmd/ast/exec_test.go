package ast

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"testing"
)

func TestNormalizeSQLExec(t *testing.T) {

	var testCases = []struct {
		description string
		SQL         string
		expect      interface{}
	}{
		{
			description: "basic update",
			SQL: `

#set(z=1)

UPDATE AD_ORDER
SET STATUS = $Status
WHERE ID IN ($Ids);

`,
		},
	}

	for _, testCase := range testCases {
		var aView = &ViewMeta{index: map[string]int{}}
		err := buildViewMetaInExecSQLMode(testCase.SQL, aView, map[string]bool{})
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		toolbox.Dump(aView)

	}

}
