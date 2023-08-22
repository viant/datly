package template

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParse(t *testing.T) {

	var testCases = []struct {
		description string
		SQL         string
		Expect      []*Value
	}{
		{
			description: "SQL with single quote",
			SQL: `SELECT DISTINCT (SELECT IF(LOWER('portal.apex.com') = LOWER(''),
                                 (SELECT JSON_OBJECT(
                                                 'cssFile', 'adelphicpdp/images/apex/logo.ico',
                                                 'platformName', 'Apex Mobile',
                                                 'adCloudName', 'Apex Mobile',
                                                 'shortcutIconFile', 'css/wl-apex.css'
                                             )),
                                 NULL)) AS branding /* {"Type": "json.RawMessage"} */,
                      (SELECT IF(CI_SYSTEM_CONFIG.VALUE LIKE LOWER('%%'), 1, 0)
                       FROM CI_SYSTEM_CONFIG
                       where 'KEY' = 'sandboxDnsList'
                                           LIMIT 1)         AS sandbox /* {"Type": "bool"}  */ ,
    'UA-136108240-1'       AS gaSiteId,
    'http://ad.ipredictive.com/d/render/fasttrack?zid=shanghai_1_0_1&creativeId={0}' AS creativeFetchUrlPattern
FROM CI_SYSTEM_CONFIG 
WHERE 1=0  LIMIT 1 $PAGINATION `,

			Expect: []*Value{{Key: "PAGINATION", Fragment: "$PAGINATION"}},
		},
	}

	for _, testCase := range testCases {

		actual, err := Parse(testCase.SQL)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.Expect, actual)
	}

}
