package view

import (
	"context"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/codec"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"testing"
)

func TestNewResourceFromURL(t *testing.T) {
	testLocation := toolbox.CallerDirectory(3)

	testCases := []struct {
		description string
		url         string
		expect      string
	}{
		{
			url: "case001",
			expect: `{
	"Connectors": [
		{
			"DSN": "./testdata/db/mydb.db",
			"Driver": "sqlite3",
			"DbName": "mydb"
		}
	],
	"Views": [
		{
			"Alias": "t",
			"BatchReadSize": null,
			"CaseFormat": "lu",
			"Caser": 5,
			"Columns": [
				{
					"DataType": "Int",
					"Filterable": false,
					"DbName": "id"
				},
				{
					"DataType": "Float",
					"Filterable": false,
					"DbName": "quantity"
				},
				{
					"DataType": "Int",
					"Filterable": false,
					"DbName": "event_type_id"
				}
			],
			"Connector": {
				"DSN": "./testdata/db/mydb.db",
				"Driver": "sqlite3",
				"DbName": "mydb",
				"Ref": "mydb"
			},
			"MatchStrategy": "read_matched",
			"DbName": "events",
			"Schema": {
				"DbName": "events",
				"OmitEmpty": false
			},
			"Selector": {
				"Constraints": {
				"Columns": null,
				"Criteria": null,
				"Limit": null,
				"Offset": null,
				"OrderBy": null
				}
			},
			"Table": "events"
		}
	]
}`,
		},
	}

	for _, testCase := range testCases {
		if !dsunit.InitFromURL(t, path.Join(testLocation, "testdata/config.yaml")) {
			return
		}

		resource, err := NewResourceFromURL(context.TODO(), path.Join(testLocation, "testdata", testCase.url, "resource.yaml"), Types{}, codec.Visitors{})
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if !assertly.AssertValues(t, testCase.expect, resource, testCase.description) {
			toolbox.DumpIndent(resource, true)
		}
	}
}
