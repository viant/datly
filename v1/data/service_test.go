package data

import (
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/v1/config"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"testing"
)

func TestConfigure(t *testing.T) {
	testCases := []struct {
		description string
		references  []*Reference
		connectors  []*config.Connector
		views       []*View
		expectError bool
	}{
		{
			description: "references",
			references: []*Reference{
				{
					Name: "employee_departments",
				},
				{
					Name: "department_address",
				},
			},
			views: []*View{
				{
					Name:      "addresses",
					Connector: "mydb",
				},
				{
					Name:      "departments",
					Connector: "mydb",
				},
				{
					Name:      "foos",
					Connector: "mydb",
					Columns: []*Column{
						{Name: "Id"},
					},
				},
			},
			connectors: []*config.Connector{
				{
					Name:   "mydb",
					Driver: "sqlite3",
					DSN:    "./testdata/db/mydb.db",
				},
			},
		},
	}

	testLocation := toolbox.CallerDirectory(3)
	for _, testCase := range testCases {
		if !dsunit.InitFromURL(t, path.Join(testLocation, "testdata", "config.yaml")) {
			return
		}
		metaService, err := Configure(&Resource{
			Connectors: testCase.connectors,
			Views:      testCase.views,
			References: testCase.references,
		})
		if testCase.expectError {
			assert.NotNil(t, err, testCase.description)
			continue
		}

		assert.Nil(t, err, testCase.description)
		for _, view := range metaService.views {
			assert.True(t, len(view.Columns) != 0, testCase.description)
			assert.True(t, view.Columns != nil, testCase.description)
			assert.True(t, len(view.Default.Columns) > 0, testCase.description)
		}
	}
}
