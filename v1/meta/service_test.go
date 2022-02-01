package meta

import (
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/v1/config"
	"github.com/viant/datly/v1/data"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"testing"
)

func TestConfigure(t *testing.T) {
	testCases := []struct {
		description string
		references  []*data.Reference
		connectors  []*config.Connector
		views       []*data.View
		relations   []*data.Relation
		expectError bool
	}{
		{
			description: "references",
			references: []*data.Reference{
				{
					Name: "employee_departments",
				},
				{
					Name: "department_address",
				},
			},
			relations: []*data.Relation{
				{
					RefId:     "department_address",
					ChildName: "addresses",
				},
			},
			views: []*data.View{
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
					Columns: []*data.Column{
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
		metaService, err := Configure(testCase.connectors, testCase.views, testCase.relations, testCase.references)
		if testCase.expectError {
			assert.NotNil(t, err, testCase.description)
			continue
		}

		assert.Nil(t, err, testCase.description)

		assert.Equal(t, len(testCase.relations), len(metaService.relations), testCase.description)
		for _, relation := range metaService.relations {
			assert.NotNil(t, relation.Child, testCase.description)
			assert.NotNil(t, relation.Ref, testCase.description)
		}

		for _, view := range metaService.views {
			assert.True(t, len(view.Columns) != 0, testCase.description)
			assert.True(t, view.Columns != nil, testCase.description)
			assert.True(t, len(view.Selector.Columns) > 0, testCase.description)
		}
	}
}
