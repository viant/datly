package reader

import (
	"github.com/viant/datly/v1/data"
	"testing"
)

func TestBuilder_Build(t *testing.T) {
	testCases := []struct {
		description string
		view        *data.View
		expectedSql string
	}{
		{
			description: "specified columns",
			view: &data.View{
				Table: "FOOS",
				Default: &data.Config{
					Columns: []string{"name", "price", "id"},
				},
			},
			expectedSql: "SELECT name, price, id FROM (FOOS)",
		},
		{
			description: "from as source",
			view: &data.View{
				From: "SELECT 'Foo' as name, 123.5 as price, 1 as id",
				Default: &data.Config{
					Columns: []string{"name", "price", "id"},
				},
			},
			expectedSql: "SELECT name, price, id FROM (SELECT 'Foo' as name, 123.5 as price, 1 as id)",
		},
		{
			description: "table with alias",
			view: &data.View{
				From:  "Foos",
				Alias: "foo_alias",
				Default: &data.Config{
					Columns: []string{"name", "price", "id"},
				},
			},
			expectedSql: "SELECT name, price, id FROM (Foos) AS foo_alias",
		},
		{
			description: "limit",
			view: &data.View{
				From: "Foos",
				Default: &data.Config{
					Columns: []string{"name", "price", "id"},
					Limit:   10,
				},
			},
			expectedSql: "SELECT name, price, id FROM (Foos) LIMIT 10",
		},
		{
			description: "Order by",
			view: &data.View{
				From: "Foos",
				Default: &data.Config{
					Columns: []string{"name", "price", "id"},
					OrderBy: "name",
				},
			},
			expectedSql: "SELECT name, price, id FROM (Foos) ORDER BY name",
		},
		{
			description: "offset",
			view: &data.View{
				From: "Foos",
				Default: &data.Config{
					Columns: []string{"name", "price", "id"},
					Offset:  10,
				},
			},
			expectedSql: "SELECT name, price, id FROM (Foos) OFFSET 10",
		},
		{
			description: "more complex Default",
			view: &data.View{
				From:  `SELECT "foo" as name, 123.5 as price, 1 as id`,
				Alias: "foos",
				Default: &data.Config{
					Columns: []string{"name", "price", "id"},
					OrderBy: "name",
					Limit:   100,
					Offset:  10,
				},
			},
			expectedSql: `SELECT name, price, id FROM (SELECT "foo" as name, 123.5 as price, 1 as id) AS foos ORDER BY name LIMIT 100 OFFSET 10`,
		},
		{
			description: "column expression",
			view: &data.View{
				From:  `SELECT "foo" as name, 123.5 as price, 1 as id`,
				Alias: "foos",
				Columns: []*data.Column{
					{
						Name:       "name",
						DataType:   "string",
						Expression: "Uppercase(name)",
					},
				},
				Default: &data.Config{
					Columns: []string{"name"},
				},
			},
			expectedSql: `SELECT Uppercase(name) AS name FROM (SELECT "foo" as name, 123.5 as price, 1 as id) AS foos`,
		},
	}

	for _ = range testCases {
		//builder := NewBuilder()
		//assert.Equal(t, testCase.expectedSql, builder.Build(testCase.view))
	}
}
