package view

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlparser"
	"github.com/viant/tagly/format/text"
)

func TestNewColumns_AliasKeepsNativeSQLXColumn(t *testing.T) {
	columns := NewColumns(sqlparser.Columns{
		&sqlparser.Column{Name: "NAME", Type: "string"},
	}, map[string]*ColumnConfig{
		"NAME": {Name: "NAME", Alias: "SITE_NAME"},
	})

	require.Len(t, columns, 1)
	assert.Equal(t, "SITE_NAME", columns[0].Name)
	assert.Contains(t, columns[0].Tag, `sqlx:"NAME"`)
	assert.Equal(t, "NAME", columns[0].DatabaseColumn)
}

func TestColumnsSchema_AliasedRelationsKeepNativeSQLXNames(t *testing.T) {
	testCases := []struct {
		name     string
		columns  sqlparser.Columns
		config   map[string]*ColumnConfig
		expected map[string]string
	}{
		{
			name: "SiteView",
			columns: sqlparser.Columns{
				&sqlparser.Column{Name: "NAME", Type: "string", RawType: reflect.TypeOf(""), IsNullable: true},
			},
			config: map[string]*ColumnConfig{
				"NAME": {Name: "NAME", Alias: "SITE_NAME"},
			},
			expected: map[string]string{"SiteName": "NAME"},
		},
		{
			name: "PublisherView",
			columns: sqlparser.Columns{
				&sqlparser.Column{Name: "ID", Type: "int", RawType: reflect.TypeOf(0)},
				&sqlparser.Column{Name: "NAME", Type: "string", RawType: reflect.TypeOf("")},
			},
			config: map[string]*ColumnConfig{
				"ID":   {Name: "ID", Alias: "PUBLISHER_ID"},
				"NAME": {Name: "NAME", Alias: "PUBLISHER_NAME"},
			},
			expected: map[string]string{"PublisherId": "ID", "PublisherName": "NAME"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			columns := NewColumns(testCase.columns, testCase.config)
			rType, err := ColumnsSchema(text.CaseFormatUpperUnderscore, columns, nil, &View{}, nil)()
			require.NoError(t, err)

			for fieldName, sqlxName := range testCase.expected {
				field, ok := rType.Elem().FieldByName(fieldName)
				require.True(t, ok, "missing generated field %s", fieldName)
				assert.Equal(t, sqlxName, field.Tag.Get("sqlx"))
				assert.NotContains(t, field.Tag.Get("sqlx"), "|")
			}
		})
	}
}
