package tags

import (
	"embed"
	_ "embed"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

//go:embed testdata/*
var embedFS embed.FS

func TestTag_updateView(t *testing.T) {
	var testCases = []struct {
		description string
		tag         reflect.StructTag
		expectView  *View
		expectSQL   ViewSQL
		expectTag   string
	}{

		{
			description: "basic view",
			tag:         `view:"foo,table=FOO,connector=dev"`,
			expectView:  &View{Name: "foo", Table: "FOO", Connector: "dev"},
		},
		{
			description: "basic view",
			tag:         `view:"foo,connector=dev"  sql:"uri=testdata/foo.sql"`,
			expectView:  &View{Name: "foo", Connector: "dev"},
			expectSQL:   ViewSQL{SQL: "SELECT * FROM FOO", URI: "testdata/foo.sql"},
			expectTag:   "foo,connector=dev",
		},
		{
			description: "parameters view",
			tag:         `view:"foo,table=FOO,connector=dev,parameters={P1,P2}"`,
			expectView:  &View{Name: "foo", Table: "FOO", Connector: "dev", Parameters: []string{"P1", "P2"}},
		},
		{
			description: "selector metadata view",
			tag:         `view:"foo,table=FOO,groupable=true,selectorNamespace=ve,selectorCriteria=true,selectorProjection=true,selectorOrderBy=true,selectorOffset=true,selectorFilterable={*},selectorOrderByColumns={accountId:ACCOUNT_ID,userCreated:USER_CREATED}"`,
			expectView: &View{
				Name:                   "foo",
				Table:                  "FOO",
				Groupable:              boolPtr(true),
				SelectorNamespace:      "ve",
				SelectorCriteria:       boolPtr(true),
				SelectorProjection:     boolPtr(true),
				SelectorOrderBy:        boolPtr(true),
				SelectorOffset:         boolPtr(true),
				SelectorFilterable:     []string{"*"},
				SelectorOrderByColumns: map[string]string{"accountId": "ACCOUNT_ID", "userCreated": "USER_CREATED"},
			},
			expectTag: "foo,table=FOO,groupable=true,selectorNamespace=ve,selectorCriteria=true,selectorProjection=true,selectorOrderBy=true,selectorOffset=true,selectorFilterable={*},selectorOrderByColumns={accountId:ACCOUNT_ID,userCreated:USER_CREATED}",
		},
		{
			description: "summary uri view",
			tag:         `view:"foo,table=FOO,summaryURI=testdata/foo_summary.sql"`,
			expectView:  &View{Name: "foo", Table: "FOO", SummaryURI: "testdata/foo_summary.sql"},
			expectTag:   "foo,table=FOO,summaryURI=testdata/foo_summary.sql",
		},
	}

	for _, testCase := range testCases {
		actual, err := Parse(testCase.tag, &embedFS, ViewTag, SQLTag, SQLSummaryTag)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expectView, actual.View, testCase.description)
		expectTag := testCase.expectTag
		if expectTag == "" {
			expectTag = testCase.tag.Get(ViewTag)
		}
		if testCase.expectSQL.SQL != "" {
			assert.EqualValues(t, testCase.expectSQL, actual.SQL, testCase.description)
		}
		assert.EqualValues(t, expectTag, string(actual.View.Tag().Values), testCase.description)
	}
}

func boolPtr(v bool) *bool {
	return &v
}
