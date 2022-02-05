package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestView_MergeWithSelector(t *testing.T) {
	testCases := []struct {
		description       string
		view              *View
		selector          *Selector
		shouldReturnError bool
	}{
		{
			description: "merge when all selector columns overlap View columns",
			view: &View{
				Columns: []*Column{
					{
						Name: "name",
					},
					{
						Name: "price",
					},
					{
						Name: "id",
					},
				},
			},
			selector: &Selector{
				Columns: []string{"name", "price"},
				OrderBy: "name",
			},
			shouldReturnError: false,
		},
		{
			description: "selector columns doesn't overlap View columns",
			view: &View{
				Columns: []*Column{
					{
						Name: "name",
					},
					{
						Name: "price",
					},
					{
						Name: "id",
					},
				},
			},
			selector: &Selector{
				Columns: []string{"abcdef", "price"},
				OrderBy: "name",
			},
			shouldReturnError: true,
		},
		{
			description: "selector order by doesn't overlap View columns",
			view: &View{
				Columns: []*Column{
					{
						Name: "name",
					},
					{
						Name: "price",
					},
					{
						Name: "id",
					},
				},
			},
			selector: &Selector{
				Columns: []string{"name", "price"},
				OrderBy: "abcdef",
			},
			shouldReturnError: true,
		},
	}

	for _, testCase := range testCases {
		newView, err := testCase.view.MergeWithSelector(testCase.selector)
		if !testCase.shouldReturnError {
			assert.Nil(t, err, testCase.description)
			view := *testCase.view
			view.Selector = *testCase.selector
			assert.EqualValues(t, &view, newView, testCase.description)
			continue
		}

		assert.NotNil(t, err, testCase.description)
		assert.Nil(t, newView, testCase.description)
	}
}
