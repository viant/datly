package criteria_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/internal/tests"
	"github.com/viant/datly/router/criteria"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
	"reflect"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	testCases := []struct {
		description       string
		input             string
		columns           view.NamedColumns
		sanitizedCriteria string
		placeholders      []interface{}
		expectErr         bool
		methods           map[string]*view.Method
	}{
		{
			description: "boolean criteria | equal true",
			input:       "IsActive = true",
			columns: map[string]*view.Column{
				"IsActive": {Name: "is_active", DataType: "bool", Filterable: true},
			},
			placeholders:      []interface{}{true},
			sanitizedCriteria: ` is_active = ?`,
		},
		{
			description: "boolean criteria | equal false",
			input:       "IsActive = false",
			columns: map[string]*view.Column{
				"IsActive": {Name: "is_active", DataType: "bool", Filterable: true},
			},
			placeholders:      []interface{}{false},
			sanitizedCriteria: ` is_active = ?`,
		},
		{
			description: "boolean criteria | not equal true",
			input:       "IsActive != true",
			columns: map[string]*view.Column{
				"IsActive": {Name: "is_active", DataType: "bool", Filterable: true},
			},
			placeholders:      []interface{}{true},
			sanitizedCriteria: ` is_active <> ?`,
		},
		{
			description: "boolean criteria | invalid comparison token",
			input:       "IsActive > 1",
			columns: map[string]*view.Column{
				"IsActive": {Name: "is_active", DataType: "bool", Filterable: true},
			},
			expectErr: true,
		},
		{
			description: "boolean criteria | invalid comparison token",
			input:       "IsActive > true",
			columns: map[string]*view.Column{
				"IsActive": {Name: "is_active", DataType: "bool", Filterable: true},
			},
			expectErr: true,
		},
		{
			description: "boolean criteria | invalid comparison value",
			input:       "IsActive = 1",
			columns: map[string]*view.Column{
				"IsActive": {Name: "is_active", DataType: "bool", Filterable: true},
			},
			expectErr: true,
		},

		{
			description: "int criteria | equal, positive int",
			input:       "Counter = 125",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "int", Filterable: true},
			},
			sanitizedCriteria: ` counter = ?`,
			placeholders:      []interface{}{125},
		},
		{
			description: "int criteria | equal, negative int",
			input:       "Counter = -125",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "int", Filterable: true},
			},
			sanitizedCriteria: ` counter = ?`,
			placeholders:      []interface{}{-125},
		},
		{
			description: "int criteria | not equal, negative int",
			input:       "Counter != -125",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "int", Filterable: true},
			},
			sanitizedCriteria: ` counter <> ?`,
			placeholders:      []interface{}{-125},
		},
		{
			description: "int criteria | greater, positive int",
			input:       "Counter > 125",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "int", Filterable: true},
			},
			sanitizedCriteria: ` counter > ?`,
			placeholders:      []interface{}{125},
		},
		{
			description: "int criteria | greater or equal, positive int",
			input:       "Counter >= 125",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "int", Filterable: true},
			},
			sanitizedCriteria: ` counter >= ?`,
			placeholders:      []interface{}{125},
		},
		{
			description: "int criteria | lower, positive int",
			input:       "Counter < 125",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "int", Filterable: true},
			},
			sanitizedCriteria: ` counter < ?`,
			placeholders:      []interface{}{125},
		},
		{
			description: "int criteria | lower or equal, positive int",
			input:       "Counter <= 125",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "int", Filterable: true},
			},
			sanitizedCriteria: ` counter <= ?`,
			placeholders:      []interface{}{125},
		},
		{
			description: "int criteria | in",
			input:       "Id in (1,2,3,4,5)",
			columns: map[string]*view.Column{
				"Id": {Name: "id", DataType: "int", Filterable: true},
			},
			sanitizedCriteria: ` id in ( ?,  ?,  ?,  ?,  ?)`,
			placeholders:      []interface{}{1, 2, 3, 4, 5},
		},

		{
			description: "float criteria | equal, positive float",
			input:       "Counter = 125.4243",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "float", Filterable: true},
			},
			sanitizedCriteria: ` counter = ?`,
			placeholders:      []interface{}{125.4243},
		},
		{
			description: "float criteria | equal, negative float",
			input:       "Counter = -125.4243",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "float", Filterable: true},
			},
			sanitizedCriteria: ` counter = ?`,
			placeholders:      []interface{}{-125.4243},
		},
		{
			description: "float criteria | greater equal, negative float",
			input:       "Counter >= -125.4243",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "float", Filterable: true},
			},
			sanitizedCriteria: ` counter >= ?`,
			placeholders:      []interface{}{-125.4243},
		},
		{
			description: "float criteria | lower equal, negative float",
			input:       "Counter <= -125.4243",
			columns: map[string]*view.Column{
				"Counter": {Name: "counter", DataType: "float", Filterable: true},
			},
			sanitizedCriteria: ` counter <= ?`,
			placeholders:      []interface{}{-125.4243},
		},

		{
			description: "string criteria | equal",
			input:       "FooName = 'foo'",
			columns: map[string]*view.Column{
				"FooName": {Name: "foo_name", DataType: "string", Filterable: true},
			},
			sanitizedCriteria: ` foo_name = ?`,
			placeholders:      []interface{}{"foo"},
		},
		{
			description: "string criteria | not equal",
			input:       "FooName != 'foo'",
			columns: map[string]*view.Column{
				"FooName": {Name: "foo_name", DataType: "string", Filterable: true},
			},
			sanitizedCriteria: ` foo_name <> ?`,
			placeholders:      []interface{}{"foo"},
		},
		{
			description: "string criteria | like",
			input:       "FooName like '%foo%'",
			columns: map[string]*view.Column{
				"FooName": {Name: "foo_name", DataType: "string", Filterable: true},
			},
			sanitizedCriteria: ` foo_name like ?`,
			placeholders:      []interface{}{"%foo%"},
		},

		{
			description: "field criteria | same type",
			input:       "IsActive != IsNotActive",
			columns: map[string]*view.Column{
				"IsActive":    {Name: "is_active", DataType: "bool", Filterable: true},
				"IsNotActive": {Name: "is_not_active", DataType: "bool", Filterable: true},
			},
			sanitizedCriteria: ` is_active <> is_not_active`,
		},

		{
			description: "time criteria | equal, default format",
			input:       "CreatedTime = '2006-01-02T15:04:05Z'",
			columns: map[string]*view.Column{
				"CreatedTime": {Name: "created_time", DataType: "time", Filterable: true},
			},
			sanitizedCriteria: ` created_time = ?`,
			placeholders:      []interface{}{newTime("2006-01-02T15:04:05Z", time.RFC3339)},
		},
		{
			description: "time criteria | equal, custom format",
			input:       "CreatedTime = '2006-01-02'",
			columns: map[string]*view.Column{
				"CreatedTime": {Name: "created_time", DataType: "time", Format: "2006-01-02", Filterable: true},
			},
			sanitizedCriteria: ` created_time = ?`,
			placeholders:      []interface{}{newTime("2006-01-02", "2006-01-02")},
		},
		{
			description: "time criteria | greater or equal, custom format",
			input:       "CreatedTime >= '2006-01-02'",
			columns: map[string]*view.Column{
				"CreatedTime": {Name: "created_time", DataType: "time", Format: "2006-01-02", Filterable: true},
			},
			sanitizedCriteria: ` created_time >= ?`,
			placeholders:      []interface{}{newTime("2006-01-02", "2006-01-02")},
		},

		{
			description: "bool and float",
			input:       "IsActive = true AND Price > 32.5",
			columns: map[string]*view.Column{
				"IsActive": {Name: "is_active", DataType: "bool", Filterable: true},
				"Price":    {Name: "price", DataType: "float", Filterable: true},
			},
			sanitizedCriteria: ` is_active = ? AND price > ?`,
			placeholders:      []interface{}{true, 32.5},
		},
		{
			description:       "bool and float with parentheses",
			input:             "Price = round(Price, 2)",
			sanitizedCriteria: ` price = round( price,  ?)`,
			columns: map[string]*view.Column{
				"Price": {Name: "price", DataType: "float", Filterable: true},
			},
			methods: map[string]*view.Method{
				"round": {
					Name: "round",
					Args: []*state.Schema{state.NewSchema(reflect.TypeOf(0.0)), state.NewSchema(reflect.TypeOf(0))},
				},
			},
			placeholders: []interface{}{2},
		},
	}

	//for i, testCase := range testCases[len(testCases)-1:] {
	for i, testCase := range testCases {
		tests.LogHeader(fmt.Sprintf("Running testcase %v\n", i))

		for _, column := range testCase.columns {
			if !assert.Nil(t, column.Init(view.EmptyResource(), format.CaseLowerUnderscore, true, nil), testCase.input) {
				continue
			}
		}

		parse, err := criteria.Parse(testCase.input, testCase.columns, testCase.methods)
		if testCase.expectErr {
			assert.NotNil(t, err, testCase.input)
			continue
		} else if !assert.Nil(t, err, testCase.input) {
			continue
		}

		assert.Equal(t, testCase.sanitizedCriteria, parse.Expression, testCase.input)
		for placeholderIndex, placeholder := range parse.Placeholders {
			assert.Equal(t, testCase.placeholders[placeholderIndex], placeholder, testCase.input)
		}
	}
}

func newTime(rawTime, layout string) time.Time {
	if layout == "" {
		layout = time.RFC3339
	}
	aTime, _ := time.Parse(layout, rawTime)
	return aTime
}
