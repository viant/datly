package sanitize

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"testing"
)

func TestNewParser(t *testing.T) {

	useCases := []struct {
		description string
		criteria    string
		expect      string
		hasError    bool
	}{
		{
			description: "criteria - single literal",
			criteria:    " true ",
			expect:      `{"Value":"true"}`,
		},

		{
			description: "criteria - binary literal",
			criteria:    " true = true",
			expect:      `{"X":{"Value":"true"},"Operator":"=","Y":{"Value":"true"}}`,
		},

		{
			description: "criteria - binary literal - brackets",
			criteria:    "(true = true)",
			expect:      `{"P":{"X":{"Value":"true", "Kind": 3},"Operator":"=","Y":{"Value":"true", "Kind": 3}}}`,
		},
		{
			description: "criteria selector literal",
			criteria:    "column_name = '123'",
			expect:      `{"X":{"Name":"column_name"},"Operator":"=","Y":{"Value":"'123'","Kind":2}}`,
		},

		{
			description: "logical operator",
			criteria:    "column_name = '123' OR column_z = 5",
			expect:      `{"X":{"Name":"column_name"},"Operator":"=","Y":{"X":{"Value":"'123'","Kind":2},"Operator":"OR","Y":{"X":{"Name":"column_z"},"Operator":"=","Y":{"Value":"5","Kind":1}}}}`,
		},
		{
			description: "criteria selector literal",
			criteria:    "column_name = ''; Drop table  ;--''",
			hasError:    true,
		},
		{
			description: "criteria selector literal",
			criteria:    "SELECT Name from Foos",
			hasError:    true,
		},
		{
			description: "null check",
			criteria:    "column_name is null",
			expect:      `{"X":{"Name":"column_name"},"Operator":"is null"}`,
		},
		{
			description: "not proper null check",
			criteria:    "column_name is a null",
			hasError:    true,
		},
		{
			description: "not null check",
			criteria:    "column_name is not null",
			expect:      `{"X":{"Name":"column_name"},"Operator":"is not null"}`,
		},
		{
			description: "in - inconsistent values types",
			criteria:    "column_name in ('abc', true, false, 123, 0.234)",
			hasError:    true,
		},
		{
			description: "in",
			criteria:    "column_name in (null,'abc',null,'cdef')",
			expect:      `{"X":{"Name":"column_name"},"Operator":"in","Y":{"Value":"null,'abc',null,'cdef'","Kind":2}}`,
		},
		{
			description: "not in",
			criteria:    "column_name not in (null,'abc',null,'cdef')",
			expect:      `{"X":{"Name":"column_name"},"Operator":"not in","Y":{"Value":"null,'abc',null,'cdef'","Kind":2}}`,
		},
		{
			description: "not is",
			criteria:    "column_name not is (null,'abc',null,'cdef')",
			hasError:    true,
		},
	}

	for _, useCase := range useCases {
		actual, err := Parse([]byte(useCase.criteria))
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		if !assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			data, _ := json.Marshal(actual)
			fmt.Printf("%s\n", data)
		}
	}
}
