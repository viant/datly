package gateway

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCommonURL(t *testing.T) {
	testCases := []struct {
		urls        []string
		expected    string
		description string
	}{
		{
			urls:     []string{"s3://abcdef/Datly/routes/", "s3://abcdef/Datly/dependencies/", "s3://abcdef/Datly/plugins/"},
			expected: "s3://abcdef/Datly",
		},
	}

	for _, testCase := range testCases {
		URL, err := CommonURL(testCase.urls...)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		assert.Equal(t, testCase.expected, URL, testCase.description)
	}
}
