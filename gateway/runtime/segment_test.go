package runtime

import (
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

func TestRemoveFirstSegment(t *testing.T) {
	testcases := []struct {
		input  string
		output string
	}{
		{
			input:  "/first/second",
			output: "/second",
		},
		{
			input:  "////first/second",
			output: "/second",
		},
		{
			input:  "/",
			output: "/",
		},
		{
			input:  "",
			output: "",
		},
		{
			input:  "////",
			output: "////",
		},
	}

	for _, testcase := range testcases {
		assert.Equal(t, RemoveFirstSegment(testcase.input), testcase.output)
	}
}

func TestParseWithoutFirstSegment(t *testing.T) {
	testcases := []struct {
		input     string
		output    string
		expectErr bool
	}{
		{
			input:  "https://some-url.com/first/second",
			output: "https://some-url.com/second",
		},
		{
			input:  "https://some-url.com/////first/second",
			output: "https://some-url.com/second",
		},
		{
			input:  "https://some-url.com/",
			output: "https://some-url.com/",
		},
		{
			input:  "abcdef.com/first/second",
			output: "/first/second",
		},
	}

	//for _, testcase := range testcases[len(testcases)-1:] {
	for _, testcase := range testcases {
		URL, err := url.Parse(testcase.input)
		if !assert.Nil(t, err, testcase.input) {
			continue
		}

		parsedURL, err := RemoveFirstURLSegment(URL)

		if testcase.expectErr && !assert.Nil(t, err, testcase.input) {
			continue
		}

		assert.Equal(t, testcase.output, parsedURL.String())
	}
}
