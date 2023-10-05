package signature

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"path"
	"testing"
)

func TestHeader_Signature(t *testing.T) {

	baseDir := toolbox.CallerDirectory(3)
	testData := path.Join(baseDir, "testdata")

	var testCases = []struct {
		description string
		ruleURL     string
		prefix      string
		uri         string
		method      string
		hasMatch    bool
	}{
		{
			description: "signature match",
			ruleURL:     testData,
			uri:         "/v1/api/dev/vendors",
			method:      "GET",
			hasMatch:    true,
		},
	}

	for _, testCase := range testCases {
		srv, err := New(context.Background(), testCase.prefix, testCase.ruleURL)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		siganture, err := srv.Signature(testCase.method, testCase.uri)
		if testCase.hasMatch {
			if !assert.Nil(t, err, testCase.description) {
				continue
			}
		}
		assert.NotNilf(t, siganture, testCase.description)
	}

}
