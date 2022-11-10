package types

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"path"
	"testing"
)

func TestParse(t *testing.T) {
	testcases := []struct {
		description  string
		URL          string
		typeName     string
		expectError  bool
		expectedType string
	}{
		{
			URL:          "case001",
			typeName:     "Foo",
			expectedType: `struct { Name string; ID int; Price float64 }`,
		},
		{
			URL:          "case002",
			typeName:     "Foo",
			expectedType: `struct { Name string; ID int; Price float64; Boo struct { BooID int; BooName string; CreatedAt time.Time }; BooPtr *struct { BooID int; BooName string; CreatedAt time.Time }; BooSlice []struct { BooID int; BooName string; CreatedAt time.Time }; BooMap map[string]struct { BooID int; BooName string; CreatedAt time.Time } }`,
		},
		{
			URL:      "case003",
			typeName: "Foo",
		},
	}

	service := afs.New()
	//for _, testcase := range testcases[len(testcases)-1:] {
	//	for _, testcase := range testcases {
	for _, testcase := range testcases[:len(testcases)-1] {
		structFile := path.Join(".", "testdata", testcase.URL, "struct.go")
		rType, err := Parse(context.TODO(), service, structFile, testcase.typeName)

		if testcase.expectError && assert.Nil(t, err, testcase.description) {
			continue
		} else if !assert.Nil(t, err, testcase.description) {
			continue
		}

		actualType := rType.String()
		assert.Equal(t, testcase.expectedType, actualType, testcase.description)
	}
}
