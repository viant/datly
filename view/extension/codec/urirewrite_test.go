package codec

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"reflect"
	"testing"
)

func TestUrlRewriter_Value(t *testing.T) {

	var testCases = []struct {
		description  string
		config       *codec.Config
		sourceURL    string
		destURL      string
		expect       string
		destTypeName string
	}{
		{
			description: "url rewriter basic checks",
			config: &codec.Config{
				Body:       "",
				InputType:  nil,
				Args:       []string{"view,format"},
				OutputType: "",
			},
			sourceURL:    "view=total&from=2023-10-02&to=2023-10-02&format=xls ",
			destURL:      "from=2023-10-02&to=2023-10-02",
			destTypeName: "URIRewriter",
		},
	}

	factory := &URIRewriterFactory{}
	for _, testCase := range testCases {
		types := xreflect.NewTypes()
		_ = types.Register(testCase.destTypeName)
		aCodec, err := factory.New(testCase.config, codec.WithTypeLookup(func(name string) (reflect.Type, error) {
			return types.Lookup(name)
		}))
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		dest, err := aCodec.Value(context.Background(), testCase.sourceURL)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.destURL, dest, testCase.description)
	}

}
