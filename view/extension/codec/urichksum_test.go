package codec

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"reflect"
	"testing"
)

func TestAsyncKeyCustomizer_Value(t *testing.T) {

	var testCases = []struct {
		description  string
		config       *codec.Config
		sourceURL    string
		destURL      string
		expect       string
		destTypeName string
	}{
		{
			description: "url customizer basic checks",
			config: &codec.Config{
				Body:       "",
				InputType:  nil,
				Args:       []string{"view,format"},
				OutputType: "",
			},
			sourceURL:    "view=total&from=2023-10-02&to=2023-10-02&format=xls ",
			destURL:      "from=2023-10-02&to=2023-10-02",
			destTypeName: "URICustomizer",
		},
		{
			description: "url customizer sorting, exclusion, ignoring empty checks",
			config: &codec.Config{
				Body:       "",
				InputType:  nil,
				Args:       []string{"views,_format"},
				OutputType: "",
			},
			sourceURL:    "views=TOTAL&from=2023-10-25&to=2023-10-25&EMPTY1=&EMPTY2=&ZZ=&AA=&bb=2&aa=1&cc=4&dd=5&&cc=3&&cc=&_format=json",
			destURL:      "aa=1&bb=2&cc=3&cc=4&dd=5&from=2023-10-25&to=2023-10-25",
			destTypeName: "URICustomizer",
		},
		{
			description: "url customizer sorting, exclusion, ignoring empty, checksum checks",
			config: &codec.Config{
				Body:       "",
				InputType:  nil,
				Args:       []string{"views,_format", "sha1"},
				OutputType: "",
			},
			sourceURL:    "views=TOTAL&from=2023-10-25&to=2023-10-25&EMPTY1=&EMPTY2=&ZZ=&AA=&bb=2&aa=1&cc=4&dd=5&&cc=3&&cc=&_format=json",
			destURL:      "08a92abdc8242d9f087cf97e108f8e5511644160",
			destTypeName: "URICustomizer",
		},
	}

	factory := &UriChecksumFactory{}
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
