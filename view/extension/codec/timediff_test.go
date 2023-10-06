package codec

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"testing"
	"time"
)

func TestTimeDiff_Value(t *testing.T) {

	var testCases = []struct {
		description  string
		config       *codec.Config
		getValue     func() interface{}
		dest         string
		destTypeName string
		expect       int
	}{
		{
			destTypeName: "TimeDiff",
			description:  "basic transfer",
			getValue: func() interface{} {
				type Range struct {
					FromX time.Time
					ToY   *time.Time
				}
				t := time.Now()

				return &Range{FromX: t.Add(-2 * time.Hour * 24), ToY: &t}
			},
			config: &codec.Config{
				Body:       "",
				InputType:  nil,
				Args:       []string{"FromX", "ToY", "day"},
				OutputType: "",
			},
			dest:   "",
			expect: 2,
		},
	}

	factory := &TimeDiffFactory{}
	for _, testCase := range testCases {
		types := xreflect.NewTypes()
		_ = types.Register(testCase.destTypeName)
		aCodec, err := factory.New(testCase.config)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		//		src, err := factory.New(testCase.config)
		assert.Nil(t, err, testCase.destTypeName)
		value := testCase.getValue()
		actual, err := aCodec.Value(context.Background(), value)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, actual, testCase.expect, testCase.description)
	}

}
