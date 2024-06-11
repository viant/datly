package codec

import (
	"context"
	"fmt"
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
			expect: 3,
		},
		{
			destTypeName: "TimeDiff",
			description:  "basic transfer",
			getValue: func() interface{} {
				type Range struct {
					FromX time.Time
					ToY   time.Time
				}
				from := time.Date(2024, 04, 23, 0, 0, 0, 0, time.UTC)
				to := time.Date(2024, 04, 29, 23, 59, 59, 59, time.UTC)
				return &Range{FromX: from, ToY: to}

			},
			config: &codec.Config{
				Body:       "",
				InputType:  nil,
				Args:       []string{"FromX", "ToY", "day"},
				OutputType: "",
			},
			dest:   "",
			expect: 7,
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
		fmt.Printf("%s\n", value)
		actual, err := aCodec.Value(context.Background(), value)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}
