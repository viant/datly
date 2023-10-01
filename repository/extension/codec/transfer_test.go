package codec

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"reflect"
	"testing"
	"time"
)

func TestTransfer_Value(t *testing.T) {

	var testCases = []struct {
		description  string
		config       *codec.Config
		source       func() interface{}
		dest         func() interface{}
		destTypeName string
		expect       func() interface{}
	}{
		{
			destTypeName: "test1",
			description:  "basic transfer",
			config: &codec.Config{
				Body:       "",
				InputType:  nil,
				Args:       []string{"test1"},
				OutputType: "",
			},
			dest: func() interface{} {
				type Foo struct {
					Elapsed     time.Duration `transfer:"from=TimeTaken"`
					Name        string        `transfer:"from=BarName"`
					Id          int           `transfer:"from=BarID"`
					Description string
				}
				return &Foo{}
			},
			source: func() interface{} {
				type Bar struct {
					TimeTaken   int
					BarName     string
					BarID       int
					Description string
				}
				return &Bar{
					TimeTaken:   int(time.Second),
					BarID:       123,
					BarName:     "test X",
					Description: "descr...",
				}
			},
			expect: func() interface{} {
				type Foo struct {
					Elapsed     time.Duration `transfer:"from=TimeTaken"`
					Name        string        `transfer:"from=BarName"`
					Id          int           `transfer:"from=BarID"`
					Description string
				}
				return Foo{Elapsed: time.Second, Name: "test X", Id: 123}
			},
		},
	}

	factory := &TransferFactory{}
	for _, testCase := range testCases {
		types := xreflect.NewTypes()
		_ = types.Register(testCase.destTypeName, xreflect.WithReflectType(reflect.TypeOf(testCase.dest())))
		aCodec, err := factory.New(testCase.config, codec.WithTypeLookup(func(name string) (reflect.Type, error) {
			return types.Lookup(name)
		}))
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		src := testCase.source()
		dest, err := aCodec.Value(context.Background(), src)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expect(), dest, testCase.description)
	}

}
