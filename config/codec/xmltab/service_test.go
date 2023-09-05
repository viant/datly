package xmltab

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestService_Transfer(t *testing.T) {

	var testCases = []struct {
		description string
		getData     func() interface{}
	}{
		{
			description: "basic slice",
			getData: func() interface{} {
				type Foo struct {
					Id   int
					Name string
					F    float64
					II   int
				}

				return []Foo{
					{
						Id:   1,
						II:   12,
						F:    12312.3,
						Name: "abc",
					},
					{
						Id:   2,
						II:   4444,
						F:    2.3,
						Name: "xyz",
					},
				}

			},
		},
		{
			description: "slice with ptr struct",
			getData: func() interface{} {
				type Foo struct {
					Id   int
					Name string
					F    float64
					II   int
				}

				return []*Foo{
					{
						Id:   1,
						II:   12,
						F:    12312.3,
						Name: "abc",
					},
					{
						Id:   2,
						II:   4444,
						F:    2.3,
						Name: "xyz",
					},
				}

			},
		},

		{
			description: "basic slice",
			getData: func() interface{} {
				type Foo struct {
					Id   int
					Name string
					F    float64
					II   int
				}

				return []Foo{}
			},
		},
	}

	srv := New()
	for _, testCase := range testCases {
		transfer, err := srv.Transfer(testCase.getData())
		assert.Nil(t, err, testCase.description)
		data, _ := json.Marshal(transfer)
		fmt.Printf("%s --- \n", testCase.description)
		fmt.Printf("%s\n", data)
	}

}
