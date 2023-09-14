package xmlfilter

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
			description: "basic filter",
			getData: func() interface{} {
				type IntFilter struct {
					include []int
					exclude []int
				}

				type Filter struct {
					IntFilter IntFilter
					Id        int
				}

				return Filter{
					IntFilter: IntFilter{
						include: []int{1, 2},
						exclude: []int{3, 4},
					},
					Id: 7,
				}
			},
		},
	}

	srv := New()
	for _, testCase := range testCases {
		fmt.Printf("%s --- \n", testCase.description)

		transfer, err := srv.Transfer(testCase.getData())
		assert.Nil(t, err, testCase.description)
		data, _ := json.Marshal(transfer)
		fmt.Printf("%s\n", data)
	}

}
