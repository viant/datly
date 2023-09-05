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

		{
			description: "custom ptr types",
			getData: func() interface{} {

				type Data []*struct {
					Avails         *int     "sqlx:\"name=avails\" velty:\"names=avails|Avails\""
					Uniqs          *int     "sqlx:\"name=uniqs\" velty:\"names=uniqs|Uniqs\""
					FinalHhUniqsV1 *int     "sqlx:\"name=final_hh_uniqs_v1\" velty:\"names=final_hh_uniqs_v1|FinalHhUniqsV1\""
					ClearingPrice  *float64 "sqlx:\"name=clearing_price\" velty:\"names=clearing_price|ClearingPrice\""
				}
				data := Data{}
				payload := `[{"Avails":70577901,"Uniqs":30706931,"FinalHhUniqsV1":7282500,"ClearingPrice":4.45}]`
				err := json.Unmarshal([]byte(payload), &data)
				if err != nil {
					panic(err)
				}
				return data
			},
		},
	}

	srv := New()
	for _, testCase := range testCases[len(testCases)-1:] {
		fmt.Printf("%s --- \n", testCase.description)

		transfer, err := srv.Transfer(testCase.getData())
		assert.Nil(t, err, testCase.description)
		data, _ := json.Marshal(transfer)
		fmt.Printf("%s\n", data)
	}

}
