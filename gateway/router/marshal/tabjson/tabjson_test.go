package tabjson

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/sqlx/io"
	"reflect"
	"testing"
	"time"
)

func Test_TabularJSON_Marshal(t *testing.T) {
	type Egg struct {
		ID     int
		Price  float32
		Weight float64
	}

	type Foo struct {
		ID    int
		Name  string
		Price string
	}

	type Boo struct {
		ID   int
		Name string
		Foo  *Foo
	}

	type Bar struct {
		ID   int
		Name string
		Bar  *Bar
	}

	type BooSlice struct {
		ID   int
		Name string
		Foos []*Foo
	}

	type multiSlices struct {
		ID        int
		Foos      []*Foo
		Name      string
		BooSlices []*BooSlice
		Boo       *Boo
	}

	type FooWithTime struct {
		ID      int
		Time    time.Time
		TimePtr *time.Time
	}

	testCases := []struct {
		description   string
		rType         reflect.Type
		input         interface{}
		expected      string
		config        *Config
		depthsConfigs []*Config
		useAssertPkg  bool
	}{
		{
			description: "basic - 2 rows",
			input: []Foo{
				{
					ID:    1,
					Name:  "Foo - 1",
					Price: "125.5",
				},
				{
					ID:    2,
					Name:  "Foo - 2",
					Price: "125.5",
				},
			},
			rType: reflect.TypeOf(Foo{}),
			expected: `[["ID","Name","Price"],
[1,"Foo - 1","125.5"],
[2,"Foo - 2","125.5"]]`,
		},
		{
			description: "one to one - 3 rows",
			input: []*Boo{
				{
					ID:   1,
					Name: "Boo",
					Foo: &Foo{
						ID:    10,
						Name:  "Foo - 10",
						Price: "125.5",
					},
				},
				{
					ID:   2,
					Name: "Boo",
					Foo: &Foo{
						ID:    20,
						Name:  "Foo - 20",
						Price: "125.5",
					},
				},
				{
					ID:   3,
					Name: "Boo",
					Foo: &Foo{
						ID:    30,
						Name:  "Foo - 30",
						Price: "125.5",
					},
				},
			},
			rType: reflect.TypeOf(&Boo{}),
			expected: `[["ID","Name", "Foo"],
[1,"Boo",[["ID","Name","Price"],
[10,"Foo - 10","125.5"]]
],
[2,"Boo",[["ID","Name","Price"],
[20,"Foo - 20","125.5"]]
],
[3,"Boo",[["ID","Name","Price"],
[30,"Foo - 30","125.5"]]
]
]`,
		},
		{
			description: "one to many - 3 rows",
			input: []*BooSlice{
				{
					ID:   1,
					Name: "Boo",
					Foos: []*Foo{
						{
							ID:    10,
							Name:  "Foo - 10",
							Price: "125.5",
						},
						{
							ID:    20,
							Name:  "Foo - 20",
							Price: "250.5",
						},
						{
							ID:    30,
							Name:  "Foo - 30",
							Price: "350.5",
						},
					},
				},
				{
					ID:   2,
					Name: "Boo",
					Foos: []*Foo{
						{
							ID:    40,
							Name:  "Foo - 10",
							Price: "125.5",
						},
						{
							ID:    50,
							Name:  "Foo - 20",
							Price: "250.5",
						},
						{
							ID:    60,
							Name:  "Foo - 30",
							Price: "350.5",
						},
					},
				},
				{
					ID:   3,
					Name: "Boo",
					Foos: []*Foo{
						{
							ID:    70,
							Name:  "Foo - 10",
							Price: "125.5",
						},
						{
							ID:    80,
							Name:  "Foo - 20",
							Price: "250.5",
						},
						{
							ID:    90,
							Name:  "Foo - 30",
							Price: "350.5",
						},
					},
				},
			},
			rType: reflect.TypeOf(&BooSlice{}),
			expected: `[["ID","Name", "Foos"],
[1,"Boo", [["ID","Name","Price"],
[10,"Foo - 10","125.5"],
[20,"Foo - 20","250.5"],
[30,"Foo - 30","350.5"]]
],
[2,"Boo", [["ID","Name","Price"],
[40,"Foo - 10","125.5"],
[50,"Foo - 20","250.5"],
[60,"Foo - 30","350.5"]]
],
[3,"Boo", [["ID","Name","Price"],
[70,"Foo - 10","125.5"],
[80,"Foo - 20","250.5"],
[90,"Foo - 30","350.5"]]
]
]`,
		},
		{
			description: "nulls",
			input: []*Boo{
				{
					ID:   1,
					Name: "Boo",
				},
			},
			rType: reflect.TypeOf(&Boo{}),
			expected: `[["ID","Name","Foo"],
[1,"Boo",null]]`,
		},
		{
			description: "floats - with custom precision",
			input: []*Egg{
				{
					ID:     1,
					Price:  1.25,
					Weight: 125.5,
				},
			},
			rType:        reflect.TypeOf(&Egg{}),
			useAssertPkg: true,
			expected:     `[["ID","Price","Weight"],[1,1.250,125.50000]]`,
			config: &Config{
				StringifierConfig: io.StringifierConfig{
					StringifierFloat32Config: io.StringifierFloat32Config{
						Precision: "3",
					},
					StringifierFloat64Config: io.StringifierFloat64Config{
						Precision: "5",
					},
				},
			},
		},
		{
			description: "floats - with default -1 precision",
			input: []*Egg{
				{
					ID:     1,
					Price:  1.25,
					Weight: 125.5,
				},
				{
					ID:     2,
					Price:  33.2343237400054931640625,
					Weight: 21.957962334156036376953125,
				},
			},
			rType:        reflect.TypeOf(&Egg{}),
			useAssertPkg: true,
			expected:     `[["ID","Price","Weight"],[1,1.25,125.5],[2,33.23432540893555,21.957962334156036]]`,
		},
		{
			description: "times",
			input: []*FooWithTime{
				{
					ID:      1,
					Time:    newTime("2019-01-02"),
					TimePtr: newTimePtr("2020-01-02"),
				},
				{
					ID:   2,
					Time: newTime("2020-04-04"),
				},
				{
					ID:      3,
					Time:    time.Date(2021, 8, 15, 14, 30, 45, 0, getLocation(time.LoadLocation("America/New_York"))),
					TimePtr: timePtr(time.Date(2021, 8, 15, 14, 30, 45, 0, getLocation(time.LoadLocation("Asia/Tokyo")))),
				},
			},
			rType: reflect.TypeOf(&FooWithTime{}),
			expected: `[["ID","Time","TimePtr"],
[1,"2019-01-02T00:00:00Z","2020-01-02T00:00:00Z"],
[2,"2020-04-04T00:00:00Z",null],
[3,"2021-08-15T14:30:45-04:00","2021-08-15T14:30:45+09:00"]]`,
		},
		{
			description: "multi slices",
			input: []*multiSlices{
				{
					ID:   1,
					Name: "multiSlice with foos",
					Foos: []*Foo{
						{
							ID:    2,
							Name:  "Foo - 1",
							Price: "125",
						},
						{
							ID:    3,
							Name:  "Foo - 2",
							Price: "250",
						},
						{
							ID:    567,
							Name:  "Foo - 567",
							Price: "12345",
						},
						{
							ID:   987,
							Name: "Foo - 987",
						},
					},
					BooSlices: []*BooSlice{
						{
							ID:   123,
							Name: "boo - 123",
							Foos: []*Foo{
								{
									ID:   234,
									Name: "foo - 234",
								},
								{
									ID:   345,
									Name: "foo - 345",
								},
							},
						},
						{
							ID:   2345,
							Name: "boo - 2345",
							Foos: []*Foo{
								{
									ID:   2346,
									Name: "foo - 2346",
								},
								{
									ID:   2347,
									Name: "foo - 2347",
								},
							},
						},
					},
					Boo: nil,
				},
				{
					ID:   2,
					Name: "multiSlice without foos",
					BooSlices: []*BooSlice{
						{
							ID:   5,
							Name: "Boo slice - name",
							Foos: []*Foo{
								{
									ID:    6,
									Name:  "Foo under Boo slice - 1",
									Price: "567",
								},
								{
									ID:    7,
									Name:  "Foo under Boo slice - 2",
									Price: "567",
								},
							},
						},
					},
					Foos: nil,
					Boo: &Boo{
						ID:   4,
						Name: "Boo - name",
						Foo:  nil,
					},
				},
			},
			rType: reflect.TypeOf(&multiSlices{}),
			expected: `[
   ["ID", "Name", "Foos", "BooSlices", "Boo"],
   [1, "multiSlice with foos", [
                              ["ID","Name","Price"],
                              [2,"Foo - 1","125"],
                              [3,"Foo - 2","250"],
                              [567,"Foo - 567","12345"],
                              [987,"Foo - 987",""]
                              ],      [
                                          ["ID","Name","Foos"],
                                          [123,"boo - 123",[
                                                                  ["ID","Name","Price"],
                                                                  [234,"foo - 234",""],
                                                                  [345,"foo - 345",""]
                                                           ]
                                          ],
                                          [2345,"boo - 2345",[
                                                                  ["ID","Name","Price"],
                                                                  [2346,"foo - 2346",""],
                                                                  [2347,"foo - 2347",""]
                                                             ]
                                          ]
                                    ], null
   
   ],
   [2, "multiSlice without foos", null, [
                                           ["ID","Name","Foos"],
                                           [5, "Boo slice - name", [
                                                                       ["ID","Name","Price"],
                                                                       [6,"Foo under Boo slice - 1","567"],
                                                                       [7,"Foo under Boo slice - 2","567"]
                                                                   ]
                                           ]
                                       ],  [
                                               ["ID", "Name", "Foo"],
                                               [4, "Boo - name", null]
                                           ]

   ]
]`,
		},
		{
			description: "one to many - Foos exclusion",
			input: []*BooSlice{
				{
					ID:   1,
					Name: "Boo",
					Foos: []*Foo{
						{
							ID:    10,
							Name:  "Foo - 10",
							Price: "125.5",
						},
						{
							ID:    20,
							Name:  "Foo - 20",
							Price: "250.5",
						},
					},
				},
			},
			rType: reflect.TypeOf(&BooSlice{}),
			expected: `[["ID","Name"],
[1,"Boo"]]`,
			config: &Config{
				ExcludedPaths: []string{"Foos"},
			},
		},
		{
			description: "one to many - 3 rows - double exclusion",
			input: []*BooSlice{
				{
					ID:   1,
					Name: "Boo",
					Foos: []*Foo{
						{
							ID:    10,
							Name:  "Foo - 10",
							Price: "125.5",
						},
						{
							ID:    20,
							Name:  "Foo - 20",
							Price: "250.5",
						},
						{
							ID:    30,
							Name:  "Foo - 30",
							Price: "350.5",
						},
					},
				},
				{
					ID:   2,
					Name: "Boo",
					Foos: []*Foo{
						{
							ID:    40,
							Name:  "Foo - 10",
							Price: "125.5",
						},
						{
							ID:    50,
							Name:  "Foo - 20",
							Price: "250.5",
						},
						{
							ID:    60,
							Name:  "Foo - 30",
							Price: "350.5",
						},
					},
				},
				{
					ID:   3,
					Name: "Boo",
					Foos: []*Foo{
						{
							ID:    70,
							Name:  "Foo - 10",
							Price: "125.5",
						},
						{
							ID:    80,
							Name:  "Foo - 20",
							Price: "250.5",
						},
						{
							ID:    90,
							Name:  "Foo - 30",
							Price: "350.5",
						},
					},
				},
			},
			rType: reflect.TypeOf(&BooSlice{}),
			expected: `[["ID","Foos"],
[1, [["ID","Name"],
[10,"Foo - 10"],
[20,"Foo - 20"],
[30,"Foo - 30"]]
],
[2, [["ID","Name"],
[40,"Foo - 10"],
[50,"Foo - 20"],
[60,"Foo - 30"]]
],
[3, [["ID","Name"],
[70,"Foo - 10"],
[80,"Foo - 20"],
[90,"Foo - 30"]]
]
]`,
			config: &Config{
				ExcludedPaths: []string{"Name", "Foos.Price"},
			},
		},
		{
			description: "multi slices with exclusions",
			input: []*multiSlices{
				{
					ID:   1,
					Name: "multiSlice with foos",
					Foos: []*Foo{
						{
							ID:    2,
							Name:  "Foo - 1",
							Price: "125",
						},
						{
							ID:    3,
							Name:  "Foo - 2",
							Price: "250",
						},
						{
							ID:    567,
							Name:  "Foo - 567",
							Price: "12345",
						},
						{
							ID:   987,
							Name: "Foo - 987",
						},
					},
					BooSlices: []*BooSlice{
						{
							ID:   123,
							Name: "Boo slice - 123",
							Foos: []*Foo{
								{
									ID:   234,
									Name: "foo - 234",
								},
								{
									ID:   345,
									Name: "foo - 345",
								},
							},
						},
						{
							ID:   2345,
							Name: "Boo slice - 2345",
							Foos: []*Foo{
								{
									ID:   2346,
									Name: "foo - 2346",
								},
								{
									ID:   2347,
									Name: "foo - 2347",
								},
							},
						},
					},
					Boo: nil,
				},
				{
					ID:   2,
					Name: "multiSlice without foos",
					BooSlices: []*BooSlice{
						{
							ID:   5,
							Name: "Boo slice - 5",
							Foos: []*Foo{
								{
									ID:    6,
									Name:  "Foo under Boo slice - 1",
									Price: "567",
								},
								{
									ID:    7,
									Name:  "Foo under Boo slice - 2",
									Price: "567",
								},
							},
						},
					},
					Foos: nil,
					Boo: &Boo{
						ID:   4,
						Name: "Boo - name",
						Foo:  nil,
					},
				},
			},
			rType: reflect.TypeOf(&multiSlices{}),
			config: &Config{
				ExcludedPaths: []string{"Name", "Foos.Price", "BooSlices.Foos.ID", "Boo.Foo"},
			},
			expected: `[
   ["ID", "Foos", "BooSlices"],
   [1, [
		  ["ID","Name"],
		  [2,"Foo - 1"],
		  [3,"Foo - 2"],
		  [567,"Foo - 567"],
		  [987,"Foo - 987"]
       ],      
	   		[
				  ["ID","Name","Foos"],
				  [123,"Boo slice - 123",[
										  ["Name","Price"],
										  ["foo - 234",""],
										  ["foo - 345",""]
								   ]
				  ],
				  [2345,"Boo slice - 2345",[
										  ["Name","Price"],
										  ["foo - 2346",""],
										  ["foo - 2347",""]
									 ]
				  ]
			]
   ],
   [2, null, [
				   ["ID","Name", "Foos"],
				   [5, "Boo slice - 5", [
										   ["Name","Price"],
										   ["Foo under Boo slice - 1","567"],
										   ["Foo under Boo slice - 2","567"]
										]
				  ]
			 ]
   ]
]`,
		},
		{
			description:  "primitive",
			input:        event,
			useAssertPkg: true,
			rType:        nil,
			expected:     `[["Int","Int8","Uint8","Int16","Uint16","Int32","Uint32","Int64","Uint64","Byte","String","Float32","Float64","Bool"],[1,2,3,4,5,6,7,8,9,10,"string",5.5,11.5,true]]`,
		},
		{
			description:  "primitive pointers",
			input:        eventPtr,
			useAssertPkg: true,
			rType:        nil,
			expected:     `[["Int","Int8","Uint8","Int16","Uint16","Int32","Uint32","Int64","Uint64","Byte","String","Float32","Float64","Bool"],[1,2,3,4,5,6,7,8,9,10,"string",5.5,11.5,true]]`,
		},
		{
			description:  "nils",
			input:        nilsPtr,
			useAssertPkg: true,
			rType:        nil,
			expected:     `[["Int","Int8","Uint8","Int16","Uint16","Int32","Uint32","Int64","Uint64","Byte","String","Float32","Float64","Bool"],[null,null,null,null,null,null,null,null,null,null,null,null,null,null]]`,
		},
		{
			description:  "slice without relations",
			input:        sliceWithoutRelations,
			useAssertPkg: true,
			expected:     `[["Int","String","Float64"],[10,"str - 1",20.5],[15,"str - 2",40.5],[5,"str - 0",0.5]]`,
		},
		{
			description:  "slice with relations",
			input:        sliceWithRelations,
			useAssertPkg: true,
			rType:        nil,
			expected:     `[["Int","String","Float64","EventType"],[100,"abc",0,[["Id","Type"],[200,"event-type-1"]]]]`,
		},
		{
			description:  "nil slice and *T",
			input:        nilNonPrimitives,
			useAssertPkg: true,
			rType:        nil,
			expected:     `[["Id","Name","EventTypesEmpty","EventTypes","EventType"],[231,"",null,[["Id","Type"],[1,"t - 1"],[null,null],[1,"t - 3"]],null]]`,
		},
	}

	for _, testCase := range testCases {
		//for i, testCase := range testCases {
		//fmt.Println("====", i, " ", testCase.description)

		if testCase.rType == nil {
			fn, ok := (testCase.input).(func() interface{})
			assert.True(t, ok, testCase.description)

			testCase.input = fn()
			testCase.rType = reflect.TypeOf(testCase.input)
			if testCase.rType.Kind() == reflect.Slice {
				testCase.rType = testCase.rType.Elem()
			}
		}

		marshaller, err := NewMarshaller(testCase.rType, testCase.config)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		marshal, err := marshaller.Marshal(testCase.input, testCase.depthsConfigs)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		actual := string(marshal)

		if testCase.useAssertPkg {
			assert.EqualValues(t, testCase.expected, actual)
			continue
		}

		assertly.AssertValues(t, testCase.expected, actual)
	}
}

func newTime(date string) time.Time {
	parse, err := time.Parse("2006-01-02", date)
	if err != nil {
		panic(err)
	}
	return parse
}

func newTimePtr(date string) *time.Time {
	aTime := newTime(date)
	return &aTime
}

func getLocation(location *time.Location, err error) *time.Location {
	return location
}

func nilNonPrimitives() interface{} {
	type eventType struct {
		Id   int
		Type string
	}

	type event struct {
		Id              int
		EventTypesEmpty []*eventType
		EventTypes      []*eventType
		Name            string
		EventType       *eventType
	}

	return []*event{
		{
			Id: 231,
			EventTypes: []*eventType{
				{
					Id:   1,
					Type: "t - 1",
				},
				nil,
				{
					Id:   1,
					Type: "t - 3",
				},
			},
		},
	}
}

func sliceWithRelations() interface{} {
	type eventType struct {
		Id   int
		Type string
	}

	type event struct {
		Int       int
		String    string
		Float64   float64
		EventType eventType
	}

	return event{
		Int:    100,
		String: "abc",
		EventType: eventType{
			Id:   200,
			Type: "event-type-1",
		},
	}
}

func sliceWithoutRelations() interface{} {
	type event struct {
		Int     int
		String  string
		Float64 float64
	}

	return []event{
		{
			Int:     10,
			String:  "str - 1",
			Float64: 20.5,
		},
		{
			Int:     15,
			String:  "str - 2",
			Float64: 40.5,
		},
		{
			Int:     5,
			String:  "str - 0",
			Float64: 0.5,
		},
	}
}

func nilsPtr() interface{} {
	type event struct {
		Int     *int
		Int8    *int8
		Uint8   *uint8
		Int16   *int16
		Uint16  *uint16
		Int32   *int32
		Uint32  *uint32
		Int64   *int64
		Uint64  *uint64
		Byte    *byte
		String  *string
		Float32 *float32
		Float64 *float64
		Bool    *bool
	}
	return &event{}
}

func event() interface{} {
	type event struct {
		Int     int
		Int8    int8
		Uint8   uint8
		Int16   int16
		Uint16  uint16
		Int32   int32
		Uint32  uint32
		Int64   int64
		Uint64  uint64
		Byte    byte
		String  string
		Float32 float32
		Float64 float64
		Bool    bool
	}

	return event{
		Int:     1,
		Int8:    2,
		Uint8:   3,
		Int16:   4,
		Uint16:  5,
		Int32:   6,
		Uint32:  7,
		Int64:   8,
		Uint64:  9,
		Byte:    10,
		String:  "string",
		Float32: 5.5,
		Float64: 11.5,
		Bool:    true,
	}
}

func eventPtr() interface{} {
	type event struct {
		Int     *int
		Int8    *int8
		Uint8   *uint8
		Int16   *int16
		Uint16  *uint16
		Int32   *int32
		Uint32  *uint32
		Int64   *int64
		Uint64  *uint64
		Byte    *byte
		String  *string
		Float32 *float32
		Float64 *float64
		Bool    *bool
	}

	intV := 1
	int8V := int8(2)
	uint8V := uint8(3)
	int16V := int16(4)
	uint16V := uint16(5)
	int32V := int32(6)
	uint32V := uint32(7)
	int64V := int64(8)
	uint64V := uint64(9)
	byteV := byte(10)
	stringV := "string"
	float32V := float32(5.5)
	float64V := 11.5
	boolV := true

	return event{
		Int:     &intV,
		Int8:    &int8V,
		Uint8:   &uint8V,
		Int16:   &int16V,
		Uint16:  &uint16V,
		Int32:   &int32V,
		Uint32:  &uint32V,
		Int64:   &int64V,
		Uint64:  &uint64V,
		Byte:    &byteV,
		String:  &stringV,
		Float32: &float32V,
		Float64: &float64V,
		Bool:    &boolV,
	}
}
