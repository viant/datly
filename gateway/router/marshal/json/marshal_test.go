package json_test

import (
	goJson "encoding/json"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/internal/tests"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestJson_Marshal(t *testing.T) {
	testcases := []struct {
		description   string
		data          func() interface{}
		expect        string
		defaultConfig config.IOConfig
		filters       *json.Filters
	}{
		{
			description: "primitive",
			data:        event,
			expect:      `{"Int":1,"Int8":2,"Uint8":3,"Int16":4,"Uint16":5,"Int32":6,"Uint32":7,"Int64":8,"Uint64":9,"Byte":10,"String":"string","Float32":5.5,"Float64":11.5,"Bool":true}`,
		},
		{
			description: "primitive pointers",
			data:        eventPtr,
			expect:      `{"Int":1,"Int8":2,"Uint8":3,"Int16":4,"Uint16":5,"Int32":6,"Uint32":7,"Int64":8,"Uint64":9,"Byte":10,"String":"string","Float32":5.5,"Float64":11.5,"Bool":true}`,
		},
		{
			description: "nils",
			data:        nilsPtr,
			expect:      `{"Int":null,"Int8":null,"Uint8":null,"Int16":null,"Uint16":null,"Int32":null,"Uint32":null,"Int64":null,"Uint64":null,"Byte":null,"String":null,"Float32":null,"Float64":null,"Bool":null}`,
		},
		{
			description: "slice without relations",
			data:        sliceWithoutRelations,
			expect:      `[{"Int":10,"String":"str - 1","Float64":20.5},{"Int":15,"String":"str - 2","Float64":40.5},{"Int":5,"String":"str - 0","Float64":0.5}]`,
		},
		{
			description: "slice with relations",
			data:        sliceWithRelations,
			expect:      `{"Int":100,"String":"abc","Float64":0,"EventType":{"Id":200,"Type":"event-type-1"}}`,
		},
		{
			description: "nil slice and *T",
			data:        nilNonPrimitives,
			expect:      `[{"Id":231,"EventTypesEmpty":[],"EventTypes":[{"Id":1,"Type":"t - 1"},null,{"Id":1,"Type":"t - 3"}],"Name":"","EventType":null}]`,
		},
		{
			description: "caser and json tags",
			data:        caserAndJson,
			expect:      `[{"id":1,"quantity":125.5,"EventName":"ev-1","time_ptr":"2012-07-12T00:00:00Z"},{"id":2,"quantity":250.5,"time":"2022-05-10T00:00:00Z"}]`,
			defaultConfig: config.IOConfig{
				OmitEmpty:  true,
				CaseFormat: text.CaseFormatLowerUnderscore,
			},
		},
		{
			description: "filtered fields",
			data:        sliceWithRelations,
			expect:      `{"Int":100,"EventType":{"Type":"event-type-1"}}`,
			filters: json.NewFilters(
				&json.FilterEntry{Fields: []string{"Int", "EventType"}},
				&json.FilterEntry{Path: "EventType", Fields: []string{"Type"}},
			),
		},
		{
			description: "interface",
			data:        withInterface,
			expect:      `{"Int":100,"EventType":{"Type":"event-type-1"}}`,
			filters: json.NewFilters(
				&json.FilterEntry{Fields: []string{"Int", "EventType"}},
				&json.FilterEntry{Path: "EventType", Fields: []string{"Type"}},
			),
		},
		{
			description: "anonymous",
			data:        anonymous,
			expect:      `{"Id":10,"Quantity":125.5}`,
			filters: json.NewFilters(
				&json.FilterEntry{Fields: []string{"Id", "Quantity"}},
			),
		},
		{
			description: "default tag",
			data:        defaultTag,
			expect:      `[{"Int":1,"Int8":2,"Int16":3,"Int32":4,"Int64":5,"Uint":6,"Uint8":7,"Uint16":8,"Uint32":9,"Uint64":10,"IntPtr":1,"Int8Ptr":2,"Int16Ptr":3,"Int32Ptr":4,"Int64Ptr":5,"UintPtr":6,"Uint8Ptr":7,"Uint16Ptr":8,"Uint32Ptr":9,"Uint64Ptr":10,"String":"empty","StringPtr":"empty","Bool":false,"BoolPtr":false,"Float32":10.5,"Float32Ptr":10.5,"Float64":20.5,"Float64Ptr":20.5,"Time":"2012-07-12","TimePtr":"2022-02-08"}]`,
		},
		{
			description: "primitive slice",
			data:        primitiveSlice,
			expect:      `["abc","def","ghi"]`,
		},
		{
			description: "primitive nested slice",
			data:        primitiveNestedSlice,
			expect:      `[{"Name":"N - 1","Price":125.5,"Ints":[1,2,3]},{"Name":"N - 1","Price":250.5,"Ints":[4,5,6]}]`,
		},
		{
			description: "anonymous nested struct",
			data:        anonymousNestedStruct,
			expect:      `{"ResponseStatus":{"Message":"","Status":"","Error":""},"Foo":[{"ID":1,"Name":"abc","Quantity":0},{"ID":2,"Name":"def","Quantity":250}]}`,
		},
		{
			description: "anonymous nested struct with ptrs",
			data:        anonymousNestedStructWithPointers,
			expect:      `{"FooWrapperName":"","Foo":[{"ID":1,"Name":"abc","Quantity":0},{"ID":2,"Name":"def","Quantity":250}]}`,
		},
		{
			description: "anonymous nested complex struct with ptrs",
			data:        complexAnonymousNestedStructWithPointers,
			expect:      `{"Status":0,"Message":"","FooWrapperName":"","Foo":[{"ID":1,"Name":"abc","Quantity":0},{"ID":2,"Name":"def","Quantity":250}],"Timestamp":"0001-01-01T00:00:00Z"}`,
		},
		{
			description: "ID field",
			data:        idStruct,
			expect:      `[{"id":10,"name":"foo","price":125.5}]`,
			defaultConfig: config.IOConfig{
				CaseFormat: text.CaseFormatLowerCamel,
			},
		},
		{
			description: "embedded",
			data:        embeddable,
			expect:      `{"id":10,"name":"foo","price":125.5}`,
			defaultConfig: config.IOConfig{
				CaseFormat: text.CaseFormatLowerCamel,
			},
		},
		{
			description: "inlining",
			data:        inlinable,
			expect:      `{"id":12,"name":"Foo name","price":125.567}`,
			defaultConfig: config.IOConfig{
				CaseFormat: text.CaseFormatLowerCamel,
			},
		},
		{
			description: "*json.RawMessage",
			data:        jsonRawMessagePtr,
			expect:      `{"id":12,"name":"Foo name","price":125.567}`,
			defaultConfig: config.IOConfig{
				CaseFormat: text.CaseFormatLowerCamel,
			},
		},
		{
			description: "json.RawMessage",
			data:        jsonRawMessage,
			expect:      `{"id":12,"name":"Foo name","price":125.567}`,
			defaultConfig: config.IOConfig{
				CaseFormat: text.CaseFormatLowerCamel,
			},
		},
		{
			description: "interface slice",
			expect:      `{"ID":1,"Name":"abc","MgrId":0,"AccountId":2,"Team":[{"ID":10,"Name":"xx","MgrId":0,"AccountId":2,"Team":[]}]}`,
			data: func() interface{} {
				type Member struct {
					ID        int
					Name      string
					MgrId     int
					AccountId int
					Team      []interface{}
				}

				return &Member{
					ID:        1,
					Name:      "abc",
					AccountId: 2,
					Team: []interface{}{
						&Member{
							ID:        10,
							Name:      "xx",
							AccountId: 2,
						},
					},
				}
			},
		},
	}

	//for i, testcase := range testcases[:len(testcases)-1] {
	//for i, testcase := range testcases[len(testcases)-1:] {
	for i, testcase := range testcases {
		json.ResetCache()
		tests.LogHeader(fmt.Sprintf("Running testcase nr: %v out of %v \n ", i, len(testcases)-1))
		data := testcase.data()

		dataType := reflect.TypeOf(data)
		if dataType.Kind() == reflect.Slice {
			dataType = dataType.Elem()
		}

		marshaller := json.New(testcase.defaultConfig)
		result, err := marshaller.Marshal(data, testcase.filters)
		if !assert.Nil(t, err, testcase.description) {
			t.Fail()
			return
		}
		if string(result) == testcase.expect {
			continue
		}
		if !assertly.AssertValues(t, testcase.expect, string(result), testcase.description) {
			fmt.Println(string(result))
			fmt.Println(string(testcase.expect))

		}

	}
}

func jsonRawMessage() interface{} {
	type Foo struct {
		ID       int
		JSONBody goJson.RawMessage `jsonx:"inline"`
		Name     string
	}

	jsonBody := goJson.RawMessage([]byte(`{"id":12,"name":"Foo name","price":125.567}`))
	return &Foo{
		ID:       125,
		Name:     "Abdef",
		JSONBody: jsonBody,
	}
}

func jsonRawMessagePtr() interface{} {
	type Foo struct {
		ID       int
		JSONBody *goJson.RawMessage `jsonx:"inline"`
		Name     string
	}

	jsonBody := goJson.RawMessage([]byte(`{"id":12,"name":"Foo name","price":125.567}`))
	return &Foo{
		ID:       125,
		Name:     "Abdef",
		JSONBody: &jsonBody,
	}
}

func embeddable() interface{} {
	type Foo struct {
		ID         int
		Embeddable map[string]interface{} `default:"embedded=true"`
	}

	return &Foo{
		ID: 10,
		Embeddable: map[string]interface{}{
			"name":  "foo",
			"price": 125.5,
		},
	}
}

func idStruct() interface{} {
	type Foo struct {
		ID    int
		Name  string
		Price float64
	}

	return []*Foo{
		{
			ID:    10,
			Name:  "foo",
			Price: 125.5,
		},
	}
}

func complexAnonymousNestedStructWithPointers() interface{} {
	type Foo struct {
		ID       int
		Name     string
		Quantity int
	}

	type FooWrapper struct {
		FooWrapperName string
		Foo            []*Foo
	}

	type Response struct {
		Status  int
		Message string
		*FooWrapper
		Timestamp time.Time
	}

	return Response{
		FooWrapper: &FooWrapper{
			Foo: []*Foo{
				{
					ID:       1,
					Name:     "abc",
					Quantity: 0,
				},
				{
					ID:       2,
					Name:     "def",
					Quantity: 250,
				},
			},
		},
	}
}

func anonymousNestedStructWithPointers() interface{} {
	type Foo struct {
		ID       int
		Name     string
		Quantity int
	}

	type FooWrapper struct {
		FooWrapperName string
		Foo            []*Foo
	}

	type Response struct {
		*FooWrapper
	}

	return &Response{
		FooWrapper: &FooWrapper{
			Foo: []*Foo{
				{
					ID:       1,
					Name:     "abc",
					Quantity: 0,
				},
				{
					ID:       2,
					Name:     "def",
					Quantity: 250,
				},
			},
		},
	}
}

func anonymousNestedStruct() interface{} {
	type Foo struct {
		ID       int
		Name     string
		Quantity int
	}

	type FooWrapper struct {
		Foo []*Foo
	}

	type ResponseStatus struct {
		Message string
		Status  string
		Error   string
	}

	type Response struct {
		ResponseStatus ResponseStatus
		FooWrapper
	}

	return Response{
		ResponseStatus: ResponseStatus{},
		FooWrapper: FooWrapper{
			Foo: []*Foo{
				{
					ID:       1,
					Name:     "abc",
					Quantity: 0,
				},
				{
					ID:       2,
					Name:     "def",
					Quantity: 250,
				},
			},
		},
	}
}

func primitiveNestedSlice() interface{} {
	type Foo struct {
		Name  string
		Price float64
		Ints  []int
	}

	return []Foo{
		{
			Name:  "N - 1",
			Price: 125.5,
			Ints:  []int{1, 2, 3},
		},
		{
			Name:  "N - 1",
			Price: 250.5,
			Ints:  []int{4, 5, 6},
		},
	}
}

func primitiveSlice() interface{} {
	return []string{"abc", "def", "ghi"}
}

func defaultTag() interface{} {
	type event struct {
		Int        int        `default:"value=1"`
		Int8       int8       `default:"value=2"`
		Int16      int16      `default:"value=3"`
		Int32      int32      `default:"value=4"`
		Int64      int64      `default:"value=5"`
		Uint       uint       `default:"value=6"`
		Uint8      uint8      `default:"value=7"`
		Uint16     uint16     `default:"value=8"`
		Uint32     uint32     `default:"value=9"`
		Uint64     uint64     `default:"value=10"`
		IntPtr     *int       `default:"value=1"`
		Int8Ptr    *int8      `default:"value=2"`
		Int16Ptr   *int16     `default:"value=3"`
		Int32Ptr   *int32     `default:"value=4"`
		Int64Ptr   *int64     `default:"value=5"`
		UintPtr    *uint      `default:"value=6"`
		Uint8Ptr   *uint8     `default:"value=7"`
		Uint16Ptr  *uint16    `default:"value=8"`
		Uint32Ptr  *uint32    `default:"value=9"`
		Uint64Ptr  *uint64    `default:"value=10"`
		String     string     `default:"value=empty"`
		StringPtr  *string    `default:"value=empty"`
		Bool       bool       `default:"value=false"`
		BoolPtr    *bool      `default:"value=false"`
		Float32    float32    `default:"value=10.5"`
		Float32Ptr *float32   `default:"value=10.5"`
		Float64    float64    `default:"value=20.5"`
		Float64Ptr *float64   `default:"value=20.5"`
		Time       time.Time  `default:"format=2006-01-02"`
		TimePtr    *time.Time `default:"value=2022-02-08,format=2006-01-02"`
	}

	return []event{
		{
			Time: newTime("12-07-2012"),
		},
	}
}

func anonymous() interface{} {
	type Event struct {
		Id       int
		Name     string
		Quantity float64
	}

	type eventHolder struct {
		Event
	}

	return eventHolder{
		Event{
			Id:       10,
			Name:     "event - name",
			Quantity: 125.5,
		},
	}
}

func caserAndJson() interface{} {
	type event struct {
		Id       int
		Quantity float64
		Name     string `json:"EventName,omitempty"`
		Type     string `json:"-,omitempty"`
		TimePtr  *time.Time
		Time     time.Time
	}

	timePtr := newTime("12-07-2012")
	return []*event{
		{
			Id:       1,
			Quantity: 125.5,
			Name:     "ev-1",
			Type:     "removed from json",
			Time:     time.Time{},
			TimePtr:  &timePtr,
		},
		{
			Id:       2,
			Quantity: 250.5,
			Type:     "removed from json",
			Time:     newTime("10-05-2022"),
		},
	}
}

func newTime(s string) time.Time {
	layout := "02-01-2006"
	time, _ := time.Parse(layout, s)
	return time
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

func withInterface() interface{} {
	type eventType struct {
		Id   int
		Type string
	}

	type event struct {
		Int       int
		String    string
		Float64   float64
		EventType interface{}
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

func BenchmarkMarshal(b *testing.B) {
	type EventType struct {
		TypeID int
		Type   string
	}

	type Event struct {
		ID    int
		Name  string
		Price float64
		Types []EventType
	}

	var benchEvents []*Event
	var benchMarshaller *json.Marshaller

	benchEvents = []*Event{
		{
			ID:    1,
			Name:  "Event - 1",
			Price: 123,
			Types: []EventType{
				{
					TypeID: 1,
					Type:   "Type - 1",
				},
				{
					TypeID: 2,
					Type:   "Type - 2",
				},
			},
		},
		{
			ID:    2,
			Name:  "Event - 2",
			Price: 226,
			Types: []EventType{
				{
					TypeID: 2,
					Type:   "Type - 2",
				},
				{
					TypeID: 3,
					Type:   "Type - 3",
				},
				{
					TypeID: 4,
					Type:   "Type - 4",
				},
				{
					TypeID: 5,
					Type:   "Type - 5",
				},
				{
					TypeID: 6,
					Type:   "Type - 6",
				},
				{
					TypeID: 7,
					Type:   "Type - 7",
				},
			},
		},
	}

	benchMarshaller = json.New(config.IOConfig{})

	b.Run("SDK json", func(b *testing.B) {
		var bytes []byte
		var err error
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			bytes, err = goJson.Marshal(benchEvents)
		}

		assert.Nil(b, err)
		assert.Equal(b, `[{"ID":1,"Name":"Event - 1","Price":123,"Types":[{"TypeID":1,"Type":"Type - 1"},{"TypeID":2,"Type":"Type - 2"}]},{"ID":2,"Name":"Event - 2","Price":226,"Types":[{"TypeID":2,"Type":"Type - 2"},{"TypeID":3,"Type":"Type - 3"},{"TypeID":4,"Type":"Type - 4"},{"TypeID":5,"Type":"Type - 5"},{"TypeID":6,"Type":"Type - 6"},{"TypeID":7,"Type":"Type - 7"}]}]`, string(bytes))
	})

	b.Run("Custom json", func(b *testing.B) {
		var bytes []byte
		var err error
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			bytes, err = benchMarshaller.Marshal(benchEvents)
		}

		assert.Nil(b, err)
		assert.Equal(b, `[{"ID":1,"Name":"Event - 1","Price":123,"Types":[{"TypeID":1,"Type":"Type - 1"},{"TypeID":2,"Type":"Type - 2"}]},{"ID":2,"Name":"Event - 2","Price":226,"Types":[{"TypeID":2,"Type":"Type - 2"},{"TypeID":3,"Type":"Type - 3"},{"TypeID":4,"Type":"Type - 4"},{"TypeID":5,"Type":"Type - 5"},{"TypeID":6,"Type":"Type - 6"},{"TypeID":7,"Type":"Type - 7"}]}]`, string(bytes))
	})
}
func inlinable() interface{} {
	type Foo struct {
		ID    int
		Name  string
		Price float64
	}

	type FooAudit struct {
		CreatedAt time.Time
		UpdatedAt time.Time
		Foo       Foo `jsonx:"inline"`
	}

	return &FooAudit{
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Foo: Foo{
			ID:    12,
			Name:  "Foo name",
			Price: 125.567,
		},
	}
}

type unmarshallTestcase struct {
	description   string
	data          string
	into          func() interface{}
	expect        string
	expectError   bool
	stringsEqual  bool
	options       []interface{}
	marshallEqual bool
}

type intsSum int

func (i *intsSum) UnmarshalJSONWithOptions(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	var ints []int
	if err := decoder.SliceInt(&ints); err != nil {
		return err
	}

	sum := intsSum(0)
	for _, value := range ints {
		sum = intsSum(value) + sum
	}

	*dst.(**intsSum) = &sum
	return nil
}

func TestMarshaller_Unmarshal(t *testing.T) {
	testCases := []unmarshallTestcase{
		{
			description: "basic struct with missing colon",
			data:        `{"Name": "Foo" "ID": 2}`,
			expect:      `{"Name": "Foo","ID": 2}`,
			expectError: false,
			into: func() interface{} {
				type Foo struct {
					ID   int
					Name string
				}

				return &Foo{}
			},
		},
		{
			description: "basic struct",
			data:        `{"Name": "Foo", "ID": 1}`,
			into: func() interface{} {
				type Foo struct {
					ID   int
					Name string
				}

				return &Foo{}
			},
		},
		{
			description: "basic slice",
			data:        `[{"Name": "Foo", "ID": 1},{"Name": "Boo", "ID": 2}]`,
			into: func() interface{} {
				type Foo struct {
					ID   int
					Name string
				}

				return &[]*Foo{}
			},
		},
		{
			description: "empty slice",
			data:        `[]`,
			into: func() interface{} {
				type Foo struct {
					ID   int
					Name string
				}

				return &[]*Foo{}
			},
		},
		{
			description: "has",
			data:        `[{"ID": 1}, {"Name": "Boo"}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `setMarker:"true"`
				}

				return &[]*Foo{}
			},
			expect: `[{"ID":1,"Name":"","Has":{"ID":true,"Name":false}},{"ID":0,"Name":"Boo","Has":{"ID":false,"Name":true}}]`,
		},
		{
			description: "setting has",
			data:        `[{"ID": 1, "Has": {"ID": true, "Name": "true"}}, {"Name": "Boo"}]`,
			expect:      `[{"ID":1,"Name":"","Has":{"ID":true,"Name":false}},{"ID":0,"Name":"Boo","Has":{"ID":false,"Name":true}}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `setMarker:"true"`
				}

				return &[]*Foo{}
			},
		},
		{
			description: "setting has",
			data:        `[{"ID": 1, "Has": {"ID": true, "Name": "true"}}, {"Name": "Boo"}]`,
			expect:      `[{"ID":1,"Name":"","Has":{"ID":true,"Name":false}},{"ID":0,"Name":"Boo","Has":{"ID":false,"Name":true}}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `setMarker:"true"`
				}

				return &[]*Foo{}
			},
		},
		{
			description: "multi nesting",
			data: `[
	{
		"Size": 1,
		"Foos":[
			{"WrapperID": 1, "WrapperName": "wrapper - 1", "Foos": [{"ID": 10, "Name": "foo - 10"}]},
			{"WrapperID": 2, "WrapperName": "wrapper - 2", "Foos": [{"ID": 20, "Name": "foo - 20"}]}
		]
	}
]`,
			expect: `[{"Foos":[{"WrapperID":1,"WrapperName":"wrapper - 1","Foos":[{"ID":10,"Name":"foo - 10","Has":{"ID":true,"Name":true}}],"Has":{"WrapperID":true,"WrapperName":true}},{"WrapperID":2,"WrapperName":"wrapper - 2","Foos":[{"ID":20,"Name":"foo - 20","Has":{"ID":true,"Name":true}}],"Has":{"WrapperID":true,"WrapperName":true}}],"Size":1}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `setMarker:"true"`
				}

				type WrapperHas struct {
					WrapperID   bool
					WrapperName bool
				}

				type FooWrapper struct {
					WrapperID   int
					WrapperName string
					Foos        []*Foo
					Has         *WrapperHas `setMarker:"true"`
				}

				type Data struct {
					Foos []*FooWrapper
					Size int
				}

				return &[]*Data{}
			},
		},
		{
			description: "multi nesting",
			data: `[
	{
		"Size": 1,
		"Foos":[
			{"WrapperName": "wrapper - 1", "Foos": [{"ID": 10}]},
			{"WrapperID": 2, "Foos": [{"Name": "foo - 20"}]}
		]
	}
]`,
			expect: `[{"Foos":[{"WrapperID":0,"WrapperName":"wrapper - 1","Foos":[{"ID":10,"Name":"","Has":{"ID":true,"Name":false}}],"Has":{"WrapperID":false,"WrapperName":true}},{"WrapperID":2,"WrapperName":"","Foos":[{"ID":0,"Name":"foo - 20","Has":{"ID":false,"Name":true}}],"Has":{"WrapperID":true,"WrapperName":false}}],"Size":1}]`,
			into: func() interface{} {
				type FooHas struct {
					ID   bool
					Name bool
				}

				type Foo struct {
					ID   int
					Name string
					Has  *FooHas `setMarker:"true"`
				}

				type WrapperHas struct {
					WrapperID   bool
					WrapperName bool
				}

				type FooWrapper struct {
					WrapperID   int
					WrapperName string
					Foos        []*Foo
					Has         *WrapperHas `setMarker:"true"`
				}

				type Data struct {
					Foos []*FooWrapper
					Size int
				}

				return &[]*Data{}
			},
		},
		{
			description: "primitive slice",
			data:        `[1,2,3,4,5]`,
			expect:      `[1,2,3,4,5]`,
			into: func() interface{} {
				return new([]int)
			},
		},
		{
			description:  "nulls",
			data:         `{"ID":null,"Name":null}`,
			stringsEqual: true,
			into: func() interface{} {
				type Foo struct {
					ID   *int
					Name *string
				}

				return &Foo{}
			},
		},
		{
			description:  "empty presence index",
			data:         `{}`,
			expect:       `{"Has":{"ID":false,"Name":false}}`,
			stringsEqual: true,
			into: func() interface{} {
				type FooHasIndex struct {
					ID   bool
					Name bool
				}
				type Foo struct {
					ID   *int         `json:",omitempty"`
					Name *string      `json:",omitempty"`
					Has  *FooHasIndex `setMarker:"true"`
				}

				return &Foo{}
			},
		},
		{
			description:  "broken case 17",
			data:         ` {"Name":"017_"}`,
			expect:       `{"Id":0,"Name":"017_"}`,
			stringsEqual: true,
			into: func() interface{} {

				rType := reflect.TypeOf(struct {
					Id       int     "sqlx:\"name=ID,autoincrement,primaryKey,required\""
					Name     *string "sqlx:\"name=NAME\" json:\",omitempty\""
					Quantity *int    "sqlx:\"name=QUANTITY\" json:\",omitempty\""
					Has      *struct {
						Id       bool
						Name     bool
						Quantity bool
					} "setMarker:\"true\" typeName:\"EventsHas\" json:\"-\" sqlx:\"presence=true\""
				}{})
				v := reflect.New(rType)
				return v.Interface()
			},
		},
		{
			description:  "broken case 17",
			data:         `{"data":null}`,
			expect:       `{"data":null}`,
			stringsEqual: true,
			into: func() interface{} {

				rType := reflect.TypeOf(struct {
					Data *struct {
						Id       int     "sqlx:\"name=ID,autoincrement,primaryKey,required\""
						Name     *string "sqlx:\"name=NAME\" json:\",omitempty\""
						Quantity *int    "sqlx:\"name=QUANTITY\" json:\",omitempty\""
						Has      *struct {
							Id       bool
							Name     bool
							Quantity bool
						} "setMarker:\"true\" typeName:\"EventsHas\" json:\"-\" sqlx:\"presence=true\""
					}
				}{})
				v := reflect.New(rType)
				return v.Interface()
			},
		},
		httpUnmarshallTestcase("Boo", `{"NormalizeObject": {"Value1": "Abc", "Value2": 125.5}}`, `{"ID":0,"NormalizeObject":{"Value1":"Abc","Value2":125.5},"Name":""}`),
		httpUnmarshallTestcase("Bar", `{"NormalizeObject": {"CreatedAt": "time.Now", "UpdatedAt": "time.Now + 5 Days"}}`, `{"ID":0,"NormalizeObject":{"CreatedAt":"time.Now","UpdatedAt":"time.Now + 5 Days"},"Name":""}`),
		{
			description: "ints slice",
			data:        `{"Name": "Foo", "Ints": [1,2,3,4,5,6,7,8,9,10]}`,
			into: func() interface{} {
				type Foo struct {
					Name string
					Ints *intsSum
				}

				return &Foo{}
			},
			expect:        `{"Name":"Foo","Ints":55}`,
			stringsEqual:  true,
			marshallEqual: true,
		},
		{
			description: "invalid conversion object to slice",
			data:        `{"Name":"Foo", "ID": 1}`,
			expect:      `{"Name":"Foo", "ID": 1}`,
			expectError: true,
			into: func() interface{} {
				type Foo struct {
					ID   int
					Name string
				}

				return []*Foo{}
			},
		},
		{
			description: "invalid conversion slice to object",
			data:        `[{"Name":"Foo", "ID": 1}]`,
			expect:      `[{"Name": "Foo","ID": 1}]`,
			expectError: true,
			into: func() interface{} {
				type Foo struct {
					ID   int
					Name string
				}

				return &Foo{}
			},
		},
	}

	//for i, testCase := range testCases[len(testCases)-1:] {
	for i, testCase := range testCases {
		fmt.Printf("Running testcase nr#%v\n", i)
		actual := testCase.into()
		marshaller := json.New(config.IOConfig{})

		marshalErr := marshaller.Unmarshal([]byte(testCase.data), actual, testCase.options...)

		if testCase.expectError {
			assert.NotNil(t, marshalErr, testCase.description)
			continue
		}

		if !assert.Nil(t, marshalErr, testCase.description) {
			fmt.Println(marshalErr)
			continue
		}

		expect := testCase.expect
		if testCase.expect == "" {
			expect = testCase.data
		}

		if !testCase.stringsEqual {
			if !assertly.AssertValues(t, expect, actual, testCase.description) {
				bytes, _ := goJson.Marshal(actual)
				fmt.Printf("%s\n", bytes)
				fmt.Printf("%s\n", expect)

			}
		} else {
			bytes, _ := goJson.Marshal(actual)
			assert.Equal(t, expect, string(bytes), testCase.description)
		}

		if testCase.marshallEqual {
			actualBytes, err := marshaller.Marshal(actual)
			if !assert.Nil(t, err, testCase.description) {
				continue
			}

			assertly.AssertValues(t, testCase.expect, string(actualBytes), testCase.description)
		}
	}
}

func httpUnmarshallTestcase(typeName string, data string, expected string) unmarshallTestcase {
	request, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:8080/v1/api/dev/custom-unmarshall?type=%v", typeName), nil)
	if err != nil {
		panic(err)
	}

	type Foo struct {
		ID     int
		Object interface{}
		Name   string
	}

	type Bar struct {
		CreatedAt string
		UpdatedAt string
	}

	type Boo struct {
		Value1 string
		Value2 float64
	}

	return unmarshallTestcase{
		description:  "broken case 17",
		data:         data,
		expect:       expected,
		stringsEqual: true,
		options: []interface{}{
			request,
			json.UnmarshalerInterceptors{
				"NormalizeObject": func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
					var httpRequest *http.Request
					for _, option := range options {
						switch actual := option.(type) {
						case *http.Request:
							httpRequest = actual
						}
					}

					embeddedJSON := gojay.EmbeddedJSON{}
					if err = decoder.EmbeddedJSON(&embeddedJSON); err != nil {
						return err
					}

					actualDst := dst.(*interface{})
					query := httpRequest.URL.Query()
					switch query.Get("type") {
					case "Bar":
						aBar := &Bar{}

						if err = goJson.Unmarshal(embeddedJSON, aBar); err != nil {
							return err
						}

						*actualDst = aBar
						return nil

					default:
						aBoo := &Boo{}
						if err = goJson.Unmarshal(embeddedJSON, aBoo); err != nil {
							return err
						}

						*actualDst = aBoo
						return nil
					}
				},
			},
		},
		into: func() interface{} {
			return &Foo{}
		},
	}
}
