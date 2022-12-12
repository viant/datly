package json_test

import (
	goJson "encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/internal/tests"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/format"
	"reflect"
	"testing"
	"time"
)

func TestJson_Marshal(t *testing.T) {
	testcases := []struct {
		description   string
		data          func() interface{}
		expect        string
		defaultConfig marshal.Default
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
			defaultConfig: marshal.Default{
				OmitEmpty:  true,
				CaseFormat: format.CaseLowerUnderscore,
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
			expect:      `[{"Int":1,"Int8":2,"Int16":3,"Int32":4,"Int64":5,"Uint":6,"Uint8":7,"Uint16":8,"Uint32":9,"Uint64":10,"IntPtr":1,"Int8Ptr":2,"Int16Ptr":3,"Int32Ptr":4,"Int64Ptr":5,"UintPtr":6,"Uint8Ptr":7,"Uint16Ptr":8,"Uint32Ptr":9,"Uint64Ptr":10,"String":"empty","StringPtr":"empty","Bool":false,"BoolPtr":false,"Float32":10.5,"Float32Ptr":10.5,"Float64":20.5,"Float64Ptr":20.5,"Time":"2012-07-12","TimePtr":2022-02-08}]`,
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
			defaultConfig: marshal.Default{
				CaseFormat: format.CaseLowerCamel,
			},
		},
		//TODO: Handle that case
		//{
		//	description: "marshal non ptr",
		//	data:        nonPtr,
		//	expect:      `[{"id":10,"name":"foo","price":125.5}]`,
		//	defaultConfig: marshal.Default{
		//		CaseFormat: format.CaseLowerCamel,
		//	},
		//},
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

		marshaller, err := json.New(dataType, testcase.defaultConfig)
		if !assert.Nil(t, err, testcase.description) {
			t.Fail()
			return
		}

		result, err := marshaller.Marshal(data, testcase.filters)
		if !assert.Nil(t, err, testcase.description) {
			t.Fail()
			return
		}

		if !assert.Equal(t, testcase.expect, string(result), testcase.description) {
			toolbox.Dump(string(result))
		}
	}
}

func nonPtr() interface{} {
	type Response struct {
		Message interface{}
		Status  string
	}

	type Event struct {
		ID    int
		Name  string
		Price float64
	}

	type Data struct {
		Response
		Events []*Event
	}

	return Data{
		Response: Response{},
		Events: []*Event{
			{
				ID:    1,
				Name:  "ABC",
				Price: 125.5,
			},
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

	benchMarshaller, _ = json.New(reflect.TypeOf(&Event{}), marshal.Default{})

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
			bytes, err = benchMarshaller.Marshal(benchEvents, nil)
		}

		assert.Nil(b, err)
		assert.Equal(b, `[{"ID":1,"Name":"Event - 1","Price":123,"Types":[{"TypeID":1,"Type":"Type - 1"},{"TypeID":2,"Type":"Type - 2"}]},{"ID":2,"Name":"Event - 2","Price":226,"Types":[{"TypeID":2,"Type":"Type - 2"},{"TypeID":3,"Type":"Type - 3"},{"TypeID":4,"Type":"Type - 4"},{"TypeID":5,"Type":"Type - 5"},{"TypeID":6,"Type":"Type - 6"},{"TypeID":7,"Type":"Type - 7"}]}]`, string(bytes))
	})
}
