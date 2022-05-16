package json_test

import (
	goJson "encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
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
		metadata      json.StructMetadata
		expect        string
		defaultConfig marshal.Default
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
			expect:      `[{"Id":231,"EventTypesEmpty":null,"EventTypes":[{"Id":1,"Type":"t - 1"},null,{"Id":1,"Type":"t - 3"}],"Name":"","EventType":null}]`,
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
	}

	//for i, testcase := range testcases[len(testcases)-1:] {
	for i, testcase := range testcases {
		fmt.Printf("Running testcase nr: %v\n", i)
		data := testcase.data()
		marshaller, err := json.New(reflect.TypeOf(data), testcase.defaultConfig)
		if !assert.Nil(t, err, testcase.description) {
			t.Fail()
			return
		}

		result, err := marshaller.Marshal(data)
		if !assert.Nil(t, err, testcase.description) {
			t.Fail()
			return
		}

		if !assert.Equal(t, testcase.expect, string(result), testcase.description) {
			toolbox.Dump(string(result))
		}
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

//Benchmarks
type Event struct {
	ID    int
	Name  string
	Price float64
	Types []EventType
}

type EventType struct {
	TypeID int
	Type   string
}

var benchEvents []*Event
var benchMarshaller *json.Marshaller

func init() {
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
}

func BenchmarkMarshal(b *testing.B) {
	var bytes []byte
	var err error
	for i := 0; i < b.N; i++ {
		bytes, err = benchMarshaller.Marshal(benchEvents)
	}

	assert.Nil(b, err)
	assert.Equal(b, `[{"ID":1,"Name":"Event - 1","Price":123,"Types":[{"TypeID":1,"Type":"Type - 1"},{"TypeID":2,"Type":"Type - 2"}]},{"ID":2,"Name":"Event - 2","Price":226,"Types":[{"TypeID":2,"Type":"Type - 2"},{"TypeID":3,"Type":"Type - 3"},{"TypeID":4,"Type":"Type - 4"},{"TypeID":5,"Type":"Type - 5"},{"TypeID":6,"Type":"Type - 6"},{"TypeID":7,"Type":"Type - 7"}]}]`, string(bytes))
}

func BenchmarkJson_Marshal(b *testing.B) {
	var bytes []byte
	var err error
	for i := 0; i < b.N; i++ {
		bytes, err = goJson.Marshal(benchEvents)
	}

	assert.Nil(b, err)
	assert.Equal(b, `[{"ID":1,"Name":"Event - 1","Price":123,"Types":[{"TypeID":1,"Type":"Type - 1"},{"TypeID":2,"Type":"Type - 2"}]},{"ID":2,"Name":"Event - 2","Price":226,"Types":[{"TypeID":2,"Type":"Type - 2"},{"TypeID":3,"Type":"Type - 3"},{"TypeID":4,"Type":"Type - 4"},{"TypeID":5,"Type":"Type - 5"},{"TypeID":6,"Type":"Type - 6"},{"TypeID":7,"Type":"Type - 7"}]}]`, string(bytes))
}
