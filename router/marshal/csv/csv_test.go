package csv

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"reflect"
	"testing"
)

func TestCsv_Unmarshal(t *testing.T) {
	type Foo struct {
		ID    int
		Name  string
		Price float64
	}

	type Boo struct {
		ID    int
		Name  string
		Price float64
		Foo   *Foo
	}

	type BooMany struct {
		ID    int
		Name  string
		Price float64
		Foo   []*Foo
	}

	type EventType struct {
		ID    int     `csvName:"Type id"`
		Price float64 `csvName:"Type price"`
		Name  string  `csvName:"Type name"`
	}

	type Event struct {
		ID        int
		Name      string
		Price     float64
		EventType *EventType
	}

	testCases := []struct {
		description string
		rType       reflect.Type
		input       string
		expected    string
		config      *Config
	}{
		{
			description: "basic",
			input: `ID,Name,Price
1,"foo",125.5`,
			rType:    reflect.TypeOf(Foo{}),
			expected: `[{"ID":1,"Name":"foo","Price":125.5}]`,
		},
		{
			description: "multiple rows",
			input: `ID,Name,Price
1,"foo",125.5
2,"boo",250`,
			rType:    reflect.TypeOf(Foo{}),
			expected: `[{"ID":1,"Name":"foo","Price":125.5}, {"ID": 2, "Name": "boo", "Price": 250}]`,
		},
		{
			description: "one to one relation",
			input: `ID,Name,Price,Foo.ID,Foo.Name,Foo.Price
1,"Boo",250,10,"Foo",125.5`,
			rType:    reflect.TypeOf(Boo{}),
			expected: `[{"ID":1,"Name":"Boo","Price":250,"Foo":{"ID":10,"Name":"Foo","Price":125.5}}]`,
		},
		{
			description: "one to many relations",
			input: `ID,Name,Price,Foo.ID,Foo.Name,Foo.Price
1,"Boo",250,10,"Foo",125.5`,
			rType:    reflect.TypeOf(BooMany{}),
			expected: `[{"ID":1,"Name":"Boo","Price":250,"Foo":[{"ID":10,"Name":"Foo","Price":125.5}]}]`,
		},
		{
			description: "one to many, multiple rows",
			input: `ID,Name,Price,Foo.ID,Foo.Name,Foo.Price
1,"Boo",250,10,"Foo",125.5
2,"Boo - 2",50,20,"Foo - 2",300`,
			rType:    reflect.TypeOf(BooMany{}),
			expected: `[{"ID":1,"Name":"Boo","Price":250,"Foo":[{"ID":10,"Name":"Foo","Price":125.5}]},{"ID":2,"Name":"Boo - 2","Price":50,"Foo":[{"ID":20,"Name":"Foo - 2","Price":300}]}]`,
		},
		{
			description: "one to one, custom names",
			input: `ID,Name,Price,Type id,Type name,Type price
1,"Boo",250,10,"Foo",125.5
2,"Boo - 2",50,20,"Foo - 2",300`,
			rType:    reflect.TypeOf(Event{}),
			expected: `[{"ID":1,"Name":"Boo","Price":250,"EventType":{"ID":10,"Price":125.5,"Name":"Foo"}},{"ID":2,"Name":"Boo - 2","Price":50,"EventType":{"ID":20,"Price":300,"Name":"Foo - 2"}}]`,
		},
	}

	for _, testCase := range testCases[len(testCases)-1:] {
		//for _, testCase := range testCases {
		marshaller, err := NewMarshaller(testCase.rType, testCase.config)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		dest := reflect.New(reflect.SliceOf(testCase.rType)).Interface()
		if !assert.Nil(t, marshaller.Unmarshal([]byte(testCase.input), dest), testCase.description) {
			continue
		}

		if !assertly.AssertValues(t, testCase.expected, dest) {
			toolbox.Dump(dest)
		}
	}
}
