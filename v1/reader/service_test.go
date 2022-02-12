package reader

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/v1/data"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"reflect"
	"testing"
	"time"
)

func TestRead(t *testing.T) {
	type Event struct {
		ID          int
		EventTypeID int
		Quantity    float64
		Timestamp   time.Time
	}

	type Foo struct {
		Id   int
		Name string
	}

	testLocation := toolbox.CallerDirectory(3)

	var useCases = []struct {
		selectors             data.Selectors
		description           string
		dataURI               string
		expect                string
		dest                  interface{}
		errorOnClientSelector bool
		view                  string
		options               Options
		compType              reflect.Type
		compTypeName          string
	}{
		{
			description:  "read all data with specified columns",
			dataURI:      "case001/",
			dest:         new([]*Event),
			view:         "events",
			expect:       `[{"ID":1,"EventTypeID":2,"Quantity":33.23432374000549,"Timestamp":"0001-01-01T00:00:00Z"},{"ID":10,"EventTypeID":11,"Quantity":21.957962334156036,"Timestamp":"0001-01-01T00:00:00Z"},{"ID":100,"EventTypeID":111,"Quantity":5.084940046072006,"Timestamp":"0001-01-01T00:00:00Z"}]`,
			compTypeName: "events",
			compType:     reflect.TypeOf(&Event{}),
		},
		{
			description: "read all data with specified columns",
			dataURI:     "case002/",
			dest:        new(interface{}),
			view:        "events",
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description: "excluded columns",
			dataURI:     "case003/",
			dest:        new(interface{}),
			view:        "events",
			expect:      `[{"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"UserId":1},{"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2},{"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description: "more complex selector",
			dataURI:     "case004/",
			view:        "events",
			dest:        new(interface{}),
			expect:      `[{"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2},{"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3}]`,
			selectors: map[string]*data.Selector{
				"events": {
					Offset: 1,
				},
			},
		},
		{
			description: "read unmapped",
			dataURI:     "case005/",
			view:        "foos",
			dest:        new([]*Foo),
			options: Options{
				AllowUnmapped(true),
			},
			expect: `[{"Id":1,"Name":"foo"},{"Id":2,"Name":"another foo"},{"Id":3,"Name":"yet another foo"}]`,
		},
		{
			description: "columns expression",
			dataURI:     "case006/",
			view:        "foos",
			dest:        new([]*Foo),
			expect:      `[{"Id":1,"Name":"FOO"},{"Id":2,"Name":"ANOTHER FOO"},{"Id":3,"Name":"YET ANOTHER FOO"}]`,
		},
		{
			description: "custom selector",
			dataURI:     "case007/",
			view:        "events",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Quantity":33.23432374000549},{"Id":10,"Quantity":21.957962334156036},{"Id":100,"Quantity":5.084940046072006}]`,
			selectors: map[string]*data.Selector{
				"events": {
					Columns: []string{"id", "quantity"},
					OrderBy: "id",
					Offset:  0,
					Limit:   0,
				},
			},
		},
		{
			description: "one to one, include false",
			dataURI:     "case008/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventType":null,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventType":{"Id":11,"Name":"type 2","AccountId":33},"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventType":{"Id":111,"Name":"type 3","AccountId":36},"Quantity":5.084940046072006,"UserId":3}]`,
		},

		{
			description: "one to one, include join column true",
			dataURI:     "case014/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventType":null,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventType":{"Id":11,"Name":"type 2","AccountId":33},"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventType":{"Id":111,"Name":"type 3","AccountId":36},"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			dataURI:     "case009/",
			view:        "users_accounts",
			description: "many to one",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Name":"John","Accounts":[{"Id":1,"Name":"John account","UserId":1},{"Id":3,"Name":"Another John account","UserId":1}]},{"Id":2,"Name":"David","Accounts":[{"Id":2,"Name":"Anna account","UserId":2}]},{"Id":3,"Name":"Anna","Accounts":null}]`,
		},
		//{
		//	dataURI:     "case010/",
		//	view:        "events",
		//	description: "expressions as columns",
		//	dest:        new(interface{}),
		//	expect:      `[{"Id":1,"Quantity":33.23432374000549,"EventTypeId":2,"Current_time":"2022-02-06"},{"Id":10,"Quantity":21.957962334156036,"EventTypeId":11,"Current_time":"2022-02-06"},{"Id":100,"Quantity":5.084940046072006,"EventTypeId":111,"Current_time":"2022-02-06"}]`,
		//},
		//{
		//	dataURI:     "case011/",
		//	view:        "dual",
		//	description: "read from dual like table",
		//	dest:        new(interface{}),
		//	expect:      `[{"Id":123,"Quantity":255.5,"Registered":false,"Name":"abc"}]`,
		//},
		//{
		//	dataURI:     "case012/",
		//	view:        "event_event-types",
		//	description: "read from dual like table",
		//	dest:        new(interface{}),
		//	expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventType":null,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventType":{"Id":11,"Name":"type 2","AccountId":33},"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventType":{"Id":111,"Name":"type 3","AccountId":36},"Quantity":5.084940046072006,"UserId":3}]`,
		//},
		//{
		//	dataURI:     "case013/",
		//	view:        "articles_languages",
		//	description: "read from dual like table",
		//	dest:        new(interface{}),
		//	expect:      `[{"Id":1,"Content":"Lorem ipsum","Language":{"Id":2,"Code":"en-US"}},{"Id":2,"Content":"dolor sit amet","Language":{"Id":12,"Code":"ky-KG"}},{"Id":3,"Content":"consectetur adipiscing elit","Language":{"Id":13,"Code":"lb-LU"}},{"Id":4,"Content":"sed do eiusmod tempor incididunt","Language":{"Id":9,"Code":"zh-CN"}}]`,
		//},

		//{
		//	description: "selector columns doesn't overlap view columns",
		//	dataURI:     "case007/",
		//	view:        "foos",
		//	selectors: &view.Request{
		//		DataColumns: []string{"Id", "abcdef"},
		//	},
		//	options:        []interface{}{AllowUnmapped(true)},
		//	errorOnClientSelector: false,
		//	dest:           new([]*Foo),
		//},
		//
		//{
		//	description: "client selector should be used instead of view selector",
		//	dataURI:     "case001/",
		//	view: &view.view{
		//		Connector: "mydb",
		//		Name:      "foos",
		//		Table:     "foos",
		//		selector: view.selector{
		//			Columns: []string{"Id", "Name"},
		//			OrderBy: "Id",
		//			Offset:  0,
		//			Limit:   100,
		//		},
		//		Columns: []*data.Column{
		//			{
		//				Name: "Id",
		//			},
		//			{
		//				Name: "Name",
		//			},
		//		},
		//	},
		//	selectors: &view.Request{
		//		DataColumns: []string{"Name"},
		//		DataOrderBy: "Name",
		//		DataOffset:  1,
		//	},
		//	options:        []interface{}{AllowUnmapped(true)},
		//	errorOnClientSelector: true,
		//	connectors: []*config.Connector{
		//		{
		//			Name:   "mydb",
		//			Driver: "sqlite3",
		//			DSN:    "./testdata/db/mydb.db",
		//		},
		//	},
		//	dest:   new(interface{}),
		//	expect: ``,
		//},
		//

	}

	for _, testCase := range useCases[:10] {
		if initDb(t, path.Join(testLocation, "testdata", "mydb_config.yaml"), path.Join(testLocation, fmt.Sprintf("testdata/case/populate_mydb")), "db") {
			return
		}

		if initDb(t, path.Join(testLocation, "testdata", "other_config.yaml"), path.Join(testLocation, fmt.Sprintf("testdata/case/populate_other")), "other") {
			return
		}

		types := data.Types{}
		if testCase.compType != nil {
			types.Register(testCase.compTypeName, testCase.compType)
		}
		resource, err := data.NewResourceFromURL(context.TODO(), path.Join(testLocation, fmt.Sprintf("testdata/case/"+testCase.dataURI+"/resources.yaml")), types)
		if err != nil {
			t.Fatalf(err.Error())
		}

		service := New()
		service.Apply(testCase.options)

		dataView, _ := resource.View(testCase.view)

		if (err != nil) && testCase.errorOnClientSelector {
			t.Fatal(err)
		}

		session := &data.Session{
			Dest:      testCase.dest,
			View:      dataView,
			Selectors: testCase.selectors,
		}

		session.Init()

		err = service.Read(context.TODO(), session)
		assert.Nil(t, err, testCase.description)
		b, _ := json.Marshal(testCase.dest)
		result := string(b)
		if !assertly.AssertValues(t, testCase.expect, result, testCase.description) {
			fmt.Println(result)
			fmt.Println(testCase.expect)
		}
	}
}

func initDb(t *testing.T, configPath, datasetPath, dataStore string) bool {
	if !dsunit.InitFromURL(t, configPath) {
		return true
	}

	initDataset := dsunit.NewDatasetResource(dataStore, datasetPath, "", "")
	if !dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset)) {
		return true
	}
	return false
}
