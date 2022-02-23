package reader

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/data"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"net/http"
	"net/url"
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

	type Language struct {
		Id   int
		Code string
	}

	type Article struct {
		Id       int
		Content  string
		LangId   int
		Language Language
	}

	testLocation := toolbox.CallerDirectory(3)

	var useCases = []struct {
		selectors   data.Selectors
		description string
		dataURI     string
		expect      string
		dest        interface{}
		view        string
		options     Options
		compTypes   map[string]reflect.Type
		subject     string
		request     *http.Request
		path        string
		expectError bool
	}{
		{
			description: "read all data with specified columns",
			dataURI:     "case001/",
			dest:        new([]*Event),
			view:        "events",
			expect:      `[{"ID":1,"EventTypeID":2,"Quantity":33.23432374000549,"Timestamp":"0001-01-01T00:00:00Z"},{"ID":10,"EventTypeID":11,"Quantity":21.957962334156036,"Timestamp":"0001-01-01T00:00:00Z"},{"ID":100,"EventTypeID":111,"Quantity":5.084940046072006,"Timestamp":"0001-01-01T00:00:00Z"}]`,
			compTypes: map[string]reflect.Type{
				"events": reflect.TypeOf(&Event{}),
			},
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
			compTypes: map[string]reflect.Type{
				"foo": reflect.TypeOf(&Foo{}),
			},
		},
		{
			description: "columns expression",
			dataURI:     "case006/",
			view:        "foos",
			dest:        new([]*Foo),
			expect:      `[{"Id":1,"Name":"FOO"},{"Id":2,"Name":"ANOTHER FOO"},{"Id":3,"Name":"YET ANOTHER FOO"}]`,
			compTypes: map[string]reflect.Type{
				"foo": reflect.TypeOf(&Foo{}),
			},
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
			dataURI:     "case009/",
			view:        "users_accounts",
			description: "many to one",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Name":"John","Accounts":[{"Id":1,"Name":"John account","UserId":1},{"Id":3,"Name":"Another John account","UserId":1}]},{"Id":2,"Name":"David","Accounts":[{"Id":2,"Name":"Anna account","UserId":2}]},{"Id":3,"Name":"Anna","Accounts":null}]`,
		},
		{
			description: "one to one, include join column true",
			dataURI:     "case010/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventType":null,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventType":{"Id":11,"Name":"type 2","AccountId":33},"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventType":{"Id":111,"Name":"type 3","AccountId":36},"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			dataURI:     "case011/",
			view:        "users_accounts",
			description: "parameters",
			dest:        new(interface{}),
			expect:      `[{"Id":4,"Name":"Kamil","Role":"ADMIN"},{"Id":5,"Name":"Bob","Role":"ADMIN"}]`,
			subject:     "Kamil",
		},
		{
			description: "read all strategy, one to one",
			dataURI:     "case012/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventType":null,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventType":{"Id":11,"Name":"type 2","AccountId":33},"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventType":{"Id":111,"Name":"type 3","AccountId":36},"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description: "read all strategy, many to one",
			dataURI:     "case013/",
			view:        "users_accounts",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Name":"John","Accounts":[{"Id":1,"Name":"John account","UserId":1},{"Id":3,"Name":"Another John account","UserId":1}]},{"Id":2,"Name":"David","Accounts":[{"Id":2,"Name":"Anna account","UserId":2}]},{"Id":3,"Name":"Anna","Accounts":null}]`,
		},
		{
			description: "read all strategy, batch size",
			dataURI:     "case014/",
			view:        "articles_languages",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Content":"Lorem ipsum","Language":{"Id":2,"Code":"en-US"}},{"Id":2,"Content":"dolor sit amet","Language":{"Id":12,"Code":"ky-KG"}},{"Id":3,"Content":"consectetur adipiscing elit","Language":{"Id":13,"Code":"lb-LU"}},{"Id":4,"Content":"sed do eiusmod tempor incididunt","Language":{"Id":9,"Code":"zh-CN"}}]`,
		},
		{
			description: "T type one to one relation",
			dataURI:     "case015/",
			view:        "articles_languages",
			dest:        new([]Article),
			expect:      `[{"Id":1,"Content":"Lorem ipsum","LangId":2,"Language":{"Id":2,"Code":"en-US"}},{"Id":2,"Content":"dolor sit amet","LangId":12,"Language":{"Id":12,"Code":"ky-KG"}},{"Id":3,"Content":"consectetur adipiscing elit","LangId":13,"Language":{"Id":13,"Code":"lb-LU"}},{"Id":4,"Content":"sed do eiusmod tempor incididunt","LangId":9,"Language":{"Id":9,"Code":"zh-CN"}},{"Id":5,"Content":"content without lang","LangId":0,"Language":{"Id":0,"Code":""}}]`,
			compTypes: map[string]reflect.Type{
				"article": reflect.TypeOf(Article{}),
			},
		},
		{
			description: "T type one to one relation",
			dataURI:     "case015/",
			view:        "articles_languages",
			dest:        new([]Article),
			expect:      `[{"Id":1,"Content":"Lorem ipsum","LangId":2,"Language":{"Id":2,"Code":"en-US"}},{"Id":2,"Content":"dolor sit amet","LangId":12,"Language":{"Id":12,"Code":"ky-KG"}},{"Id":3,"Content":"consectetur adipiscing elit","LangId":13,"Language":{"Id":13,"Code":"lb-LU"}},{"Id":4,"Content":"sed do eiusmod tempor incididunt","LangId":9,"Language":{"Id":9,"Code":"zh-CN"}},{"Id":5,"Content":"content without lang","LangId":0,"Language":{"Id":0,"Code":""}}]`,
			compTypes: map[string]reflect.Type{
				"article": reflect.TypeOf(Article{}),
			},
		},
		{
			description: "path parameter",
			dataURI:     "case016/",
			view:        "users",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Name":"John","Role":""}]`,
			request: &http.Request{
				URL: &url.URL{
					Path: "/users/1",
				},
			},
			path: "/users/{userId}",
		},
		{
			description: "query parameter",
			dataURI:     "case017/",
			view:        "languages",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Code":"en-GB"},{"Id":2,"Code":"en-US"}]`,
			request: &http.Request{
				RequestURI: "/languages",
				URL: &url.URL{
					RawQuery: "lang=en",
					Path:     "/languages",
				},
			},
			path: "/languages",
		},
		{
			description: "header parameter",
			dataURI:     "case018/",
			view:        "users",
			dest:        new(interface{}),
			expect:      `[{"Id":3,"Name":"Anna","Role":""}]`,
			request: &http.Request{
				Header: map[string][]string{
					"user-name": {"Anna"},
				},
				URL: &url.URL{},
			},
		},
		{
			description: "cookie parameter",
			dataURI:     "case019/",
			view:        "users",
			dest:        new(interface{}),
			expect:      `[{"Id":2,"Name":"David","Role":""}]`,
			request: &http.Request{
				Header: map[string][]string{
					"Cookie": {"user-id=2"},
				},
				URL: &url.URL{},
			},
		},
		{
			description: "cookie parameter union sql injection",
			dataURI:     "case019/",
			view:        "users",
			dest:        new(interface{}),
			request: &http.Request{
				Header: map[string][]string{
					"Cookie": {"user-id=2 UNION Select 1 as Id, 'abc' as Name, 'ADMIN' as Role"},
				},
				URL: &url.URL{},
			},
			expectError: true,
		},
		{
			description: "cookie parameter drop table sql injection",
			dataURI:     "case019/",
			view:        "users",
			dest:        new(interface{}),
			expect:      `[{"Id":2,"Name":"David","Role":""}]`,
			request: &http.Request{
				Header: map[string][]string{
					"Cookie": {`user-id=2%3BDROP TABLE USERS`},
				},
				URL: &url.URL{},
			},
			expectError: true,
		},
		{
			description: "not used cookie",
			dataURI:     "case019/",
			view:        "users",
			dest:        new(interface{}),
			expect:      `[{"Id":2,"Name":"David","Role":""}]`,
			request: &http.Request{
				Header: map[string][]string{
					"Cookie": {`user-id=2;other-cookie="--#DROP"`},
				},
				URL: &url.URL{},
			},
		},
		{
			description: "header parameter union sql injection",
			dataURI:     "case018/",
			view:        "users",
			dest:        new(interface{}),
			request: &http.Request{
				Header: map[string][]string{
					"user-name": {"'Anna' UNION SELECT 1 as Id, 'Abc' as Name, 'ADMIN' as Role; --"},
				},
				URL: &url.URL{},
			},
			expectError: true,
		},
		{
			description: "not used header",
			dataURI:     "case018/",
			view:        "users",
			dest:        new(interface{}),
			request: &http.Request{
				Header: map[string][]string{
					"user-name":    {"Anna"},
					"other-header": {"#--DROP"},
				},
				URL: &url.URL{},
			},
			expect: `[{"Id":3,"Name":"Anna","Role":""}]`,
		},
		{
			description: "query sql injection parameter",
			dataURI:     "case017/",
			view:        "languages",
			dest:        new(interface{}),
			request: &http.Request{
				RequestURI: "/languages",
				URL: &url.URL{
					RawQuery: "lang=en'%20UNION%20SELECT%201%20as%20Id%2C%20'abc'%20as%20Code%3B--'",
					Path:     "/languages",
				},
			},
			path:        "/languages",
			expectError: true,
		},
		{
			description: "query parameter",
			dataURI:     "case017/",
			view:        "languages",
			dest:        new(interface{}),
			request: &http.Request{
				RequestURI: "/languages",
				URL: &url.URL{
					RawQuery: "lang=en&otherParam=%20UNION%20SELECT%201%20as%20Id%2C%20'abc'%20as%20Code%3B--'",
					Path:     "/languages",
				},
			},
			expect: `[{"Id":1,"Code":"en-GB"},{"Id":2,"Code":"en-US"}]`,
			path:   "/languages",
		},
		{
			description: "path parameter sql injection",
			dataURI:     "case016/",
			view:        "users",
			dest:        new(interface{}),
			request: &http.Request{
				URL: &url.URL{
					Path: "/users/1 UNION SELECT 10 as Id, 'Abc' as Name, 'ADMIN' as Role",
				},
			},
			expectError: true,
			path:        "/users/{userId}",
		},
		{
			description: "selector sql injection",
			dataURI:     "case002/",
			dest:        new(interface{}),
			view:        "events",
			selectors: map[string]*data.Selector{
				"events": {
					Limit: 1,
					Criteria: &data.Criteria{
						Expression: "1=1;--",
					},
				},
			},
			expectError: true,
		},
		{
			description: "disabled client criteria",
			dataURI:     "case003/",
			dest:        new(interface{}),
			view:        "events",
			expect:      `[{"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"UserId":1},{"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2},{"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3}]`,
			selectors: map[string]*data.Selector{
				"events": {
					Columns:  []string{"quantity"},
					OrderBy:  "user_id",
					Offset:   2,
					Limit:    1,
					Criteria: &data.Criteria{Expression: "quantity > 30"},
				},
			},
		},
		{
			description: "one to one, without relation, by field name",
			dataURI:     "case008/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","UserId":3}]`,
			selectors: map[string]*data.Selector{
				"event_event-types": {
					Columns: []string{"Id", "Timestamp", "UserId"},
				},
			},
		},
		{
			description: "one to one, include column, by field name",
			dataURI:     "case008/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventType":null},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventType":{"Id":11,"Name":"type 2","AccountId":33}},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventType":{"Id":111,"Name":"type 3","AccountId":36}}]`,
			selectors: map[string]*data.Selector{
				"event_event-types": {
					Columns: []string{"Id", "Timestamp", "EventType"},
				},
			},
		},
		{
			description: "query parameter, required",
			dataURI:     "case020/",
			view:        "events",
			dest:        new(interface{}),
			expectError: true,
		},
		{
			description: "path parameter, required",
			dataURI:     "case021/",
			view:        "events",
			dest:        new(interface{}),
			expectError: true,
		},
		{
			description: "cookie parameter, required",
			dataURI:     "case022/",
			view:        "events",
			dest:        new(interface{}),
			expectError: true,
		},
		{
			description: "header parameter, required",
			dataURI:     "case023/",
			view:        "events",
			dest:        new(interface{}),
			expectError: true,
		},
	}

	//for _, testCase := range useCases[len(useCases)-1:] {
	for _, testCase := range useCases {
		if initDb(t, path.Join(testLocation, "testdata", "mydb_config.yaml"), path.Join(testLocation, fmt.Sprintf("testdata/case/populate_mydb")), "db") {
			return
		}

		if initDb(t, path.Join(testLocation, "testdata", "other_config.yaml"), path.Join(testLocation, fmt.Sprintf("testdata/case/populate_other")), "other") {
			return
		}

		types := data.Types{}

		for key, rType := range testCase.compTypes {
			types.Register(key, rType)
		}

		resource, err := data.NewResourceFromURL(context.TODO(), path.Join(testLocation, fmt.Sprintf("testdata/case/"+testCase.dataURI+"/resources.yaml")), types)
		if err != nil {
			t.Fatalf(err.Error())
		}

		service := New(resource)
		service.Apply(testCase.options)

		dataView, err := resource.View(testCase.view)

		if err != nil {
			t.Fatal(err)
		}

		session := &data.Session{
			Dest:        testCase.dest,
			View:        dataView,
			Selectors:   testCase.selectors,
			Subject:     testCase.subject,
			HttpRequest: testCase.request,
			MatchedPath: testCase.path,
		}

		err = service.Read(context.TODO(), session)

		if testCase.expectError {
			assert.NotNil(t, err, testCase.description)
			continue
		}

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
	request := dsunit.NewPrepareRequest(initDataset)
	if !dsunit.Prepare(t, request) {
		return true
	}

	return false
}
