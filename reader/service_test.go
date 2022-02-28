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
	"strconv"
	"strings"
	"testing"
	"time"
)

type audience struct {
	Id            int
	Info          string
	Info2         string
	DealsId       []int
	Deals         []Deal
	StringDealsId []string
}

type Deal struct {
	Id     int
	Name   string
	DealId string
}

func (a *audience) OnFetch(ctx context.Context) error {
	if a.Info == "" && a.Info2 == "" {
		return nil
	}
	if a.Info != "" {
		for _, item := range strings.Split(a.Info, ",") {
			i, err := strconv.Atoi(item)
			if err != nil {
				return err
			}
			a.DealsId = append(a.DealsId, i)
		}
	}

	if a.Info2 != "" {
		for _, item := range strings.Split(a.Info2, ",") {
			a.StringDealsId = append(a.StringDealsId, item)
		}
	}
	return nil
}

func TestRead(t *testing.T) {
	type Event struct {
		ID          int
		EventTypeID int
		Quantity    float64
		Timestamp   time.Time
	}

	type EventType struct {
		Id   int
		Name string
	}

	type Boo struct {
		ID        int
		Quantity  float64
		EventType *EventType
		Timestamp time.Time
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

	type AclRecord struct {
		DatlyCriteria string `sqlx:"name=criteria"`
		Subject       string
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
			description: "criteria for non existing column",
			dataURI:     "case002/",
			dest:        new(interface{}),
			view:        "events",
			selectors: map[string]*data.Selector{
				"events": {
					Criteria: &data.Criteria{
						Expression: "foo_column = 'abc'",
					},
				},
			},
			expectError: true,
		},
		{
			description: "criteria for column, by field name",
			dataURI:     "case002/",
			dest:        new(interface{}),
			view:        "events",
			selectors: map[string]*data.Selector{
				"events": {
					Criteria: &data.Criteria{
						Expression: "EventTypeId = 11",
					},
				},
			},
			expect: `[{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2}]`,
		},
		{
			description: "excluded columns",
			dataURI:     "case003/",
			dest:        new(interface{}),
			view:        "events",
			expect:      `[{"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"UserId":1},{"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2},{"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3}]`,
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
			expect:      `[{"Id":1,"Timestamp":"","Quantity":33.23432374000549},{"Id":10,"Timestamp":"","Quantity":21.957962334156036},{"Id":100,"Timestamp":"","Quantity":5.084940046072006}]`,
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
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"UserId":1,"EventType":{"Id":2,"Name":"type 6","AccountId":37}},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2,"EventType":{"Id":11,"Name":"type 2","AccountId":33}},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3,"EventType":{"Id":111,"Name":"type 3","AccountId":36}}]`,
		},
		{
			description: "one to one, include column, by field name",
			dataURI:     "case008/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","Quantity":0,"UserId":0,"EventType":{"Id":2,"Name":"type 6","AccountId":37}},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","Quantity":0,"UserId":0,"EventType":{"Id":11,"Name":"type 2","AccountId":33}},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","Quantity":0,"UserId":0,"EventType":{"Id":111,"Name":"type 3","AccountId":36}}]`,
			selectors: map[string]*data.Selector{
				"event_event-types": {
					Columns: []string{"Id", "Timestamp", "EventType"},
				},
			},
		},
		{
			description: "one to one, without relation, by field name",
			dataURI:     "case008/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","Quantity":0,"UserId":1,"EventType":null},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","Quantity":0,"UserId":2,"EventType":null},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","Quantity":0,"UserId":3,"EventType":null}]`,
			selectors: map[string]*data.Selector{
				"event_event-types": {
					Columns: []string{"Id", "Timestamp", "UserId"},
				},
			},
		},
		{
			dataURI:     "case009/",
			view:        "users_accounts",
			description: "many to one",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Name":"John","Accounts":[{"Id":1,"Name":"John account","UserId":1},{"Id":3,"Name":"Another John account","UserId":1}]},{"Id":2,"Name":"David","Accounts":[{"Id":2,"Name":"Anna account","UserId":2}]},{"Id":3,"Name":"Anna","Accounts":null},{"Id":4,"Name":"Kamil","Accounts":null},{"Id":5,"Name":"Bob","Accounts":null}]`,
		},
		{
			description: "one to one, include column true",
			dataURI:     "case010/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1,"EventType":{"Id":2,"Name":"type 6","AccountId":37}},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2,"EventType":{"Id":11,"Name":"type 2","AccountId":33}},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3,"EventType":{"Id":111,"Name":"type 3","AccountId":36}}]`,
		},
		{
			dataURI:     "case011/",
			view:        "users_accounts",
			description: "parameters",
			dest:        new(interface{}),
			expect:      `[{"Id":4,"Name":"Kamil","Role":"ADMIN","Accounts":null},{"Id":5,"Name":"Bob","Role":"ADMIN","Accounts":null}]`,
			subject:     "Kamil",
		},
		{
			description: "read all strategy, one to one",
			dataURI:     "case012/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"UserId":1,"EventType":{"Id":2,"Name":"type 6","AccountId":37}},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2,"EventType":{"Id":11,"Name":"type 2","AccountId":33}},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3,"EventType":{"Id":111,"Name":"type 3","AccountId":36}}]`,
		},
		{
			description: "read all strategy, many to one",
			dataURI:     "case013/",
			view:        "users_accounts",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Name":"John","Role":"","Accounts":[{"Id":1,"Name":"John account","UserId":1},{"Id":3,"Name":"Another John account","UserId":1}]},{"Id":2,"Name":"David","Role":"","Accounts":[{"Id":2,"Name":"Anna account","UserId":2}]},{"Id":3,"Name":"Anna","Role":"","Accounts":null},{"Id":4,"Name":"Kamil","Role":"ADMIN","Accounts":null},{"Id":5,"Name":"Bob","Role":"ADMIN","Accounts":null}]`,
		},
		{
			description: "read all strategy, batch size",
			dataURI:     "case014/",
			view:        "articles_languages",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Content":"Lorem ipsum","Language":{"Id":2,"Code":"en-US"}},{"Id":2,"Content":"dolor sit amet","Language":{"Id":12,"Code":"ky-KG"}},{"Id":3,"Content":"consectetur adipiscing elit","Language":{"Id":13,"Code":"lb-LU"}},{"Id":4,"Content":"sed do eiusmod tempor incididunt","Language":{"Id":9,"Code":"zh-CN"}},{"Id":5,"Content":"content without lang","Language":null}]`,
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
		{
			description: "derive columns from schema type",
			dataURI:     "case024/",
			view:        "datly_acl",
			dest:        new([]AclRecord),
			compTypes: map[string]reflect.Type{
				"datly_acl": reflect.TypeOf(AclRecord{}),
			},
			expect: `[{"DatlyCriteria":"ROLE IN ('ADMIN')","Subject":"Kamil"}]`,
		},
		{
			description: "derive columns from schema type with relation",
			dataURI:     "case025/",
			view:        "event_event-types",
			dest:        new([]Boo),
			compTypes: map[string]reflect.Type{
				"event_event-type": reflect.TypeOf(Boo{}),
			},
			expect: `[{"ID":1,"Quantity":33.23432374000549,"EventType":{"Id":2,"Name":"type 6"},"Timestamp":"2019-03-11T02:20:33Z"},{"ID":10,"Quantity":21.957962334156036,"EventType":{"Id":11,"Name":"type 2"},"Timestamp":"2019-03-15T12:07:33Z"},{"ID":100,"Quantity":5.084940046072006,"EventType":{"Id":111,"Name":"type 3"},"Timestamp":"2019-04-10T05:15:33Z"}]`,
		},
		{
			description: "derive columns from schema type with relation",
			dataURI:     "case026/",
			view:        "audiences_deals",
			dest:        new([]audience),
			compTypes: map[string]reflect.Type{
				"audience": reflect.TypeOf(audience{}),
			},
			expect: `[{"Id":1,"Info":"1,2","Info2":"","DealsId":[1,2],"Deals":[{"Id":1,"Name":"deal 1","DealId":""},{"Id":2,"Name":"deal 2","DealId":""}],"StringDealsId":null},{"Id":2,"Info":"","Info2":"20,30","DealsId":null,"Deals":[{"Id":5,"Name":"deal 5","DealId":"20"},{"Id":6,"Name":"deal 6","DealId":"30"}],"StringDealsId":["20","30"]}]`,
		},
	}

	for index, testCase := range useCases {
		//for index, testCase := range useCases[:len(useCases)-1] {
		fmt.Println("Running testcase nr: " + strconv.Itoa(index))
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
		if dataView.InheritSchemaColumns {
			assert.Equalf(t, dataView.Schema.DereferencedType().NumField(), len(dataView.Columns), testCase.description)
		}

		if err != nil {
			t.Fatal(err)
		}

		session := &Session{
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
		d := testCase.dest
		b, _ := json.Marshal(d)
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
