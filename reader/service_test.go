package reader_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/config"
	"github.com/viant/datly/internal/tests"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/gmetric/counter/base"
	"github.com/viant/toolbox/format"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
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
	DealsSize     int
}

func (a *audience) OnRelation(ctx context.Context) {
	a.DealsSize = len(a.Deals)
}

type Deal struct {
	Id     int
	Name   string
	DealId string
}

func (a *audience) OnFetch(_ context.Context) error {
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

type usecase struct {
	selectors   map[string]*view.Selector
	description string
	dataURI     string
	expect      string
	dest        interface{}
	view        string
	compTypes   map[string]reflect.Type
	expectError bool
	resource    *view.Resource
	dataset     string
	provider    *base.Provider
	visitors    config.CodecsRegistry
}

type StringsCodec struct {
}

func (s *StringsCodec) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	rawString, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("expected to got string but got %T", raw)
	}

	return strings.Split(rawString, ","), nil
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

	type UserViewParams struct {
		AclCriteria string `velty:"DATLY_ACL"`
	}

	type UserIdParam struct {
		Id int `velty:"UserId"`
	}

	type LangQueryParams struct {
		Language string `velty:"Language"`
	}

	type UserHeaderParams struct {
		UserName string `velty:"User_name"`
	}

	type AclParams struct {
		Subject string
	}

	//testLocation := toolbox.CallerDirectory(3)
	testLocation := "./"

	var useCases = []usecase{
		{
			description: "read all view with specified columns",
			dataURI:     "case001_schema/",
			dest:        new([]*Event),
			view:        "events",
			expect:      `[{"ID":1,"EventTypeID":2,"Quantity":33.23432374000549,"Timestamp":"0001-01-01T00:00:00Z"},{"ID":10,"EventTypeID":11,"Quantity":21.957962334156036,"Timestamp":"0001-01-01T00:00:00Z"},{"ID":100,"EventTypeID":111,"Quantity":5.084940046072006,"Timestamp":"0001-01-01T00:00:00Z"}]`,
			compTypes: map[string]reflect.Type{
				"events": reflect.TypeOf(&Event{}),
			},
		},
		{
			description: "read all view with specified columns",
			dataURI:     "case002_from/",
			dest:        new(interface{}),
			view:        "events",
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description: "criteria for non existing column",
			dataURI:     "case002_from/",
			dest:        new(interface{}),
			view:        "events",
			selectors: map[string]*view.Selector{
				"events": {
					Criteria: "foo_column = 'abc'",
				},
			},
			expectError: true,
		},
		{
			description: "criteria for column, by field name",
			dataURI:     "case002_from/",
			dest:        new(interface{}),
			view:        "events",
			selectors: map[string]*view.Selector{
				"events": {
					Criteria: "event_type_id = 11",
				},
			},
			expect: `[{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2}]`,
		},
		{
			description: "excluded columns",
			dataURI:     "case003_exclude/",
			dest:        new(interface{}),
			view:        "events",
			expect:      `[{"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"UserId":1},{"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2},{"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description: "events selector",
			dataURI:     "case004_selector/",
			view:        "events",
			dest:        new(interface{}),
			expect:      `[{"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2},{"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3}]`,
			selectors: map[string]*view.Selector{
				"events": {
					Offset: 1,
				},
			},
		},
		{
			description: "columns expression",
			dataURI:     "case005_column_expression/",
			view:        "foos",
			dest:        new([]*Foo),
			expect:      `[{"Id":1,"Name":"FOO"},{"Id":2,"Name":"ANOTHER FOO"},{"Id":3,"Name":"YET ANOTHER FOO"}]`,
			compTypes: map[string]reflect.Type{
				"foo": reflect.TypeOf(&Foo{}),
			},
		},
		{
			description: "custom selector",
			dataURI:     "case006_client_selector/",
			view:        "events",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"","Quantity":33.23432374000549},{"Id":10,"Timestamp":"","Quantity":21.957962334156036},{"Id":100,"Timestamp":"","Quantity":5.084940046072006}]`,
			selectors: map[string]*view.Selector{
				"events": {
					Columns: []string{"id", "quantity"},
					OrderBy: "id",
				},
			},
		},
		{
			description: "one to one, include false",
			dataURI:     "case007_one_to_one/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"UserId":1,"EventType":{"Id":2,"Name":"type 6","AccountId":37}},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2,"EventType":{"Id":11,"Name":"type 2","AccountId":33}},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3,"EventType":{"Id":111,"Name":"type 3","AccountId":36}}]`,
		},
		{
			description: "one to one, include column, by field name",
			dataURI:     "case007_one_to_one/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventType":{"Id":2,"Name":"type 6","AccountId":37}},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventType":{"Id":11,"Name":"type 2","AccountId":33}},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventType":{"Id":111,"Name":"type 3","AccountId":36}}]`,
			selectors: map[string]*view.Selector{
				"event_event-types": {
					Columns: []string{"Id", "Timestamp", "EventType"},
				},
			},
		},
		{
			description: "one to one, without relation, by field name",
			dataURI:     "case007_one_to_one/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","UserId":1,"EventType":null},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","UserId":2,"EventType":null},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","UserId":3,"EventType":null}]`,
			selectors: map[string]*view.Selector{
				"event_event-types": {
					Columns: []string{"Id", "Timestamp", "UserId"},
				},
			},
		},
		{
			dataURI:     "case008_many_to_one/",
			view:        "users_accounts",
			description: "many to one",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Name":"John","Accounts":[{"Id":1,"Name":"John account","UserId":1},{"Id":3,"Name":"Another John account","UserId":1}]},{"Id":2,"Name":"David","Accounts":[{"Id":2,"Name":"Anna account","UserId":2}]},{"Id":3,"Name":"Anna","Accounts":null},{"Id":4,"Name":"Kamil","Accounts":null},{"Id":5,"Name":"Bob","Accounts":null}]`,
		},
		{
			description: "one to one, include column true",
			dataURI:     "case009_without_column/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1,"EventType":{"Id":2,"Name":"type 6","AccountId":37}},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2,"EventType":{"Id":11,"Name":"type 2","AccountId":33}},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3,"EventType":{"Id":111,"Name":"type 3","AccountId":36}}]`,
		},
		{
			dataURI:     "case010_view_parameter/",
			view:        "users_accounts",
			description: "parameters",
			dest:        new(interface{}),
			compTypes: map[string]reflect.Type{
				"user_params":      reflect.TypeOf(UserViewParams{}),
				"datly_acl_params": reflect.TypeOf(AclParams{}),
			},
			expect: `[{"Id":4,"Name":"Kamil","Role":"ADMIN","Accounts":null},{"Id":5,"Name":"Bob","Role":"ADMIN","Accounts":null}]`,
			selectors: map[string]*view.Selector{
				"users_accounts": {
					Parameters: view.ParamState{
						Values: UserViewParams{AclCriteria: "ROLE IN ('ADMIN')"},
					},
				},
				"datly_acl": {
					Parameters: view.ParamState{
						Values: AclParams{Subject: "Kamil"},
					},
				},
			},
		},
		{
			description: "read all strategy, one to one",
			dataURI:     "case011_read_all_one_to_one/",
			view:        "event_event-types",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"UserId":1,"EventType":{"Id":2,"Name":"type 6","AccountId":37}},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2,"EventType":{"Id":11,"Name":"type 2","AccountId":33}},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3,"EventType":{"Id":111,"Name":"type 3","AccountId":36}}]`,
		},
		{
			description: "read all strategy, many to one",
			dataURI:     "case012_many_to_one/",
			view:        "users_accounts",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Name":"John","Role":"","Accounts":[{"Id":1,"Name":"John account","UserId":1},{"Id":3,"Name":"Another John account","UserId":1}]},{"Id":2,"Name":"David","Role":"","Accounts":[{"Id":2,"Name":"Anna account","UserId":2}]},{"Id":3,"Name":"Anna","Role":"","Accounts":null},{"Id":4,"Name":"Kamil","Role":"ADMIN","Accounts":null},{"Id":5,"Name":"Bob","Role":"ADMIN","Accounts":null}]`,
		},
		{
			description: "read all strategy, batch size",
			dataURI:     "case013_read_all_batch_size/",
			view:        "articles_languages",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Content":"Lorem ipsum","Language":{"Id":2,"Code":"en-US"}},{"Id":2,"Content":"dolor sit amet","Language":{"Id":12,"Code":"ky-KG"}},{"Id":3,"Content":"consectetur adipiscing elit","Language":{"Id":13,"Code":"lb-LU"}},{"Id":4,"Content":"sed do eiusmod tempor incididunt","Language":{"Id":9,"Code":"zh-CN"}},{"Id":5,"Content":"content without lang","Language":null}]`,
		},
		{
			description: "T type one to one relation",
			dataURI:     "case014_T_one_to_one/",
			view:        "articles_languages",
			dest:        new([]Article),
			expect:      `[{"Id":1,"Content":"Lorem ipsum","LangId":2,"Language":{"Id":2,"Code":"en-US"}},{"Id":2,"Content":"dolor sit amet","LangId":12,"Language":{"Id":12,"Code":"ky-KG"}},{"Id":3,"Content":"consectetur adipiscing elit","LangId":13,"Language":{"Id":13,"Code":"lb-LU"}},{"Id":4,"Content":"sed do eiusmod tempor incididunt","LangId":9,"Language":{"Id":9,"Code":"zh-CN"}},{"Id":5,"Content":"content without lang","LangId":0,"Language":{"Id":0,"Code":""}}]`,
			compTypes: map[string]reflect.Type{
				"article": reflect.TypeOf(Article{}),
			},
		},
		{
			description: "path parameter",
			dataURI:     "case015_path_parameter/",
			view:        "users",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Name":"John","Role":""}]`,
			compTypes: map[string]reflect.Type{
				"user_params": reflect.TypeOf(UserIdParam{}),
			},
			selectors: map[string]*view.Selector{
				"users": {
					Parameters: view.ParamState{
						Values: UserIdParam{Id: 1},
					},
				},
			},
		},
		{
			description: "query parameter",
			dataURI:     "case016_query_parameter/",
			view:        "languages",
			dest:        new(interface{}),
			expect:      `[{"Id":1,"Code":"en-GB"},{"Id":2,"Code":"en-US"}]`,
			compTypes: map[string]reflect.Type{
				"lang_params": reflect.TypeOf(LangQueryParams{}),
			},
			selectors: map[string]*view.Selector{
				"languages": {
					Parameters: view.ParamState{
						Values: LangQueryParams{Language: "en"},
					},
				},
			},
		},
		{
			description: "header parameter",
			dataURI:     "case017_header_parameter/",
			view:        "users",
			dest:        new(interface{}),
			expect:      `[{"Id":3,"Name":"Anna","Role":""}]`,
			compTypes: map[string]reflect.Type{
				"header_params": reflect.TypeOf(UserHeaderParams{}),
			},
			selectors: map[string]*view.Selector{
				"users": {
					Parameters: view.ParamState{
						Values: UserHeaderParams{UserName: "Anna"},
					},
				},
			},
		},
		{
			description: "cookie parameter",
			dataURI:     "case018_cookie_parameter/",
			view:        "users",
			dest:        new(interface{}),
			expect:      `[{"Id":2,"Name":"David","Role":""}]`,
			compTypes: map[string]reflect.Type{
				"user_params": reflect.TypeOf(UserIdParam{}),
			},
			selectors: map[string]*view.Selector{
				"users": {
					Parameters: view.ParamState{
						Values: UserIdParam{Id: 2},
						Has:    nil,
					},
				},
			},
		},
		{
			description: "derive columns from schema type",
			dataURI:     "case019_derive_columns/",
			view:        "datly_acl",
			dest:        new([]AclRecord),
			compTypes: map[string]reflect.Type{
				"datly_acl": reflect.TypeOf(AclRecord{}),
			},
			expect: `[{"DatlyCriteria":"ROLE IN ('ADMIN')","Subject":"Kamil"}]`,
		},
		{
			description: "derive columns from schema type with relation",
			dataURI:     "case020_derive_columns_relation/",
			view:        "event_event-types",
			dest:        new([]Boo),
			compTypes: map[string]reflect.Type{
				"event_event-type": reflect.TypeOf(Boo{}),
			},
			expect: `[{"ID":1,"Quantity":33.23432374000549,"EventType":{"Id":2,"Name":"type 6"},"Timestamp":"2019-03-11T02:20:33Z"},{"ID":10,"Quantity":21.957962334156036,"EventType":{"Id":11,"Name":"type 2"},"Timestamp":"2019-03-15T12:07:33Z"},{"ID":100,"Quantity":5.084940046072006,"EventType":{"Id":111,"Name":"type 3"},"Timestamp":"2019-04-10T05:15:33Z"}]`,
		},
		{
			description: "derive columns from schema type with relation",
			dataURI:     "case021_on_fetch/",
			view:        "audiences_deals",
			dest:        new([]audience),
			compTypes: map[string]reflect.Type{
				"audience": reflect.TypeOf(audience{}),
			},
			expect: `[{"Id":1,"Info":"1,2","Info2":"","DealsId":[1,2],"Deals":[{"Id":1,"Name":"deal 1","DealId":""},{"Id":2,"Name":"deal 2","DealId":""}],"StringDealsId":null,"DealsSize":2},{"Id":2,"Info":"","Info2":"20,30","DealsId":null,"Deals":[{"Id":5,"Name":"deal 5","DealId":"20"},{"Id":6,"Name":"deal 6","DealId":"30"}],"StringDealsId":["20","30"],"DealsSize":2}]`,
		},
		eventTypeViewWithEventTypeIdColumn(),
		eventTypeViewWithoutEventTypeIdColumn(),
		inheritColumnsView(),
		sqlxColumnNames(),
		nestedRelation(),
		inheritTypeForReferencedView(),
		columnsInSource(),
		detectColumnAlias(),
		inheritConnector(),
		criteriaWhere(),
		inheritCoalesceTypes(),
		inheritLogger(),
		wildcardAllowedFilterable(),
		autoCoalesce(),
		batchParent(),
		{
			description: "type definition",
			dataURI:     "case022_types/",
			dest:        new(interface{}),
			view:        "events",
			expect:      `[{"Id":1,"EventTypeId":2,"Quantity":33.23432374000549,"Date":"2019-03-11T02:20:33Z"},{"Id":10,"EventTypeId":11,"Quantity":21.957962334156036,"Date":"2019-03-15T12:07:33Z"},{"Id":100,"EventTypeId":111,"Quantity":5.084940046072006,"Date":"2019-04-10T05:15:33Z"}]`,
		},
		{
			description: "type definition",
			dataURI:     "case023_columns_codec/",
			dest:        new(interface{}),
			view:        "events",
			visitors: config.CodecsRegistry{
				"Strings": config.NewVisitor("Strings", &StringsCodec{}),
			},
			expect: `[{"Name":["John","David","Anna"]}]`,
		},
		nestedStruct(),
	}

	//for index, testCase := range useCases[len(useCases)-1:] {
	for index, testCase := range useCases {
		tests.LogHeader(fmt.Sprintf("Running testcase nr: %v\n", index))

		resourcePath := path.Join(testLocation, "testdata", "cases", testCase.dataURI)
		if testCase.dataset != "" {
			resourcePath = path.Join(testLocation, "testdata", "datasets", testCase.dataset)
		}

		if !tests.InitDB(t, path.Join(testLocation, "testdata", "db_config.yaml"), path.Join(resourcePath, "populate_db"), "db") {
			continue
		}

		if !tests.InitDB(t, path.Join(testLocation, "testdata", "other_config.yaml"), path.Join(resourcePath, "populate_other"), "other") {
			continue
		}

		types := view.Types{}

		for key, rType := range testCase.compTypes {
			types.Register(key, rType)
		}

		var resource *view.Resource
		var err error
		if testCase.dataURI != "" {
			resource, err = view.NewResourceFromURL(context.TODO(), path.Join(testLocation, fmt.Sprintf("testdata/cases/"+testCase.dataURI+"/resources.yaml")), types, testCase.visitors)
			if err != nil {
				t.Fatalf(err.Error())
			}
		} else {
			resource = testCase.resource
			if err = resource.Init(context.TODO(), types, testCase.visitors, map[string]view.Columns{}); err != nil {
				t.Fatalf(err.Error())
			}
		}

		service := reader.New()

		dataView, err := resource.View(testCase.view)
		if err != nil {
			t.Fatal(err)
		}

		testView(t, testCase, dataView, err, service)
	}
}

func nestedStruct() usecase {
	type ev struct {
		ID          int
		EventTypeID int
		Quantity    float64
		ExtraField  string `sqlx:"FIELD" json:",omitempty"`
		Recent      struct {
			ID          int
			Quantity    float64
			EventTypeID int `sqlx:"EVENT_TYPE_ID"`
		} `sqlx:"ns=RECENT_"`
	}

	resource := view.EmptyResource()
	resource.AddViews(&view.View{
		Name:                 "events",
		Schema:               view.NewSchema(reflect.TypeOf(ev{})),
		InheritSchemaColumns: true,
		Connector: &view.Connector{
			Name:   "db",
			Driver: "sqlite3",
			DSN:    "./testdata/db/db.db",
		},
		Template: &view.Template{
			Source: `
SELECT 
id, id as RECENT_ID,
event_type_id as RECENT_event_type_id, event_type_id,
quantity as RECENT_quantity,quantity
FROM events`,
		},
	})

	return usecase{
		description: "nested struct without relation",
		expect:      `[{"ID":1,"EventTypeID":2,"Quantity":33.23432374000549,"Recent":{"ID":1,"Quantity":33.23432374000549,"EventTypeID":2}},{"ID":10,"EventTypeID":11,"Quantity":21.957962334156036,"Recent":{"ID":10,"Quantity":21.957962334156036,"EventTypeID":11}},{"ID":100,"EventTypeID":111,"Quantity":5.084940046072006,"Recent":{"ID":100,"Quantity":5.084940046072006,"EventTypeID":111}}]`,
		dest:        new([]ev),
		view:        "events",
		resource:    resource,
		dataset:     "dataset001_events/",
		provider:    nil,
		visitors:    nil,
	}
}

func testView(t *testing.T, testCase usecase, dataView *view.View, err error, service *reader.Service) {
	selectors := view.Selectors{
		Index:   testCase.selectors,
		RWMutex: sync.RWMutex{},
	}

	session := &reader.Session{
		Dest:      testCase.dest,
		View:      dataView,
		Selectors: &selectors,
	}

	err = service.Read(context.TODO(), session)
	if testCase.expectError {
		assert.NotNil(t, err, testCase.description)
		return
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

func batchParent() usecase {
	resource, viewName := eventsResource(&view.Batch{
		Parent: 1,
	})
	return usecase{
		description: "batch parent",
		dest:        new([]*event),
		view:        viewName,
		dataset:     "dataset001_events/",
		expect:      `[{"Id":1,"Quantity":33.23432374000549,"Timestamp":"2019-03-11T02:20:33Z","TypeId":2,"EventType":{"Id":2,"Events":null,"Name":"type 6"}},{"Id":10,"Quantity":21.957962334156036,"Timestamp":"2019-03-15T12:07:33Z","TypeId":11,"EventType":{"Id":11,"Events":null,"Name":"type 2"}},{"Id":100,"Quantity":5.084940046072006,"Timestamp":"2019-04-10T05:15:33Z","TypeId":111,"EventType":{"Id":111,"Events":null,"Name":"type 3"}}]`,
		resource:    resource,
	}
}

func autoCoalesce() usecase {
	type Event struct {
		Id          int
		Quantity    float64
		EventTypeId int
	}

	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "events",
		Table:                "events",
		Schema:               view.NewSchema(reflect.TypeOf(&Event{})),
		InheritSchemaColumns: true,
		Caser:                format.CaseUpperUnderscore,
	})

	return usecase{
		view:        "events",
		dataset:     "dataset002_nils/",
		description: "inherit coalesce types | Int, Float64, Int",
		expect:      `[{"Id":1,"Quantity":0,"EventTypeId":0}]`,
		resource:    resource,
		dest:        new([]*Event),
	}
}

func wildcardAllowedFilterable() usecase {
	type Event struct {
		Id          int
		Quantity    float64
		EventTypeId int
	}

	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "events",
		Alias:                "ev",
		From:                 `SELECT COALESCE(e.id, 0) as ID, COALESCE(e.quantity, 0) as Quantity, COALESCE (e.event_type_id, 0) as EVENT_TYPE_ID FROM events as e `,
		Schema:               view.NewSchema(reflect.TypeOf(&Event{})),
		InheritSchemaColumns: true,
		Caser:                format.CaseUpperUnderscore,
		Selector: &view.Config{
			Constraints: &view.Constraints{
				Filterable: []string{"*"},
			},
		},
	})

	return usecase{
		view:        "events",
		dataset:     "dataset001_events/",
		description: "inherit coalesce types | filtered columns",
		selectors: map[string]*view.Selector{
			"events": {
				Columns: []string{"ID", "QUANTITY"},
			},
		},
		expect:   `[{"Id":1,"Quantity":33.23432374000549,"EventTypeId":0},{"Id":10,"Quantity":21.957962334156036,"EventTypeId":0},{"Id":100,"Quantity":5.084940046072006,"EventTypeId":0}]`,
		resource: resource,
		dest:     new([]*Event),
	}
}

func inheritLogger() usecase {
	type Event struct {
		Id          int
		Quantity    float64
		EventTypeId int
	}

	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddLoggers(logger.NewLogger("logger", nil))

	resource.AddViews(&view.View{
		Logger: &logger.Adapter{
			Reference: shared.Reference{
				Ref: "logger",
			},
			Name: "events_logger",
		},
		Connector:            connector,
		Name:                 "events",
		Alias:                "ev",
		From:                 `SELECT COALESCE(e.id, 0) as ID, COALESCE(e.quantity, 0) as Quantity, COALESCE (e.event_type_id, 0) as EVENT_TYPE_ID FROM events as e `,
		Schema:               view.NewSchema(reflect.TypeOf(&Event{})),
		InheritSchemaColumns: true,
		Caser:                format.CaseUpperUnderscore,
	})

	return usecase{
		view:        "events",
		dataset:     "dataset001_events/",
		description: "inherit logger",
		resource:    resource,
		expect:      `[{"Id":1,"Quantity":33.23432374000549,"EventTypeId":2},{"Id":10,"Quantity":21.957962334156036,"EventTypeId":11},{"Id":100,"Quantity":5.084940046072006,"EventTypeId":111}]`,
		dest:        new([]*Event),
	}
}

func inheritCoalesceTypes() usecase {
	type Event struct {
		Id          int
		Quantity    float64
		EventTypeId int
	}

	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "events",
		Alias:                "ev",
		From:                 `SELECT COALESCE(e.id, 0) as ID, COALESCE(e.quantity, 0) as Quantity, COALESCE (e.event_type_id, 0) as EVENT_TYPE_ID FROM events as e `,
		Schema:               view.NewSchema(reflect.TypeOf(&Event{})),
		InheritSchemaColumns: true,
		Caser:                format.CaseUpperUnderscore,
	})

	return usecase{
		view:        "events",
		dataset:     "dataset001_events/",
		description: "inherit coalesce types",
		resource:    resource,
		expect:      `[{"Id":1,"Quantity":33.23432374000549,"EventTypeId":2},{"Id":10,"Quantity":21.957962334156036,"EventTypeId":11},{"Id":100,"Quantity":5.084940046072006,"EventTypeId":111}]`,
		dest:        new([]*Event),
	}
}

func criteriaWhere() usecase {
	type Event3 struct {
		Id          int
		Quantity    float64
		EventTypeId int
	}

	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "events",
		Alias:                "ev",
		Table:                "events",
		From:                 `SELECT * FROM events as e ` + string(keywords.WhereCriteria),
		Schema:               view.NewSchema(reflect.TypeOf(&Event3{})),
		InheritSchemaColumns: true,
	})

	return usecase{
		view:        "events",
		dataset:     "dataset001_events/",
		description: "where criteria",
		resource:    resource,
		expect:      `[{"Id":1,"Quantity":33.23432374000549,"EventTypeId":2},{"Id":10,"Quantity":21.957962334156036,"EventTypeId":11},{"Id":100,"Quantity":5.084940046072006,"EventTypeId":111}]`,
		dest:        new([]*Event3),
	}
}

func inheritConnector() usecase {
	resource, viewName := eventsResource(nil)
	return usecase{
		description: "inherit connector",
		dest:        new([]*event),
		view:        viewName,
		dataset:     "dataset001_events/",
		expect:      `[{"Id":1,"Quantity":33.23432374000549,"Timestamp":"2019-03-11T02:20:33Z","TypeId":2,"EventType":{"Id":2,"Events":null,"Name":"type 6"}},{"Id":10,"Quantity":21.957962334156036,"Timestamp":"2019-03-15T12:07:33Z","TypeId":11,"EventType":{"Id":11,"Events":null,"Name":"type 2"}},{"Id":100,"Quantity":5.084940046072006,"Timestamp":"2019-04-10T05:15:33Z","TypeId":111,"EventType":{"Id":111,"Events":null,"Name":"type 3"}}]`,
		resource:    resource,
	}
}

func eventsResource(batch *view.Batch) (*view.Resource, string) {
	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Table: "event_types",
		Name:  "event-types",
	})

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "events",
		Table:                "events",
		InheritSchemaColumns: true,
		With: []*view.Relation{
			{
				Name: "event-event_types",
				Of: &view.ReferenceView{
					View: view.View{
						Name:      "event-event_types",
						Reference: shared.Reference{Ref: "event-types"},
						Batch:     batch,
					},
					Column: "id",
				},
				Cardinality: view.One,
				Column:      "event_type_id",
				Holder:      "EventType",
			},
		},
		Schema: view.NewSchema(reflect.TypeOf(&event{})),
	})
	return resource, "events"
}

func columnsInSource() usecase {
	viewName, resource := columnsInResource("event_type_id", "et")

	return usecase{
		description: "parent column values in the source position",
		dest:        new([]*event),
		view:        viewName,
		dataset:     "dataset001_events/",
		expect:      `[{"Id":1,"Quantity":33.23432374000549,"Timestamp":"2019-03-11T02:20:33Z","TypeId":2,"EventType":{"Id":0,"Events":null,"Name":""}},{"Id":10,"Quantity":21.957962334156036,"Timestamp":"2019-03-15T12:07:33Z","TypeId":11,"EventType":{"Id":11,"Events":null,"Name":"type 2"}},{"Id":100,"Quantity":5.084940046072006,"Timestamp":"2019-04-10T05:15:33Z","TypeId":111,"EventType":{"Id":0,"Events":null,"Name":""}}]`,
		resource:    resource,
	}
}

func detectColumnAlias() usecase {
	viewName, resource := columnsInResource("et.event_type_id", "")

	return usecase{
		description: "detect column alias",
		dest:        new([]*event),
		view:        viewName,
		dataset:     "dataset001_events/",
		expect:      `[{"Id":1,"Quantity":33.23432374000549,"Timestamp":"2019-03-11T02:20:33Z","TypeId":2,"EventType":{"Id":0,"Events":null,"Name":""}},{"Id":10,"Quantity":21.957962334156036,"Timestamp":"2019-03-15T12:07:33Z","TypeId":11,"EventType":{"Id":11,"Events":null,"Name":"type 2"}},{"Id":100,"Quantity":5.084940046072006,"Timestamp":"2019-04-10T05:15:33Z","TypeId":111,"EventType":{"Id":0,"Events":null,"Name":""}}]`,
		resource:    resource,
	}
}

func columnsInResource(column, alias string) (string, *view.Resource) {
	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "events",
		Table:                "events",
		InheritSchemaColumns: true,
		With: []*view.Relation{
			{
				Name: "event-event_types",
				Of: &view.ReferenceView{
					View: view.View{
						Connector: connector,
						From:      "SELECT * FROM EVENT_TYPES as et WHERE name like '%2%' " + string(keywords.AndColumnInPosition),
						Name:      "event_types",
					},
					Column: "id",
				},
				Cardinality: view.One,
				Column:      column,
				ColumnAlias: alias,
				Holder:      "EventType",
			},
		},
		Schema: view.NewSchema(reflect.TypeOf(&event{})),
	})
	return "events", resource
}

type event struct {
	Id        int
	Quantity  float64
	Timestamp time.Time
	TypeId    int `sqlx:"name=event_type_id"`
	EventType eventType
}

type eventType struct {
	Id     int
	Events []*event
	Name   string
}

func inheritTypeForReferencedView() usecase {
	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector: connector,
		Table:     "event_types",
		Name:      "event-types",
	})

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "events",
		Table:                "events",
		InheritSchemaColumns: true,
		With: []*view.Relation{
			{
				Name: "event-event_types",
				Of: &view.ReferenceView{
					View:   *view.ViewReference("event-event_types", "event-types"),
					Column: "id",
				},
				Cardinality: view.One,
				Column:      "event_type_id",
				Holder:      "EventType",
			},
		},
		Schema: view.NewSchema(reflect.TypeOf(&event{})),
	})

	return usecase{
		description: "inherit type for reference view",
		dest:        new([]*event),
		view:        "events",
		dataset:     "dataset001_events/",
		expect:      `[{"Id":1,"Quantity":33.23432374000549,"Timestamp":"2019-03-11T02:20:33Z","TypeId":2,"EventType":{"Id":2,"Events":null,"Name":"type 6"}},{"Id":10,"Quantity":21.957962334156036,"Timestamp":"2019-03-15T12:07:33Z","TypeId":11,"EventType":{"Id":11,"Events":null,"Name":"type 2"}},{"Id":100,"Quantity":5.084940046072006,"Timestamp":"2019-04-10T05:15:33Z","TypeId":111,"EventType":{"Id":111,"Events":null,"Name":"type 3"}}]`,
		resource:    resource,
	}
}

func nestedRelation() usecase {
	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "event-type_events",
		Table:                "event_types",
		InheritSchemaColumns: true,
		Schema:               view.NewSchema(reflect.TypeOf(&eventType{})),
		With: []*view.Relation{
			{
				Name: "event-type_rel",
				Of: &view.ReferenceView{
					View: view.View{
						Connector:            connector,
						Name:                 "events",
						Table:                "events",
						InheritSchemaColumns: true,
						With: []*view.Relation{
							{
								Name: "event-event_types",
								Of: &view.ReferenceView{
									View: view.View{
										Connector:            connector,
										Name:                 "event-event_types",
										Table:                "event_types",
										InheritSchemaColumns: true,
									},
									Column: "id",
								},
								Cardinality: view.One,
								Column:      "event_type_id",
								Holder:      "EventType",
							},
						},
					},
					Column: "event_type_id",
				},
				Cardinality: view.Many,
				Column:      "Id",
				Holder:      "Events",
			},
		},
	})

	return usecase{
		description: "event type -> events -> event type, many to one, programmatically",
		dest:        new([]*eventType),
		view:        "event-type_events",
		dataset:     "dataset001_events/",
		expect:      `[{"Id":1,"Events":null,"Name":"type 1"},{"Id":2,"Events":[{"Id":1,"Quantity":33.23432374000549,"Timestamp":"2019-03-11T02:20:33Z","TypeId":2,"EventType":{"Id":2,"Events":null,"Name":"type 6"}}],"Name":"type 6"},{"Id":4,"Events":null,"Name":"type 4"},{"Id":5,"Events":null,"Name":"type 5"},{"Id":11,"Events":[{"Id":10,"Quantity":21.957962334156036,"Timestamp":"2019-03-15T12:07:33Z","TypeId":11,"EventType":{"Id":11,"Events":null,"Name":"type 2"}}],"Name":"type 2"},{"Id":111,"Events":[{"Id":100,"Quantity":5.084940046072006,"Timestamp":"2019-04-10T05:15:33Z","TypeId":111,"EventType":{"Id":111,"Events":null,"Name":"type 3"}}],"Name":"type 3"}]`,
		resource:    resource,
	}
}

func sqlxColumnNames() usecase {
	type Event struct {
		Id             int
		EventQuantity  float64   `sqlx:"name=quantity"`
		EventTimestamp time.Time `sqlx:"name=timestamp"`
	}

	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "events",
		Table:                "events",
		Schema:               view.NewSchema(reflect.TypeOf(&Event{})),
		InheritSchemaColumns: true,
	})

	return usecase{
		view:        "events",
		dataset:     "dataset001_events/",
		description: "sqlx column names programmatically",
		resource:    resource,
		expect:      `[{"Id":1,"EventQuantity":33.23432374000549,"EventTimestamp":"2019-03-11T02:20:33Z"},{"Id":10,"EventQuantity":21.957962334156036,"EventTimestamp":"2019-03-15T12:07:33Z"},{"Id":100,"EventQuantity":5.084940046072006,"EventTimestamp":"2019-04-10T05:15:33Z"}]`,
		dest:        new([]*Event),
	}
}

func inheritColumnsView() usecase {
	type Event struct {
		Id        int
		Quantity  float64
		Timestamp time.Time
	}

	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "events",
		Table:                "events",
		Alias:                "e",
		Schema:               view.NewSchema(reflect.TypeOf(&Event{})),
		InheritSchemaColumns: true,
	})

	return usecase{
		description: "inherit columns",
		view:        "events",
		resource:    resource,
		dataset:     "dataset001_events/",
		expect:      `[{"Id":1,"Quantity":33.23432374000549,"Timestamp":"2019-03-11T02:20:33Z"},{"Id":10,"Quantity":21.957962334156036,"Timestamp":"2019-03-15T12:07:33Z"},{"Id":100,"Quantity":5.084940046072006,"Timestamp":"2019-04-10T05:15:33Z"}]`,
		dest:        new([]*Event),
	}
}

func eventTypeViewWithEventTypeIdColumn() usecase {
	type Event2 struct {
		Id        int
		Quantity  float64
		Timestamp time.Time
		TypeId    int `sqlx:"name=event_type_id"`
	}

	type EventType2 struct {
		Id     int
		Events []*Event2
		Name   string
	}

	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "event-type_events",
		Table:                "event_types",
		InheritSchemaColumns: true,
		Schema:               view.NewSchema(reflect.TypeOf(&EventType2{})),
		With: []*view.Relation{
			{
				Name: "event-type_rel",
				Of: &view.ReferenceView{
					View: view.View{
						Connector:            connector,
						Name:                 "events",
						Table:                "events",
						InheritSchemaColumns: true,
					},
					Column: "event_type_id",
				},
				Cardinality: view.Many,
				Column:      "Id",
				Holder:      "Events",
			},
		},
	})

	return usecase{
		description: "event type -> events, many to one, programmatically, with EventTypeId column",
		expect:      `[{"Id":1,"Events":null,"Name":"type 1"},{"Id":2,"Events":[{"Id":1,"Quantity":33.23432374000549,"Timestamp":"2019-03-11T02:20:33Z","TypeId":2}],"Name":"type 6"},{"Id":4,"Events":null,"Name":"type 4"},{"Id":5,"Events":null,"Name":"type 5"},{"Id":11,"Events":[{"Id":10,"Quantity":21.957962334156036,"Timestamp":"2019-03-15T12:07:33Z","TypeId":11}],"Name":"type 2"},{"Id":111,"Events":[{"Id":100,"Quantity":5.084940046072006,"Timestamp":"2019-04-10T05:15:33Z","TypeId":111}],"Name":"type 3"}]`,
		dest:        new([]*EventType2),
		view:        "event-type_events",
		dataset:     "dataset001_events/",
		resource:    resource,
	}
}

func eventTypeViewWithoutEventTypeIdColumn() usecase {
	type Event struct {
		Id        int
		Quantity  float64
		Timestamp time.Time
	}

	type EventType struct {
		Id     int
		Events []*Event
		Name   string
	}

	resource := view.EmptyResource()
	connector := &view.Connector{
		Name:   "db",
		DSN:    "./testdata/db/db.db",
		Driver: "sqlite3",
	}

	resource.AddViews(&view.View{
		Connector:            connector,
		Name:                 "event-type_events",
		Table:                "event_types",
		InheritSchemaColumns: true,
		Schema:               view.NewSchema(reflect.TypeOf(&EventType{})),
		With: []*view.Relation{
			{
				Name: "event-type_rel",
				Of: &view.ReferenceView{
					View: view.View{
						Connector:            connector,
						Name:                 "events",
						Table:                "events",
						InheritSchemaColumns: true,
					},
					Column: "event_type_id",
				},
				Cardinality: view.Many,
				Column:      "Id",
				Holder:      "Events",
			},
		},
	})

	return usecase{
		description: "event type -> events, many to one, programmatically, without EventTypeId column",
		expect:      `[{"Id":1,"Events":null,"Name":"type 1"},{"Id":2,"Events":[{"Id":1,"Quantity":33.23432374000549,"Timestamp":"2019-03-11T02:20:33Z"}],"Name":"type 6"},{"Id":4,"Events":null,"Name":"type 4"},{"Id":5,"Events":null,"Name":"type 5"},{"Id":11,"Events":[{"Id":10,"Quantity":21.957962334156036,"Timestamp":"2019-03-15T12:07:33Z"}],"Name":"type 2"},{"Id":111,"Events":[{"Id":100,"Quantity":5.084940046072006,"Timestamp":"2019-04-10T05:15:33Z"}],"Name":"type 3"}]`,
		dest:        new([]*EventType),
		view:        "event-type_events",
		dataset:     "dataset001_events/",
		resource:    resource,
	}
}
