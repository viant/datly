package reader

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/v1/config"
	"github.com/viant/datly/v1/data"
	"github.com/viant/datly/v1/meta"
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
		connectors     []*config.Connector
		view           *data.View
		refRelations   []*data.Relation
		clientSelector *data.Selector
		description    string
		dataURI        string
		expect         string
		dest           interface{}
		expectError    bool
		references     []*data.Reference
		relations      []*data.Relation
		options        Options
	}{
		{
			description: "read all data with specified columns",
			dataURI:     "case001/",
			view: &data.View{
				Connector: "mydb",
				Name:      "events",
				Table:     "events",
				Selector:  data.Selector{},
				Component: data.NewComponent(reflect.TypeOf(Event{})),
				Columns: []*data.Column{
					{
						Name:       "id",
						DataType:   "",
						Expression: "",
					},
					{
						Name:       "quantity",
						DataType:   "",
						Expression: "",
					},
					{
						Name:       "event_type_id",
						DataType:   "",
						Expression: "",
					},
				},
			},
			connectors: []*config.Connector{
				{
					Name:   "mydb",
					Driver: "sqlite3",
					DSN:    "./testdata/db/mydb.db",
				},
			},
			dest:   new([]*Event),
			expect: `[{"ID":1,"EventTypeID":2,"Quantity":33.23432374000549,"Timestamp":"0001-01-01T00:00:00Z"},{"ID":10,"EventTypeID":11,"Quantity":21.957962334156036,"Timestamp":"0001-01-01T00:00:00Z"},{"ID":100,"EventTypeID":111,"Quantity":5.084940046072006,"Timestamp":"0001-01-01T00:00:00Z"}]`,
		},
		{
			description: "detect type and columns",
			dataURI:     "case001/",
			view: &data.View{
				Connector: "mydb",
				Name:      "events",
				Table:     "events",
				Selector:  data.Selector{},
			},
			connectors: []*config.Connector{
				{
					Name:   "mydb",
					Driver: "sqlite3",
					DSN:    "./testdata/db/mydb.db",
				},
			},
			dest:   new(interface{}),
			expect: `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","Event_type_id":2,"Quantity":33.23432374000549,"User_id":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","Event_type_id":11,"Quantity":21.957962334156036,"User_id":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","Event_type_id":111,"Quantity":5.084940046072006,"User_id":3}]`,
		},
		{
			description: "exclude columns",
			dataURI:     "case001/",
			view: &data.View{
				Connector: "mydb",
				Name:      "events",
				Table:     "events",
				Selector: data.Selector{
					ExcludedColumns: []string{"id", "event_type_id"},
				},
			},
			connectors: []*config.Connector{
				{
					Name:   "mydb",
					Driver: "sqlite3",
					DSN:    "./testdata/db/mydb.db",
				},
			},
			dest:   new(interface{}),
			expect: `[{"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"User_id":1},{"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"User_id":2},{"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"User_id":3}]`,
		},
		{
			description: "more complex selector",
			dataURI:     "case001/",
			view: &data.View{
				Connector: "mydb",
				Name:      "events",
				Table:     "events",
				Selector: data.Selector{
					ExcludedColumns: []string{"id", "event_type_id"},
					Offset:          1,
					Limit:           2,
					OrderBy:         "timestamp",
				},
			},
			connectors: []*config.Connector{
				{
					Name:   "mydb",
					Driver: "sqlite3",
					DSN:    "./testdata/db/mydb.db",
				},
			},
			dest:   new(interface{}),
			expect: `[{"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"User_id":2},{"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"User_id":3}]`,
		},
		{
			description: "read unmapped",
			dataURI:     "case001/",
			view: &data.View{
				Connector: "mydb",
				Name:      "foos",
				Table:     "foos",
				Selector:  data.Selector{},
			},
			connectors: []*config.Connector{
				{
					Name:   "mydb",
					Driver: "sqlite3",
					DSN:    "./testdata/db/mydb.db",
				},
			},
			dest: new([]*Foo),
			options: Options{
				AllowUnmapped(true),
			},
			expect: `[{"Id":1,"Name":"foo"},{"Id":2,"Name":"another foo"},{"Id":3,"Name":"yet another foo"}]`,
		},
		{
			description: "columns expression",
			dataURI:     "case001/",
			view: &data.View{
				Connector: "mydb",
				Name:      "foos",
				Table:     "foos",
				Selector: data.Selector{
					Columns: []string{"Id", "Name"},
				},
				Columns: []*data.Column{
					{
						Name:     "Id",
						DataType: "int",
					},
					{
						Name:       "Name",
						DataType:   "string",
						Expression: "Upper(Name)",
					},
				},
			},
			connectors: []*config.Connector{
				{
					Name:   "mydb",
					Driver: "sqlite3",
					DSN:    "./testdata/db/mydb.db",
				},
			},
			dest:   new([]*Foo),
			expect: `[{"Id":1,"Name":"FOO"},{"Id":2,"Name":"ANOTHER FOO"},{"Id":3,"Name":"YET ANOTHER FOO"}]`,
		},
		{
			description: "selector columns doesn't overlap view columns",
			dataURI:     "case001/",
			view: &data.View{
				Connector: "mydb",
				Name:      "foos",
				Table:     "foos",
				Selector: data.Selector{
					Columns: []string{"Id", "Name"},
				},
				Columns: []*data.Column{
					{
						Name:       "Id",
						DataType:   "int",
						Expression: "BooId",
					},
					{
						Name:       "Name",
						DataType:   "string",
						Expression: "BooName",
					},
				},
			},
			clientSelector: &data.Selector{
				Columns: []string{"Id", "abcdef"},
			},
			options: []interface{}{AllowUnmapped(true)},
			connectors: []*config.Connector{
				{
					Name:   "mydb",
					Driver: "sqlite3",
					DSN:    "./testdata/db/mydb.db",
				},
			},
			expectError: true,
			dest:        new([]*Foo),
		},
		{
			description: "one to one",
			dataURI:     "case001/",
			view: &data.View{
				Connector: "mydb",
				Name:      "events",
				Table:     "events",
				Selector:  data.Selector{},
			},
			refRelations: []*data.Relation{
				{
					Name: "event_event-types",
					Child: &data.View{
						Connector: "mydb",
						Name:      "event_types",
						Table:     "event_types",
						Selector: data.Selector{
							OrderBy: "Id",
						},
					},
					Ref: &data.Reference{
						Cardinality: "One",
						On: &data.ColumnMatch{
							Column:    "event_type_id",
							RefColumn: "id",
							RefHolder: "event_type",
						},
					},
				},
			},
			connectors: []*config.Connector{
				{
					Name:   "mydb",
					Driver: "sqlite3",
					DSN:    "./testdata/db/mydb.db",
				},
			},
			dest:   new(interface{}),
			expect: `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","Event_type":null,"Quantity":33.23432374000549,"User_id":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","Event_type":{"Id":11,"Name":"type 2","Account_id":33},"Quantity":21.957962334156036,"User_id":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","Event_type":{"Id":111,"Name":"type 3","Account_id":36},"Quantity":5.084940046072006,"User_id":3}]`,
		},

		{
			description: "many to one",
			dataURI:     "case001/",
			view: &data.View{
				Connector: "mydb",
				Name:      "users",
				Table:     "users",
				Selector:  data.Selector{},
			},
			refRelations: []*data.Relation{
				{
					Name: "user_accounts",
					Child: &data.View{
						Connector: "mydb",
						Name:      "accounts",
						Table:     "accounts",
						Selector: data.Selector{
							OrderBy: "Id",
						},
					},
					Ref: &data.Reference{
						Cardinality: "Many",
						On: &data.ColumnMatch{
							Column:    "id",
							RefColumn: "user_id",
							RefHolder: "accounts",
						},
					},
				},
			},
			connectors: []*config.Connector{
				{
					Name:   "mydb",
					Driver: "sqlite3",
					DSN:    "./testdata/db/mydb.db",
				},
			},
			dest:   new(interface{}),
			expect: ``,
		},
	}

	for _, testCase := range useCases {
		if !dsunit.InitFromURL(t, path.Join(testLocation, "testdata", "config.yaml")) {
			return
		}
		initDataset := dsunit.NewDatasetResource("db", path.Join(testLocation, fmt.Sprintf("testdata/case/populate")), "", "")
		if !dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset)) {
			return
		}

		allViews := append(make([]*data.View, 0), testCase.view)
		for _, relation := range testCase.refRelations {
			allViews = append(allViews, relation.Child)
		}

		metaService, err := meta.Configure(testCase.connectors, allViews, testCase.relations, testCase.references)
		assert.Nil(t, err, testCase.description)
		service := New(metaService)
		service.Apply(testCase.options)

		err = service.Read(context.TODO(), &Session{
			Dest:         testCase.dest,
			View:         testCase.view,
			RefRelations: testCase.refRelations,
			Selector:     testCase.clientSelector,
		})

		if testCase.expectError {
			assert.Nil(t, reflect.ValueOf(testCase.dest).Elem().Interface(), testCase.description)
			assert.NotNil(t, err, testCase.description)
			continue
		}

		assert.Nil(t, err, testCase.description)
		stringified, _ := json.Marshal(testCase.dest)
		assert.EqualValues(t, testCase.expect, string(stringified), testCase.description)
	}
}

func TestName(t *testing.T) {

}
