package reader

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/v1/config"
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

	testLocation := toolbox.CallerDirectory(3)

	var useCases = []struct {
		connector   *config.Connector
		view        *data.View
		description string
		dataURI     string
		expect      string
		dest        interface{}
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
			connector: &config.Connector{
				Name:   "mydb",
				Driver: "sqlite3",
				DSN:    "./testdata/db/mydb.db",
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
			connector: &config.Connector{
				Name:   "mydb",
				Driver: "sqlite3",
				DSN:    "./testdata/db/mydb.db",
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
			connector: &config.Connector{
				Name:   "mydb",
				Driver: "sqlite3",
				DSN:    "./testdata/db/mydb.db",
			},
			dest:   new(interface{}),
			expect: `[{"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"User_id":1},{"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"User_id":2},{"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"User_id":3}]`,
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

		service, err := New(testCase.connector)
		assert.Nil(t, err, testCase.description)

		err = service.Read(context.TODO(), testCase.view, testCase.dest)
		assert.Nil(t, err, testCase.description)
		stringified, _ := json.Marshal(testCase.dest)
		assert.EqualValues(t, testCase.expect, string(stringified), testCase.description)
	}
}
