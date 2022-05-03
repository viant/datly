package reader

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/config"
	"github.com/viant/datly/data"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestBuilder_Build(t *testing.T) {
	testLocation := toolbox.CallerDirectory(3)

	type Params struct {
		EventId int
	}

	type PresenceMap struct {
	}

	useCases := []struct {
		batchData    *BatchData
		view         *data.View
		relation     *data.Relation
		selector     *data.Selector
		placeholders []interface{}
		description  string
		output       string
		dataset      string
	}{
		{
			dataset:     "dataset001_events/",
			description: `select statement`,
			output:      `SELECT  t.ID,  t.Price FROM events AS t`,
			view: &data.View{
				Columns: []*data.Column{
					{
						Name:     "ID",
						DataType: "Int",
					},
					{
						Name:     "Price",
						DataType: "Float",
					},
				},
				Name:  "events",
				Table: "events",
				Template: &data.Template{
					Schema:         data.NewSchema(reflect.TypeOf(Params{})),
					PresenceSchema: data.NewSchema(reflect.TypeOf(PresenceMap{})),
				},
			},
			batchData: &BatchData{},
			selector: &data.Selector{Parameters: data.ParamState{
				Values: Params{},
				Has:    PresenceMap{},
			}},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement with offset and limit`,
			output:      `SELECT  t.ID,  t.Price FROM events AS t    LIMIT 10 OFFSET 5`,
			view: &data.View{
				Columns: []*data.Column{
					{
						Name:     "ID",
						DataType: "Int",
					},
					{
						Name:     "Price",
						DataType: "Float",
					},
				},
				Name: "events",
				Selector: &data.Config{
					Limit: 10,
				},
				Table: "events",
				Template: &data.Template{
					Schema:         data.NewSchema(reflect.TypeOf(Params{})),
					PresenceSchema: data.NewSchema(reflect.TypeOf(PresenceMap{})),
				},
			},
			batchData: &BatchData{},
			selector: &data.Selector{
				Parameters: data.ParamState{
					Values: Params{},
					Has:    PresenceMap{},
				},
				Offset: 5,
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement with $PAGINATION`,
			output:      `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS  LIMIT 10 OFFSET 5) AS t`,
			view: &data.View{
				Columns: []*data.Column{
					{
						Name:     "ID",
						DataType: "Int",
					},
					{
						Name:     "Price",
						DataType: "Float",
					},
				},
				Name: "events",
				Selector: &data.Config{
					Limit: 10,
				},
				From:  "SELECT * FROM EVENTS $PAGINATION",
				Table: "events",
				Template: &data.Template{
					Schema:         data.NewSchema(reflect.TypeOf(Params{})),
					PresenceSchema: data.NewSchema(reflect.TypeOf(PresenceMap{})),
				},
			},
			batchData: &BatchData{},
			selector: &data.Selector{
				Parameters: data.ParamState{
					Values: Params{},
					Has:    PresenceMap{},
				},
				Offset: 5,
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement with View Criteria`,
			output:      `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS ) AS t  WHERE ID = 1`,
			view: &data.View{
				Criteria: "ID = 1",
				Columns: []*data.Column{
					{
						Name:     "ID",
						DataType: "Int",
					},
					{
						Name:     "Price",
						DataType: "Float",
					},
				},
				Name:  "events",
				From:  "SELECT * FROM EVENTS $PAGINATION",
				Table: "Events",
				Template: &data.Template{
					Schema:         data.NewSchema(reflect.TypeOf(Params{})),
					PresenceSchema: data.NewSchema(reflect.TypeOf(PresenceMap{})),
				},
			},
			batchData: &BatchData{},
			selector: &data.Selector{
				Parameters: data.ParamState{
					Values: Params{},
					Has:    PresenceMap{},
				},
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement with $CRITERIA`,
			output:      `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS  WHERE ID = 1) AS t`,
			view: &data.View{
				Criteria: "ID = 1",
				Columns: []*data.Column{
					{
						Name:     "ID",
						DataType: "Int",
					},
					{
						Name:     "Price",
						DataType: "Float",
					},
				},
				Name:  "events",
				From:  "SELECT * FROM EVENTS $CRITERIA",
				Table: "Events",
				Template: &data.Template{
					Schema:         data.NewSchema(reflect.TypeOf(Params{})),
					PresenceSchema: data.NewSchema(reflect.TypeOf(PresenceMap{})),
				},
			},
			batchData: &BatchData{},
			selector: &data.Selector{
				Parameters: data.ParamState{
					Values: Params{},
					Has:    PresenceMap{},
				},
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement with parameters`,
			output:      `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS  WHERE ID = ?) AS t`,
			view: &data.View{
				Criteria: "ID = $EventId",
				Columns: []*data.Column{
					{
						Name:     "ID",
						DataType: "Int",
					},
					{
						Name:     "Price",
						DataType: "Float",
					},
				},
				Name:  "events",
				From:  "SELECT * FROM EVENTS $CRITERIA",
				Table: "Events",
				Template: &data.Template{
					Schema: data.NewSchema(reflect.TypeOf(Params{})),
					Parameters: []*data.Parameter{
						{
							Name: "EventId",
							In: &data.Location{
								Kind: data.PathKind,
								Name: "eventId",
							},
							Schema: &data.Schema{
								DataType: "int",
							},
						},
					},
				},
			},
			placeholders: []interface{}{10},
			batchData:    &BatchData{},
			selector: &data.Selector{
				Parameters: data.ParamState{
					Values: Params{EventId: 10},
					Has:    PresenceMap{},
				},
			},
		},
		{
			dataset:      "dataset001_events/",
			description:  `select statement with $COLUMN_IN`,
			output:       `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS ev WHERE ev.ID = ? AND  ev.user_id IN (?, ?, ?, ?)) AS t`,
			placeholders: []interface{}{10, 4, 5, 9, 2},
			relation:     &data.Relation{ColumnAlias: "ev"},
			view: &data.View{
				Columns: []*data.Column{
					{
						Name:     "ID",
						DataType: "Int",
					},
					{
						Name:     "Price",
						DataType: "Float",
					},
				},
				Name:  "events",
				From:  "SELECT * FROM EVENTS ev WHERE ev.ID = $EventId AND $COLUMN_IN",
				Table: "Events",
				Template: &data.Template{
					Schema: data.NewSchema(reflect.TypeOf(Params{})),
					Parameters: []*data.Parameter{
						{
							Name: "EventId",
							In: &data.Location{
								Kind: data.PathKind,
								Name: "eventId",
							},
							Schema: &data.Schema{
								DataType: "int",
							},
						},
					},
				},
			},
			batchData: &BatchData{
				ColumnName:  "user_id",
				ValuesBatch: []interface{}{4, 5, 9, 2},
			},
			selector: &data.Selector{
				Parameters: data.ParamState{
					Values: Params{EventId: 10},
					Has:    PresenceMap{},
				},
			},
		},
		{
			dataset:      "dataset001_events/",
			description:  `select statement without $COLUMN_IN`,
			output:       `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS ev WHERE ev.ID = ?) AS t  WHERE  t.user_id IN (?, ?, ?, ?)`,
			placeholders: []interface{}{10, 4, 5, 9, 2},
			relation:     &data.Relation{ColumnAlias: "ev"},
			view: &data.View{
				Columns: []*data.Column{
					{
						Name:     "ID",
						DataType: "Int",
					},
					{
						Name:     "Price",
						DataType: "Float",
					},
				},
				Name:  "events",
				From:  "SELECT * FROM EVENTS ev WHERE ev.ID = $EventId",
				Table: "Events",
				Template: &data.Template{
					Schema: data.NewSchema(reflect.TypeOf(Params{})),
					Parameters: []*data.Parameter{
						{
							Name: "EventId",
							In: &data.Location{
								Kind: data.PathKind,
								Name: "eventId",
							},
							Schema: &data.Schema{
								DataType: "int",
							},
						},
					},
				},
			},
			batchData: &BatchData{
				ColumnName:  "user_id",
				ValuesBatch: []interface{}{4, 5, 9, 2},
			},
			selector: &data.Selector{
				Parameters: data.ParamState{
					Values: Params{EventId: 10},
					Has:    PresenceMap{},
				},
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement | selectors`,
			output:      `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS) AS t  WHERE price > 10   ORDER BY Price LIMIT 100 OFFSET 10`,
			view: &data.View{
				Columns: []*data.Column{
					{
						Name:     "ID",
						DataType: "Int",
					},
					{
						Name:     "Price",
						DataType: "Float",
					},
				},
				Name:  "events",
				From:  "SELECT * FROM EVENTS",
				Table: "Events",
				Template: &data.Template{
					Schema: data.NewSchema(reflect.TypeOf(Params{})),
					Parameters: []*data.Parameter{
						{
							Name: "EventId",
							In: &data.Location{
								Kind: data.PathKind,
								Name: "eventId",
							},
							Schema: &data.Schema{
								DataType: "int",
							},
						},
					},
				},
			},
			selector: &data.Selector{
				OrderBy:  "price",
				Criteria: "price > 10",
				Limit:    100,
				Offset:   10,
				Parameters: data.ParamState{
					Values: Params{},
					Has:    PresenceMap{},
				},
			},
		},
	}

	//for index, useCase := range useCases[len(useCases)-1:] {
	for index, useCase := range useCases {
		fmt.Println("Running testcase nr: " + strconv.Itoa(index))
		resourcePath := path.Join(testLocation, "testdata", "datasets", useCase.dataset, "populate")
		if initDb(t, path.Join(testLocation, "testdata", "db_config.yaml"), resourcePath, "db") {
			return
		}

		useCase.view.Connector = &config.Connector{
			Name:   "db",
			DSN:    "./testdata/db/db.db",
			Driver: "sqlite3",
		}

		if !assert.Nil(t, useCase.view.Init(context.TODO(), data.EmptyResource()), useCase.description) {
			continue
		}

		builder := NewBuilder()

		useCase.selector.Init()
		sql, placeholders, err := builder.Build(useCase.view, useCase.selector, useCase.batchData, useCase.relation)

		assert.Nil(t, err, useCase.description)
		assertly.AssertValues(t, useCase.placeholders, placeholders, useCase.description)
		assert.Equal(t, useCase.output, strings.TrimSpace(sql), useCase.description)
	}
}

func initDb(t *testing.T, configPath, datasetPath, dataStore string) bool {
	datasetPath = datasetPath + "_" + dataStore
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
