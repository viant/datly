package reader

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/internal/tests"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"reflect"
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
		batchData    *view.BatchData
		view         *view.View
		relation     *view.Relation
		selector     *view.State
		placeholders []interface{}
		description  string
		output       string
		dataset      string
	}{
		{
			dataset:     "dataset001_events/",
			description: `select statement`,
			output:      `SELECT  t.ID,  t.Price FROM events AS t`,
			view: &view.View{
				Columns: []*view.Column{
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
				Template: &view.Template{
					Schema:         state.NewSchema(reflect.TypeOf(Params{})),
					PresenceSchema: state.NewSchema(reflect.TypeOf(PresenceMap{})),
				},
			},
			batchData: &view.BatchData{},
			selector: &view.State{Parameters: view.ParamState{
				Values: Params{},
				Has:    PresenceMap{},
			}},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement with offset and limit`,
			output:      `SELECT  t.ID,  t.Price FROM events AS t     LIMIT 10 OFFSET 5`,
			view: &view.View{
				Columns: []*view.Column{
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
				Selector: &view.Config{
					Limit: 10,
				},
				Table: "events",
				Template: &view.Template{
					Schema:         state.NewSchema(reflect.TypeOf(Params{})),
					PresenceSchema: state.NewSchema(reflect.TypeOf(PresenceMap{})),
				},
			},
			batchData: &view.BatchData{},
			selector: &view.State{
				Parameters: view.ParamState{
					Values: Params{},
					Has:    PresenceMap{},
				},
				Offset: 5,
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement with $PAGINATION`,
			output:      `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS  LIMIT 10 OFFSET 5  ) AS t`,
			view: &view.View{
				Columns: []*view.Column{
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
				Selector: &view.Config{
					Limit: 10,
				},
				From:  "SELECT * FROM EVENTS $PAGINATION",
				Table: "events",
				Template: &view.Template{
					Schema:         state.NewSchema(reflect.TypeOf(Params{})),
					PresenceSchema: state.NewSchema(reflect.TypeOf(PresenceMap{})),
				},
			},
			batchData: &view.BatchData{},
			selector: &view.State{
				Parameters: view.ParamState{
					Values: Params{},
					Has:    PresenceMap{},
				},
				Offset: 5,
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement with View Criteria`,
			output:      `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS   ) AS t`,
			view: &view.View{
				Columns: []*view.Column{
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
				Template: &view.Template{
					Schema:         state.NewSchema(reflect.TypeOf(Params{})),
					PresenceSchema: state.NewSchema(reflect.TypeOf(PresenceMap{})),
				},
			},
			batchData: &view.BatchData{},
			selector: &view.State{
				Parameters: view.ParamState{
					Values: Params{},
					Has:    PresenceMap{},
				},
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement with $WHERE_CRITERIA`,
			output:      `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS ) AS t`,
			view: &view.View{
				Columns: []*view.Column{
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
				From:  "SELECT * FROM EVENTS $WHERE_CRITERIA",
				Table: "Events",
				Template: &view.Template{
					Schema:         state.NewSchema(reflect.TypeOf(Params{})),
					PresenceSchema: state.NewSchema(reflect.TypeOf(PresenceMap{})),
				},
			},
			batchData: &view.BatchData{},
			selector: &view.State{
				Parameters: view.ParamState{
					Values: Params{},
					Has:    PresenceMap{},
				},
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement with parameters`,
			output:      `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS WHERE ID = ?   ) AS t`,
			view: &view.View{
				Columns: []*view.Column{
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
				From:  "SELECT * FROM EVENTS WHERE ID = $EventId",
				Table: "Events",
				Template: &view.Template{
					Schema: state.NewSchema(reflect.TypeOf(Params{})),
					Parameters: []*state.Parameter{
						{
							Name: "EventId",
							In: &state.Location{
								Kind: state.KindPath,
								Name: "eventId",
							},
							Schema: &state.Schema{
								DataType: "int",
							},
						},
					},
				},
			},
			placeholders: []interface{}{10},
			batchData:    &view.BatchData{},
			selector: &view.State{
				Parameters: view.ParamState{
					Values: Params{EventId: 10},
					Has:    PresenceMap{},
				},
			},
		},
		{
			dataset:      "dataset001_events/",
			description:  `select statement with $AND_COLUMN_IN`,
			output:       `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS ev WHERE ev.ID = ?  AND ( ev.user_id IN (?, ?, ?, ?))   ) AS t`,
			placeholders: []interface{}{10, 4, 5, 9, 2},
			relation:     &view.Relation{ColumnNamespace: "ev", Of: &view.ReferenceView{Column: "ID"}},
			view: &view.View{
				Columns: []*view.Column{
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
				From:  "SELECT * FROM EVENTS ev WHERE ev.ID = $EventId $AND_COLUMN_IN",
				Table: "Events",
				Template: &view.Template{
					Schema: state.NewSchema(reflect.TypeOf(Params{})),
					Parameters: []*state.Parameter{
						{
							Name: "EventId",
							In: &state.Location{
								Kind: state.KindPath,
								Name: "eventId",
							},
							Schema: &state.Schema{
								DataType: "int",
							},
						},
					},
				},
			},
			batchData: &view.BatchData{
				ColumnName:  "user_id",
				ValuesBatch: []interface{}{4, 5, 9, 2},
			},
			selector: &view.State{
				Parameters: view.ParamState{
					Values: Params{EventId: 10},
					Has:    PresenceMap{},
				},
			},
		},
		{
			dataset:      "dataset001_events/",
			description:  `select statement without $COLUMN_IN`,
			output:       `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS ev WHERE ev.ID = ?  AND ( ev.user_id IN (?, ?, ?, ?))  ) AS t`,
			placeholders: []interface{}{10, 4, 5, 9, 2},
			relation:     &view.Relation{ColumnNamespace: "ev", Of: &view.ReferenceView{Column: "ID"}},
			view: &view.View{
				Columns: []*view.Column{
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
				Template: &view.Template{
					Schema: state.NewSchema(reflect.TypeOf(Params{})),
					Parameters: []*state.Parameter{
						{
							Name: "EventId",
							In: &state.Location{
								Kind: state.KindPath,
								Name: "eventId",
							},
							Schema: &state.Schema{
								DataType: "int",
							},
						},
					},
				},
			},
			batchData: &view.BatchData{
				ColumnName:  "user_id",
				ValuesBatch: []interface{}{4, 5, 9, 2},
			},
			selector: &view.State{
				Parameters: view.ParamState{
					Values: Params{EventId: 10},
					Has:    PresenceMap{},
				},
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `select statement | selectors`,
			output:      `SELECT  t.ID,  t.Price FROM (SELECT * FROM EVENTS    ORDER BY Price LIMIT 100 OFFSET 10) AS t  WHERE price > 10`,
			view: &view.View{
				Columns: []*view.Column{
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
				Template: &view.Template{
					Schema: state.NewSchema(reflect.TypeOf(Params{})),
					Parameters: []*state.Parameter{
						{
							Name: "EventId",
							In: &state.Location{
								Kind: state.KindPath,
								Name: "eventId",
							},
							Schema: &state.Schema{
								DataType: "int",
							},
						},
					},
				},
			},
			selector: &view.State{
				OrderBy:  "price",
				Criteria: "price > 10",
				Limit:    100,
				Offset:   10,
				Parameters: view.ParamState{
					Values: Params{},
					Has:    PresenceMap{},
				},
			},
		},
	}

	//for index, useCase := range useCases[len(useCases)-1:] {
	for index, useCase := range useCases {
		tests.LogHeader(fmt.Sprintf("Running testcase nr: %v | %v \n", index, useCase.description))
		resourcePath := path.Join(testLocation, "testdata", "datasets", useCase.dataset, "populate")
		if initDb(t, path.Join(testLocation, "testdata", "db_config.yaml"), resourcePath, "db") {
			return
		}

		useCase.view.Connector = &view.Connector{
			Name:   "db",
			DSN:    "./testdata/db/db.db",
			Driver: "sqlite3",
		}

		if !assert.Nil(t, useCase.view.Init(context.TODO(), view.EmptyResource()), useCase.description) {
			continue
		}

		builder := NewBuilder()

		useCase.selector.Init(useCase.view)
		matcher, err := builder.Build(useCase.view, useCase.selector, useCase.batchData, useCase.relation, nil, nil, nil)

		assert.Nil(t, err, useCase.description)
		assertly.AssertValues(t, useCase.placeholders, matcher.Args, useCase.description)
		assert.Equal(t, useCase.output, strings.TrimSpace(matcher.SQL), useCase.description)
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
