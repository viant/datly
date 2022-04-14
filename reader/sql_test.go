package reader

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/config"
	"github.com/viant/datly/data"
	"github.com/viant/datly/shared"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
	"strconv"
	"strings"
	"testing"
)

func TestBuilder_Build(t *testing.T) {
	testLocation := toolbox.CallerDirectory(3)

	useCases := []struct {
		batchData   *BatchData
		view        *data.View
		relation    *data.Relation
		description string
		output      string
		dataset     string
	}{
		{
			dataset:     "dataset001_events/",
			description: `basic select statement`,
			output:      `SELECT t.ID, t.Price FROM Events AS t`,
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
				Name:                "events",
				SelectorConstraints: &data.Constraints{},
				Selector:            &data.Config{},
				Table:               "Events",
			},
			batchData: &BatchData{},
		},
		{
			dataset:     "dataset001_events/",
			description: `from`,
			output:      `SELECT f.ID, f.Price FROM (SELECT 1 as ID, 25.2 as Price) AS f`,
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
				SelectorConstraints: &data.Constraints{},
				Selector:            &data.Config{},
				Name:                "events",
				From:                "SELECT 1 as ID, 25.2 as Price",
				Alias:               "f",
			},
			batchData: &BatchData{},
		},
		{
			dataset:     "dataset001_events/",
			description: `columns in`,
			output:      `SELECT f.ID, f.Price FROM Events AS f  WHERE f.ID IN (?, ?, ?)`,
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
				SelectorConstraints: &data.Constraints{},
				Selector:            &data.Config{},
				Table:               "Events",
				Name:                "events",
				Alias:               "f",
			},
			batchData: &BatchData{
				ColumnName:  "ID",
				ValuesBatch: []interface{}{1, 2, 3},
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `columns in source`,
			output:      `SELECT f.ID, f.Price FROM (SELECT * FROM EVENTS WHERE ID IN (?, ?, ?) ) AS f`,
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
				SelectorConstraints: &data.Constraints{},
				Selector:            &data.Config{},
				From:                "SELECT * FROM EVENTS WHERE " + string(shared.ColumnInPosition),
				Name:                "events",
				Alias:               "f",
			},
			batchData: &BatchData{
				ColumnName:  "ID",
				ValuesBatch: []interface{}{1, 2, 3},
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `criteria replacement`,
			output:      `SELECT f.ID, f.Price FROM (SELECT * FROM EVENTS as ev  WHERE ev.ID IN (?, ?, ?) ) AS f`,
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
				Selector: &data.Config{},
				From:     "SELECT * FROM EVENTS as ev " + string(shared.Criteria),
				Name:     "events",
				Alias:    "f",
			},
			relation: &data.Relation{
				ColumnAlias: "ev",
			},
			batchData: &BatchData{
				ColumnName:  "ID",
				ValuesBatch: []interface{}{1, 2, 3},
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `empty criteria replacement`,
			output:      `SELECT f.ID, f.Price FROM (SELECT * FROM EVENTS as ev ) AS f`,
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
				Selector: &data.Config{},
				From:     "SELECT * FROM EVENTS as ev " + string(shared.Criteria),
				Name:     "events",
				Alias:    "f",
			},
			relation: &data.Relation{ColumnAlias: "ev"},
			batchData: &BatchData{
				ValuesBatch: []interface{}{},
			},
		},
		{
			dataset:     "dataset001_events/",
			description: `criteria replacement with where clause`,
			output:      `SELECT f.ID, f.Price FROM (SELECT * FROM EVENTS as ev WHERE 0=1  AND (ev.id IN (?, ?, ?) ) ) AS f`,
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
				Selector: &data.Config{},
				From:     "SELECT * FROM EVENTS as ev WHERE 0=1 " + string(shared.Criteria),
				Name:     "events",
				Alias:    "f",
			},
			batchData: &BatchData{
				ColumnName:  "id",
				ValuesBatch: []interface{}{1, 2, 3},
			},
			relation: &data.Relation{ColumnAlias: "ev"},
		},
		{
			dataset:     "dataset001_events/",
			description: `pagination replacement`,
			output:      `SELECT f.ID, f.Price FROM (SELECT * FROM EVENTS as ev ) AS f  WHERE f.ID IN (?, ?, ?)`,
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
				From:  "SELECT * FROM EVENTS as ev " + string(shared.Pagination),
				Name:  "events",
				Alias: "f",
			},
			relation: &data.Relation{ColumnAlias: "ev"},
			batchData: &BatchData{
				ColumnName:  "ID",
				ValuesBatch: []interface{}{1, 2, 3},
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
		sql, err := builder.Build(useCase.view, nil, useCase.batchData, useCase.relation)
		assert.Nil(t, err, useCase.description)
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
