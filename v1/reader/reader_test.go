package reader

import (
	"fmt"
	odata "github.com/viant/datly/data"
	"github.com/viant/datly/v1/config"
	"github.com/viant/datly/v1/data"
	"reflect"

	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"
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
		expect      interface{}
		destType    interface{}
	}{
		{
			description: "basic data read",
			dataURI:     "case001/",
			view: &data.View{
				Connector: "mydb",
				Name:      "events",
				Selector:  odata.Selector{},
				Component: data.NewComponent(reflect.TypeOf(Event{})),
			},
			destType: reflect.SliceOf(reflect.TypeOf(Event{})),
			expect: []*Event{
				{
					ID:          1,
					EventTypeID: 2,
					Quantity:    33.23432374000549,
				},
				{
					ID:          2,
					EventTypeID: 2,
					Quantity:    21.957962334156036,
				},
				{
					ID:          3,
					EventTypeID: 2,
					Quantity:    5.084940046072006,
				},
			},
		},
	}

	for _, useCase := range useCases {
		if !dsunit.InitFromURL(t, path.Join(testLocation, "testdata", "config.yaml")) {
			return
		}
		initDataset := dsunit.NewDatasetResource("db", path.Join(testLocation, fmt.Sprintf("test/cases%vpopulate", useCase.dataURI)), "", "")
		if !dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset)) {
			return
		}
	}
}
