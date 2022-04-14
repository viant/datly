package router_test

import (
	"context"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"io/ioutil"
	"net/http/httptest"

	"github.com/viant/datly/router"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"net/http"
	"path"
	"testing"
)

type testcase struct {
	description string
	resourceURI string
	uri         string
	method      string
	expected    string
}

func initDb(t *testing.T, datasetPath string, resourceURI string) bool {
	configPath := path.Join(datasetPath, "db_config.yaml")
	if !dsunit.InitFromURL(t, configPath) {
		return false
	}

	datasetURI := path.Join(datasetPath, resourceURI, "populate")
	initDataset := dsunit.NewDatasetResource("db", datasetURI, "", "")
	request := dsunit.NewPrepareRequest(initDataset)
	if !dsunit.Prepare(t, request) {
		return false
	}

	return true
}

func TestRouter(t *testing.T) {
	testLocation := toolbox.CallerDirectory(3)

	testcases := []*testcase{
		{
			description: "Regular http",
			resourceURI: "001_get",
			uri:         "/api/events",
			method:      http.MethodGet,
			expected:    `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description: "selectors",
			resourceURI: "002_selectors",
			uri:         "/api/events?fields=ev.Id|ev.Quantity&offset=ev.1&limit=ev.1",
			expected:    `[{"Id":1,"Quantity":33.23432374000549},{"Id":10,"Quantity":21.957962334156036},{"Id":100,"Quantity":5.084940046072006}]`,
			method:      http.MethodGet,
		},
	}

	for _, testcase := range testcases[1:2] {
		testUri := path.Join(testLocation, "testdata")
		routingHandler, ok := testcase.init(t, testUri)
		if !ok {
			continue
		}

		httpRequest := httptest.NewRequest(testcase.method, testcase.uri, nil)
		responseWriter := httptest.NewRecorder()
		err := routingHandler.Handle(responseWriter, httpRequest)
		if !assert.Nil(t, err, testcase.description) {
			continue
		}

		response, err := ioutil.ReadAll(responseWriter.Result().Body)
		if !assert.Nil(t, err, testcase.description) {
			continue
		}

		if !assertly.AssertValues(t, testcase.expected, string(response), testcase.description) {
			fmt.Println(string(response))
		}
	}
}

func (c *testcase) init(t *testing.T, testDataLocation string) (*router.Router, bool) {
	resourceURI := path.Join(testDataLocation, c.resourceURI)

	if !initDb(t, testDataLocation, c.resourceURI) {
		return nil, false
	}

	resource, err := router.NewResourceFromURL(context.TODO(), path.Join(resourceURI, "resource.yaml"))
	if !assert.Nil(t, err, c.description) {
		return nil, false
	}

	return router.New(resource), true
}
