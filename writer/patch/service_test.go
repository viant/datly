package patch

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/base/contract"
	"github.com/viant/datly/config"
	"github.com/viant/datly/data"
	"github.com/viant/datly/shared"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"

	"path"
	"testing"
)

func TestService_Path(t *testing.T) {

	testLocation := toolbox.CallerDirectory(3)
	basePath := path.Join(testLocation, "test/cases/")
	connectorURL := path.Join(basePath, "connectors")

	var useCases = []struct {
		description       string
		config            *config.Config
		expectConfigError bool
		expectPatchError  bool
		visitor           string
		visit             data.Visit
		caseDataPath      string
		request           *Request
		expect            interface{}
		prepareData       bool
		checkDatabase     bool
	}{
		{
			description:   "basic data patch - inserts",
			caseDataPath:  "/case001/",
			checkDatabase: true,
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case001/rule"),
				},
			},
			request: &Request{
				Request: contract.Request{
					TraceID: "case 001",
					Path:    "/case001/",
					Data: map[string]interface{}{
						"events": []interface{}{
							map[string]interface{}{
								"id":            1,
								"event_type_id": 2,
								"quantity":      33.23432374000549,
								"timestamp":     "2019-03-11 02:20:33",
							},
							map[string]interface{}{
								"id":            2,
								"event_type_id": 2,
								"timestamp":     "2019-03-15 12:07:33",
							},
						},
					},
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 2,
		"@assertPath@events[0].id": 1
	  }
}`,
		},

		{
			description:   "basic data patch - inserts/updates",
			caseDataPath:  "/case002/",
			checkDatabase: true,
			prepareData:   true,
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case002/rule"),
				},
			},
			request: &Request{
				Request: contract.Request{
					TraceID: "case 002",
					Path:    "/case002/",
					Data: map[string]interface{}{
						"events": []interface{}{
							map[string]interface{}{
								"id":            1,
								"event_type_id": 2,
								"quantity":      32.4,
								"timestamp":     nil,
							},
							map[string]interface{}{
								"id":        2,
								"quantity":  37.4,
								"timestamp": "2019-03-15 15:07:34",
							},
							map[string]interface{}{
								"id":            3,
								"event_type_id": 2,
								"quantity":      5.084940046072006,
								"timestamp":     "2019-04-10 05:15:33",
							},
						},
					},
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 3,
		"@assertPath@events[0].id": 1
	  }
}`,
		},
		{
			description:   " patch with URI path param",
			caseDataPath:  "/case003/",
			checkDatabase: true,
			prepareData:   true,
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case003/rule"),
				},
			},
			request: &Request{
				Request: contract.Request{
					TraceID: "case 003",
					Path:    "/case003/event_type/2/",
					Data: map[string]interface{}{
						"event_type": map[string]interface{}{
							"name":       "type X",
							"account_id": 5,
						},
					},
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@assertPath@event_type.id": 2
	  }
}`,
		},
	}

	for _, useCase := range useCases {
		if !dsunit.InitFromURL(t, path.Join(testLocation, "test", "config.yaml")) {
			return
		}

		if useCase.prepareData {
			caseData := dsunit.NewDatasetResource("db", path.Join(testLocation, fmt.Sprintf("test/cases%vprepare", useCase.caseDataPath)), "", "")
			if !dsunit.Prepare(t, dsunit.NewPrepareRequest(caseData)) {
				return
			}
		}

		ctx := context.Background()
		srv, err := New(ctx, useCase.config)
		if useCase.expectConfigError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			fmt.Printf("%v\n", err)
			continue
		}

		response := srv.Patch(ctx, useCase.request)
		if useCase.expectPatchError {
			assert.EqualValues(t, shared.StatusError, response.Status, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		jsonResponse, _ := json.Marshal(response)
		if !assertly.AssertValues(t, useCase.expect, string(jsonResponse), useCase.description) {
			fmt.Printf("patch: %s\n", jsonResponse)
			toolbox.DumpIndent(response, false)
		}

		if useCase.checkDatabase {
			expectData := dsunit.NewDatasetResource("db", path.Join(testLocation, fmt.Sprintf("test/cases%vexpect", useCase.caseDataPath)), "", "")
			if !dsunit.Expect(t, dsunit.NewExpectRequest(dsunit.SnapshotDatasetCheckPolicy, expectData)) {
				return
			}
		}

	}

}
