package reader

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/base/contract"
	"github.com/viant/datly/config"
	"github.com/viant/datly/data"
	"github.com/viant/datly/db"
	"github.com/viant/datly/generic"
	"github.com/viant/datly/shared"

	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"net/http"
	"net/url"
	"path"
	"testing"
)

func TestService_Read(t *testing.T) {

	testLocation := toolbox.CallerDirectory(3)
	basePath := path.Join(testLocation, "test/cases/")
	connectorURL := path.Join(basePath, "connectors")

	var useCases = []struct {
		description    string
		config         *config.Config
		hasConfigError bool
		hasReadError   bool
		visitor        string
		visit          data.Visit
		caseDataPath   string
		request        *Request
		expect         interface{}
	}{
		{
			description:  "basic data read",
			caseDataPath: "/case001/",
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
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 11,
		"@assertPath@events[0].id": 1
	  }
}`,
		},

		{
			description:  "data view bindingData",
			caseDataPath: "/case002/",
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
					Path:    "/case002/36/blah",
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 5,
		"@assertPath@events[0].id": 4
	  }
}`,
		},
		{
			description:  "multi data selection",
			caseDataPath: "/case003/",
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
					Path:    "/case003/",
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 11,
		"@length@types": 5,
		"@assertPath@events[0].id": 1
	  }
}`,
		},

		{
			description:  "query selector",
			caseDataPath: "/case004/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case004/rule"),
				},
			},
			request: &Request{
				Request: contract.Request{
					TraceID: "case 004",
					Path:    "/case004/",
					QueryParams: url.Values{
						"_fields":  []string{"id,timestamp"},
						"_orderBy": []string{"timestamp"},
						"_limit":   []string{"3"},
						"_offset":  []string{"1"},
					},
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 3,
		"@assertPath@events[0].id": 6
	  }
}`,
		},
		{
			description:  "selector criteria",
			caseDataPath: "/case005/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case005/rule"),
				},
			},
			request: &Request{
				Request: contract.Request{
					TraceID: "case 005",
					Path:    "/case005/",
					Headers: http.Header{
						"User-Id": []string{
							"2",
						},
					},
					QueryParams: url.Values{
						"_criteria": []string{"quantity > 10"},
					},
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 2,
		"@assertPath@events[0].id": 7
	  }
}`,
		},

		{
			description:  "multi selector",
			caseDataPath: "/case006/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case006/rule"),
				},
			},
			request: &Request{
				Request: contract.Request{
					TraceID: "case 006",
					Path:    "/case006/",
					QueryParams: url.Values{
						"_fields":      []string{"id,timestamp"},
						"_limit":       []string{"3"},
						"types_fields": []string{"id,name"},
						"types_limit":  []string{"3"},
					},
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 3,
		"@length@event_types": 3
	  }
}`,
		},
		{
			description:  "one to many reference",
			caseDataPath: "/case007/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case007/rule"),
				},
			},
			request: &Request{
				Request: contract.Request{
					TraceID: "case 007",
					Path:    "/case007/",
					QueryParams: url.Values{
						"_criteria": []string{"account_id IN(33, 37)"},
					},
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@event_types": 3,
		"@assertPath@event_types[0].account.id": 33
	  }
}`,
		},

		{
			description:  "one to one reference",
			caseDataPath: "/case008/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case008/rule"),
				},
			},
			request: &Request{
				Request: contract.Request{
					TraceID: "case 008",
					Path:    "/case008/events/1",
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 1
	  }
}`,
		},

		{
			description:  "read with visitor",
			caseDataPath: "/case001/",
			visitor:      "EventColors",
			visit: func(ctx context.Context, db db.Service, view *data.View, object *generic.Object) (b bool, err error) {
				quantity, err := object.FloatValue("quantity")
				if err != nil || quantity == nil {
					return true, err
				}
				if *quantity > 10 {
					object.SetValue("color", "orange")
				} else {
					object.SetValue("color", "green")
				}
				return true, nil
			},
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case009/rule"),
				},
			},
			request: &Request{
				Request: contract.Request{
					TraceID: "case 009",
					Path:    "/case009/",
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 11,
		"@assertPath@events[0].id": 1,
		"@assertPath@events[0].color": "orange"
	  }
}`,
		},
	}

	//Set visitors
	for _, useCase := range useCases {
		if useCase.visitor != "" {
			data.VisitorRegistry().Register(useCase.visitor, useCase.visit)
		}
	}

	for _, useCase := range useCases {
		if !dsunit.InitFromURL(t, path.Join(testLocation, "test", "config.yaml")) {
			return
		}
		initDataset := dsunit.NewDatasetResource("db", path.Join(testLocation, fmt.Sprintf("test/cases%vprepare", useCase.caseDataPath)), "", "")
		if !dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset)) {
			return
		}

		ctx := context.Background()
		srv, err := New(ctx, useCase.config)
		if useCase.hasConfigError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			fmt.Printf("%v\n", err)
			continue
		}

		response := srv.Read(ctx, useCase.request)
		if useCase.hasReadError {
			assert.EqualValues(t, shared.StatusError, response.Status, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		jsonResponse, _ := json.Marshal(response)
		if !assertly.AssertValues(t, useCase.expect, string(jsonResponse), useCase.description) {
			fmt.Printf("read: %s\n", jsonResponse)
			toolbox.DumpIndent(response, false)
		}
	}

}
