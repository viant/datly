package reader

import (
	"context"
	"datly/base"
	"datly/config"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"

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
	basePath := path.Join(testLocation, "test/read/")
	connectorURL := path.Join(basePath, "connectors")

	var useCases = []struct {
		description    string
		config         *config.Config
		hasConfigError bool
		hasReadError   bool
		caseDataURI    string
		request        *Request
		expect         interface{}
	}{
		{
			description: "basic data read",
			caseDataURI: "/case001/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case001/rule"),
				},
			},
			request: &Request{
						Request: base.Request{
							TraceID: "case 001",
							URI:     "/case001/",
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
			description: "data view binding",
			caseDataURI: "/case002/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case002/rule"),
				},
			},
			request: &Request{
				Request: base.Request{
					TraceID: "case 002",
					URI:     "/case002/36/blah",
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
			description: "multi data selection",
			caseDataURI: "/case003/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case003/rule"),
				},
			},
			request: &Request{
				Request: base.Request{
					TraceID: "case 003",
					URI:     "/case003/",
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
			description: "query selector",
			caseDataURI: "/case004/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case004/rule"),
				},
			},
			request: &Request{
				Request: base.Request{
					TraceID: "case 004",
					URI:     "/case004/",
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
			description: "selector criteria",
			caseDataURI: "/case005/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case005/rule"),
				},
			},
			request: &Request{
				Request: base.Request{
					TraceID: "case 005",
					URI:     "/case005/",
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
			description: "multi selector",
			caseDataURI: "/case006/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case006/rule"),
				},
			},
			request: &Request{
				Request: base.Request{
					TraceID: "case 006",
					URI:     "/case006/",
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
			description: "one to many reference",
			caseDataURI: "/case007/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case007/rule"),
				},
			},
			request: &Request{
				Request: base.Request{
					TraceID: "case 007",
					URI:     "/case007/",
					QueryParams: url.Values{
						"_criteria": []string{"account_id IN(33, 37)"},
					},
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@event_types": 3,
		"@assertPath@event_types[0].account.id": 34
	  }
}`,
		},

		{
			description: "one to one reference",
			caseDataURI: "/case008/",
			config: &config.Config{
				Connectors: config.Connectors{
					URL: connectorURL,
				},
				Rules: config.Rules{
					URL: path.Join(basePath, "case008/rule"),
				},
			},
			request: &Request{
				Request: base.Request{
					TraceID: "case 008",
					URI:     "/case008/events/1",
				},
			},

			expect: `{
	  "Status": "ok",
	  "Data": {
		"@length@events": 1
	  }
}`,
		},
	}

	for _, useCase := range useCases {
		if !dsunit.InitFromURL(t, path.Join(testLocation, "test", "config.yaml")) {
			return
		}
		initDataset := dsunit.NewDatasetResource("db", path.Join(testLocation, fmt.Sprintf("test/read%vprepare", useCase.caseDataURI)), "", "")
		if !dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset)) {
			return
		}

		ctx := context.Background()
		srv, err := New(ctx, useCase.config)
		if useCase.hasConfigError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if ! assert.Nil(t, err, useCase.description) {
			fmt.Printf("%v\n", err)
			continue
		}

		response := srv.Read(ctx, useCase.request)
		if useCase.hasReadError {
			assert.EqualValues(t, base.StatusError, response.Status, useCase.description)
			continue
		}
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		jsonResponse, _ := json.Marshal(response)
		if ! assertly.AssertValues(t, useCase.expect, string(jsonResponse), useCase.description) {
			toolbox.DumpIndent(response, true)
		}
	}

}
