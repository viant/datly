package loader

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"path"
	"testing"
)


func TestService_Load(t *testing.T) {

	parentDir := toolbox.CallerDirectory(3)

	var testCases = []struct{
		description string
		URL string
		expect string
	} {
		{
			description: "parameter ref",
			URL: path.Join(parentDir, "testdata/params/openapi.yaml"),
			expect: `{"openapi":"3.0.0","components":{"parameters":{"limitParam":{"name":"limit","in":"query","description":"The numbers of items to return.","schema":{"type":"integer","default":20,"maximum":50,"minimum":1}},"offsetParam":{"name":"offset","in":"query","description":"The number of items to skip before starting to collect the result set.","schema":{"type":"integer","minimum":0}}}},"info":{"title":"Swagger Petstore","description":"Multi-file boilerplate for OpenAPI Specification.","license":{"name":"MIT"},"version":"1.0.0"},"paths":{"/teams":{"get":{"summary":"Gets a list of teams.","parameters":[{"$ref":"#/components/parameters/offsetParam","name":"offset","in":"query","description":"The number of items to skip before starting to collect the result set.","schema":{"type":"integer","minimum":0}},{"$ref":"#/components/parameters/limitParam","name":"limit","in":"query","description":"The numbers of items to return.","schema":{"type":"integer","default":20,"maximum":50,"minimum":1}}],"responses":{"200":{"description":"OK"}}}},"/users":{"get":{"summary":"Gets a list of users.","parameters":[{"$ref":"#/components/parameters/offsetParam","name":"offset","in":"query","description":"The number of items to skip before starting to collect the result set.","schema":{"type":"integer","minimum":0}},{"$ref":"#/components/parameters/limitParam","name":"limit","in":"query","description":"The numbers of items to return.","schema":{"type":"integer","default":20,"maximum":50,"minimum":1}}],"responses":{"200":{"description":"OK"}}}}},"servers":[{"url":"http://petstore.swagger.io/v1"}]}`,
		},
		{
			description: "schema ref",
			URL: path.Join(parentDir, "testdata/schema/openapi.yaml"),
			expect: `{"openapi":"3.0.0","components":{"schemas":{"User":{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}}}}},"info":{"title":"Swagger Petstore","description":"Multi-file boilerplate for OpenAPI Specification.","license":{"name":"MIT"},"version":"1.0.0"},"paths":{"/users":{"get":{"summary":"Get all users","responses":{"200":{"description":"A list of users.","content":{"application/json":{"schema":{"type":"array","items":{"$ref":"#/components/schemas/User","type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}}}}}}}}}},"/users/{userId}":{"get":{"summary":"Get a user by ID","responses":{"200":{"description":"A single user.","content":{"application/json":{"schema":{"$ref":"#/components/schemas/User","type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}}}}}}}}}}}l`,
		},
	}

	srv := New(afs.New())
	for _, testCase := range testCases[:1] {

		doc, err := srv.LoadURL(context.Background(), testCase.URL)
		if ! assert.Nil(t, err, testCase.description) {
			continue
		}

		data, err := json.Marshal(doc)
		if ! assert.Nil(t, err, testCase.description) {
			continue
		}


		if ! assertly.AssertValues(t, testCase.expect, string(data)) {
			JSON, _ := json.Marshal(doc)
			fmt.Println(string(JSON))
			//YAML, _ := yaml.Marshal(doc)
			//fmt.Println(string(YAML))
		}
	}



}
