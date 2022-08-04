package cmd

import (
	"bytes"
	"context"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/mem"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/router/openapi3"
	"github.com/viant/dsunit"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"
)

type memoryWriter struct {
	data []byte
}

func (m *memoryWriter) Write(data []byte) (n int, err error) {
	m.data = append(m.data, data...)
	return len(data), nil
}

type testcase struct {
	URI         string
	args        []string
	description string
	openApiURL  string
	viewURL     string
	dataURL     string
	dataMethod  string
}

func TestRun(t *testing.T) {
	TimeNow = func() time.Time {
		parse, _ := time.Parse("2006-01-02 15:04:05.000000000 -0700 MST", "2014-11-12 11:45:26.000000000 +0000 UTC")
		return parse
	}

	_ = toolbox.CreateDirIfNotExist(path.Join("/", "tmp", "datly", "generator"))

	currentLocation := toolbox.CallerDirectory(3)
	configLocation := path.Join(currentLocation, "testdata", "config.yaml")

	testCases := []*testcase{
		{
			description: "basic",
			URI:         "case001_basic",
			openApiURL:  "/v1/api/meta/openapi/",
			args:        []string{"-N=foos", "-D=sqlite3", "-A=/tmp/datly/generator/db.db"},
			viewURL:     "/v1/api/meta/view/dev/foos",
			dataURL:     "/v1/api/dev/foos",
			dataMethod:  http.MethodGet,
		},
		{
			description: "relation one to one",
			URI:         "case002_one_to_one",
			openApiURL:  "/v1/api/meta/openapi/",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly/generator/db.db", "-R=event_types:event_types:One"},
			viewURL:     "/v1/api/meta/view/dev/events",
			dataURL:     "/v1/api/dev/events",
			dataMethod:  http.MethodGet,
		},
		{
			description: "column codec",
			URI:         "case003_columns_codec",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly/generator/db.db", "-X=testdata/case003_columns_codec/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events",
			dataURL:     "/v1/api/dev/events",
			dataMethod:  http.MethodGet,
		},
		{
			description: "group by",
			URI:         "case004_group_by",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly/generator/db.db", "-X=testdata/case004_group_by/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events",
			dataURL:     "/v1/api/dev/events?quantity=10",
			dataMethod:  http.MethodGet,
		},
		{
			description: "inner join",
			URI:         "case005_inner_join",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly/generator/db.db", "-X=testdata/case005_inner_join/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events",
			dataURL:     "/v1/api/dev/events?quantity=10",
			dataMethod:  http.MethodGet,
		},
		{
			description: "velty syntax",
			URI:         "case006_velty",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly/generator/db.db", "-X=testdata/case006_velty/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events",
			dataURL:     "/v1/api/dev/events?quantity=10",
			dataMethod:  http.MethodGet,
		},
		{
			description: "param column alias",
			URI:         "case007_param_alias",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly/generator/db.db", "-X=testdata/case007_param_alias/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events/1",
			dataURL:     "/v1/api/dev/events/1",
			dataMethod:  http.MethodGet,
		},
		{
			description: "cache hint",
			URI:         "case008_cache_hint",
			args: []string{
				"-N=events",
				"-D=sqlite3",
				"-A=/tmp/datly/generator/db.db",
				"-X=testdata/case008_cache_hint/events.sql",
			},
			viewURL:    "/v1/api/meta/view/dev/events/1",
			dataMethod: http.MethodGet,
		},
		{
			description: "selector hint",
			URI:         "case009_selector_hint",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly/generator/db.db", "-X=testdata/case009_selector_hint/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events/1",
			dataURL:     "/v1/api/dev/events/1",
			dataMethod:  http.MethodGet,
		},
		{
			description: "type definition",
			URI:         "case010_acl_param",
			args: []string{
				"-N=eventTypes",
				"-D=sqlite3",
				"-A=/tmp/datly/generator/db.db",
				"-X=testdata/case010_acl_param/event_types.sql",
			},
			viewURL:    "/v1/api/meta/view/dev/event_types",
			dataMethod: http.MethodGet,
		},
	}

	loader := afs.New()
	//for i, testCase := range testCases[len(testCases)-1:] {
	for i, testCase := range testCases[1:] {
		mem.ResetSingleton()
		gateway.ResetSingleton()

		fmt.Printf("Running testcase: %v\n", i)
		logger := &memoryWriter{}
		testLocation := path.Join(currentLocation, "testdata", testCase.URI)
		datasetPath := path.Join(testLocation, "populate")
		if !initDb(t, configLocation, datasetPath) {
			continue
		}

		server, err := New("", testCase.args, logger)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		checkGeneratedOpenAPI(t, loader, testLocation, testCase, server)
		checkGeneratedView(t, loader, testLocation, testCase, server)
		checkReadData(t, server, testCase, loader, testLocation)
		//generateLogs(loader, testLocation, logger, testCase)
	}
}

func generateLogs(loader afs.Service, testLocation string, logger *memoryWriter, testCase *testcase) {
	_ = loader.Upload(context.TODO(), path.Join(testLocation, "log.txt"), file.DefaultFileOsMode, bytes.NewReader(logger.data))
}

func checkReadData(t *testing.T, server *standalone.Server, testCase *testcase, loader afs.Service, testLocation string) {
	if testCase.dataURL == "" {
		return
	}

	actualData, err := readResponse(server.Handler, testCase.dataURL, testCase.dataMethod, nil)
	if !assert.Nil(t, err, testCase.description) {
		return
	}

	expectedData, err := readExpectedData(loader, testLocation)
	if !assert.Nil(t, err, testCase.description) {
		return
	}

	assert.Equal(t, string(expectedData), string(actualData), testCase.description)
}

func checkGeneratedView(t *testing.T, loader afs.Service, testLocation string, testCase *testcase, server *standalone.Server) {
	expectedView, err := readExpectedView(loader, testLocation)
	if !assert.Nil(t, err, testCase.description) {
		return
	}

	actualView, err := readResponse(server.Handler, testCase.viewURL, http.MethodGet, nil)
	if !assert.Nil(t, err, testCase.description) {
		return
	}

	assert.Equal(t, string(expectedView), string(actualView), testCase.description)
	return
}

func checkGeneratedOpenAPI(t *testing.T, loader afs.Service, testLocation string, testCase *testcase, server *standalone.Server) {
	actualLocation := path.Join(testLocation, "openapi3.yaml")
	if ok, err := loader.Exists(context.TODO(), actualLocation); !ok || err == nil {
		return
	}

	expectedOpenAPI, err := readExpectedOpenAPI(loader, actualLocation)
	if !assert.Nil(t, err, testCase.description) {
		return
	}

	actualOpenAPI, err := readResponse(server.Handler, testCase.openApiURL, http.MethodGet, nil)
	if !assert.Nil(t, err, testCase.description) {
		return
	}

	assert.Equal(t, string(expectedOpenAPI), string(actualOpenAPI), testCase.description)
}

func readExpectedData(loader afs.Service, location string) ([]byte, error) {
	location = path.Join(location, "expected.txt")
	return loader.DownloadWithURL(context.TODO(), location)
}

func readExpectedView(loader afs.Service, location string) ([]byte, error) {
	location = path.Join(location, "view.yaml")

	data, err := loader.DownloadWithURL(context.TODO(), location)
	if err != nil {
		return nil, err
	}

	dest := map[string]interface{}{}
	if err = yaml.Unmarshal(data, &dest); err != nil {
		return nil, err
	}

	return yaml.Marshal(dest)
}

func readExpectedOpenAPI(loader afs.Service, testLocation string) ([]byte, error) {
	asBytes, err := loader.DownloadWithURL(context.TODO(), testLocation)
	if err != nil {
		return nil, err
	}

	api, err := asOpenApi(asBytes)
	if err != nil {
		return nil, err
	}

	return yaml.Marshal(api)
}

func asOpenApi(bytes []byte) (*openapi3.OpenAPI, error) {
	openapi := &openapi3.OpenAPI{}
	if err := yaml.Unmarshal(bytes, openapi); err != nil {
		return nil, err
	}

	return openapi, nil
}

func initDb(t *testing.T, configPath, datasetPath string) bool {
	if !dsunit.InitFromURL(t, configPath) {
		return false
	}

	initDataset := dsunit.NewDatasetResource("db", datasetPath, "", "")
	request := dsunit.NewPrepareRequest(initDataset)
	if !dsunit.Prepare(t, request) {
		return false
	}

	return true
}

func readResponse(handler http.Handler, url string, method string, body io.Reader) ([]byte, error) {
	request := httptest.NewRequest(method, url, body)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	return ioutil.ReadAll(recorder.Body)
}
