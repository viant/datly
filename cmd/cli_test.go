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
	"github.com/viant/datly/internal/tests"
	"github.com/viant/datly/router/openapi3"
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
	httpMethod  string
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
			description: "column codec",
			URI:         "case001_columns_codec",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly_tests/db.db", "-X=testdata/case001_columns_codec/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events",
			dataURL:     "/v1/api/dev/events",
			httpMethod:  http.MethodGet,
		},
		{
			description: "group by",
			URI:         "case002_group_by",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly_tests/db.db", "-X=testdata/case002_group_by/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events",
			dataURL:     "/v1/api/dev/events?quantity=10",
			httpMethod:  http.MethodGet,
		},
		{
			description: "inner join",
			URI:         "case003_inner_join",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly_tests/db.db", "-X=testdata/case003_inner_join/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events",
			dataURL:     "/v1/api/dev/events?quantity=10",
			httpMethod:  http.MethodGet,
		},
		{
			description: "velty syntax",
			URI:         "case004_velty",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly_tests/db.db", "-X=testdata/case004_velty/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events",
			dataURL:     "/v1/api/dev/events?quantity=10",
			httpMethod:  http.MethodGet,
		},
		{
			description: "param column alias",
			URI:         "case005_param_alias",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly_tests/db.db", "-X=testdata/case005_param_alias/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events/1",
			dataURL:     "/v1/api/dev/events/1",
			httpMethod:  http.MethodGet,
		},
		{
			description: "cache hint",
			URI:         "case006_cache_hint",
			args: []string{
				"-N=events",
				"-D=sqlite3",
				"-A=/tmp/datly_tests/db.db",
				"-X=testdata/case006_cache_hint/events.sql",
			},
			viewURL:    "/v1/api/meta/view/dev/events/1",
			httpMethod: http.MethodGet,
		},
		{
			description: "selector hint",
			URI:         "case007_selector_hint",
			args:        []string{"-N=events", "-D=sqlite3", "-A=/tmp/datly_tests/db.db", "-X=testdata/case007_selector_hint/events.sql"},
			viewURL:     "/v1/api/meta/view/dev/events/1",
			dataURL:     "/v1/api/dev/events/1",
			httpMethod:  http.MethodGet,
		},
		{
			description: "type definition",
			URI:         "case008_acl_param",
			args: []string{
				"-N=eventTypes",
				"-D=sqlite3",
				"-A=/tmp/datly_tests/db.db",
				"-X=testdata/case008_acl_param/event_types.sql",
			},
			viewURL:    "/v1/api/meta/view/dev/event_types",
			httpMethod: http.MethodGet,
		},
		{
			description: "update",
			URI:         "case009_update",
			args: []string{
				"-N=eventTypes",
				"-D=sqlite3",
				"-A=/tmp/datly_tests/db.db",
				"-X=testdata/case009_update/update.sql",
			},
			viewURL:    "/v1/api/meta/view/dev/status",
			httpMethod: http.MethodGet,
		},
		{
			description: "set view param",
			URI:         "case010_set_view_param",
			args: []string{
				"-N=eventTypes",
				"-D=sqlite3",
				"-A=/tmp/datly_tests/db.db",
				"-X=testdata/case010_set_view_param/update.sql",
			},
			viewURL:    "/v1/api/meta/view/dev/status",
			httpMethod: http.MethodGet,
		},
		{
			description: "AsInts codec",
			URI:         "case011_ints_codec",
			args: []string{
				"-N=eventTypes",
				"-D=sqlite3",
				"-A=/tmp/datly_tests/db.db",
				"-X=testdata/case011_ints_codec/update.sql",
			},
			viewURL:    "/v1/api/meta/view/dev/status",
			httpMethod: http.MethodGet,
		},
		//{
		//	description: "Insert type detection",
		//	URI:         "case014_insert",
		//	args: []string{
		//		"-N=eventTypes",
		//		"-D=sqlite3",
		//		"-A=/tmp/datly_tests/db.db",
		//		"-X=testdata/case014_insert/insert.sql",
		//	},
		//	viewURL:    "/v1/api/meta/view/dev/status",
		//	httpMethod: http.MethodPost,
		//},
	}

	loader := afs.New()
	//for i, testCase := range testCases[len(testCases)-1:] {
	for i, testCase := range testCases[7:8] {
		mem.ResetSingleton()
		gateway.ResetSingleton()
		tests.LogHeader(fmt.Sprintf("Running testcase: %v\n", i))

		logger := &memoryWriter{}
		testLocation := path.Join(currentLocation, "testdata", testCase.URI)
		datasetPath := path.Join(testLocation, "populate")
		if !tests.InitDB(t, configLocation, datasetPath, "db") {
			continue
		}

		server, err := New("", testCase.args, logger)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		checkGeneratedOpenAPI(t, testCase, loader, testLocation, testCase, server)
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

	actualData, err := readResponse(server.Handler, testCase.dataURL, testCase.httpMethod, nil)
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

	httpMethod := http.MethodGet
	if testCase.httpMethod != "" {
		httpMethod = testCase.httpMethod
	}

	actualView, err := readResponse(server.Handler, testCase.viewURL, httpMethod, nil)
	if !assert.Nil(t, err, testCase.description) {
		return
	}

	assert.Equal(t, string(expectedView), string(actualView), testCase.description)
	return
}

func checkGeneratedOpenAPI(t *testing.T, c *testcase, loader afs.Service, testLocation string, testCase *testcase, server *standalone.Server) {
	if c.openApiURL == "" {
		return
	}

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

func readResponse(handler http.Handler, url string, method string, body io.Reader) ([]byte, error) {
	request := httptest.NewRequest(method, url, body)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	return ioutil.ReadAll(recorder.Body)
}
