package cmd

import (
	"context"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
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
	return
	TimeNow = func() time.Time {
		parse, _ := time.Parse("2006-01-02 15:04:05.000000000 -0700 MST", "2014-11-12 11:45:26.000000000 +0000 UTC")
		return parse
	}

	defer func() {
		TimeNow = func() time.Time {
			return time.Now()
		}
	}()

	_ = toolbox.CreateDirIfNotExist(path.Join("/", "tmp", "datly_tests"))

	currentLocation := toolbox.CallerDirectory(3)
	configLocation := path.Join(currentLocation, "testdata", "config.yaml")

	testCases := []*testcase{
		{
			description: "enable last child batch when ExecKind = Service specified",
			args: []string{
				"-C=dev|sqlite3|/tmp/datly_tests/db.db",
				"-X=testdata/cases/case001_batch_enabled_no_ref/events.sql",
			},
			viewURL:    "/v1/api/meta/view/dev/events",
			URI:        "cases/case001_batch_enabled_no_ref",
			httpMethod: "POST",
		},
	}

	loader := afs.New()
	//for i, testCase := range testCases[len(testCases)-1:] {
	for i, testCase := range testCases {
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

	if !assert.Equal(t, string(expectedView), string(actualView), testCase.description) {
		return
	}

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
