package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/generate"
)

func TestHTTPDeclaredOutputFormatSelector(t *testing.T) {
	for _, source := range []string{"header/Accept", "query/_format"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx,
				"CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT, secret TEXT)",
				"INSERT INTO records(id,name,secret) VALUES(7,'Alpha','value')"))
			compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{
				Scope: "example.com/format", Name: "Records",
				Text: fmt.Sprintf(`#setting($_ = $route('/records', 'GET'))
#define($_ = $OutputFormat<string>(%s).Optional().FormatSelector())
#define($_ = $Rows<[]*Row>(output/view))
SELECT records.id, records.name, records.secret FROM records records`, source),
			})
			require.NoError(t, err)
			generated := generate.New(generate.Input{Component: compiled.Component})
			inputType, err := generated.RuntimeInputType()
			require.NoError(t, err)
			field, ok := inputType.FieldByName("OutputFormat")
			require.True(t, ok)
			require.Equal(t, "true", field.Tag.Get("formatSelector"))
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component,
				InputType: inputType, OutputType: reflect.TypeFor[encodingEnvelope](), DirectViewField: "Rows"})
			require.NoError(t, err)
			formatSource, selected := artifact.Output.FormatSelector()
			require.True(t, selected)
			require.Equal(t, source, formatSource.Kind+"/"+formatSource.Name)
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &sql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			entry, err := artifact.Registration(registry.RegisteredComponent{Reader: reader})
			require.NoError(t, err)
			rt, err := runtime.NewRuntime([]*runtime.RegisteredComponent{entry})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(ctx)) })
			handler := NewHandler(rt, nil, "test")
			for _, test := range []struct {
				name, selected, accept, content string
				status                          int
			}{
				{"default", "", "", "application/json", http.StatusOK},
				{"csv", "csv", "text/csv", "text/csv", http.StatusOK},
				{"xml", "xml", "application/xml", "application/xml", http.StatusOK},
				{"xlsx", "xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", http.StatusOK},
				{"unacceptable", "invalid", "text/html", "application/json", http.StatusNotAcceptable},
				{"nonmatching-xml-alias", "invalid", "text/xml", "application/json", http.StatusNotAcceptable},
			} {
				t.Run(test.name, func(t *testing.T) {
					request := httptest.NewRequest(http.MethodGet, "/records", nil)
					if source == "header/Accept" {
						request.Header.Set("Accept", test.accept)
					} else if test.selected != "" {
						query := request.URL.Query()
						query.Set("_format", test.selected)
						request.URL.RawQuery = query.Encode()
					}
					response := httptest.NewRecorder()
					handler.ServeHTTP(response, request)
					if source == "query/_format" && test.status == http.StatusNotAcceptable {
						test.status = http.StatusBadRequest
					}
					require.Equal(t, test.status, response.Code, response.Body.String())
					require.Equal(t, test.content, response.Header().Get("Content-Type"))
					if test.status == http.StatusOK {
						require.NotEmpty(t, response.Body.Bytes())
					}
					if source == "header/Accept" {
						require.Contains(t, response.Header().Values("Vary"), "Accept")
					}
				})
			}
			if source == "header/Accept" {
				for _, test := range []struct{ accept, content string }{
					{"application/json;q=0.2, text/csv;q=0.9", "text/csv"},
					{"application/json;q=0, */*;q=0.5", "text/csv"},
					{"*/*", "application/json"},
				} {
					request := httptest.NewRequest(http.MethodGet, "/records", nil)
					request.Header.Set("Accept", test.accept)
					response := httptest.NewRecorder()
					handler.ServeHTTP(response, request)
					require.Equal(t, test.content, response.Header().Get("Content-Type"), test.accept)
				}
			} else {
				request := httptest.NewRequest(http.MethodGet, "/records", nil)
				request.Header.Set("Accept", "text/csv")
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				require.Equal(t, "application/json", response.Header().Get("Content-Type"), "the query selector must not also bind Accept")
			}
			document, err := (openapi.Generator{}).Generate(ctx, openapi.Request{Info: openapi3.Info{Title: "Format", Version: "1"},
				Components: []*registry.RegisteredComponent{entry}, Routes: []spec.RouteRef{{Method: "GET", Path: "/records"}}})
			require.NoError(t, err)
			content := document.Paths["/records"].Get.Responses["200"].Content
			for _, media := range []string{"application/json", "text/csv", "application/xml", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"} {
				require.NotNil(t, content[media], strings.Join([]string{source, media}, " "))
			}
			if source == "header/Accept" {
				require.Empty(t, document.Paths["/records"].Get.Parameters, "Accept is represented by response media types")
			} else {
				require.Len(t, document.Paths["/records"].Get.Parameters, 1)
				require.Equal(t, "_format", document.Paths["/records"].Get.Parameters[0].Name)
			}
		})
	}
}
