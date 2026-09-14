package http

import (
	"context"
	"encoding/csv"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
)

func TestHTTPSingletonReaderDownloadSQLite(t *testing.T) {
	type input struct {
		ID     int      `parameter:"ID,kind=path,in=id"`
		Fields []string `parameter:"Fields,kind=query,in=_fields"`
	}
	type envelope struct {
		Record *encodingRow `json:"data"`
	}
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT,secret TEXT)", "INSERT INTO records VALUES(7,'Alpha','private'),(8,'Beta','private')"))
	for _, slot := range []string{"", "view", "body"} {
		named := slot != ""
		t.Run(fmt.Sprintf("slot=%s", slot), func(t *testing.T) {
			source := `#setting($_ = $route('/records/{id}/download','GET'))
#setting($_ = $format('csv'))
#setting($_ = $output_title('Record'))
#setting($_ = $output_exclude('Secret'))
#define($_ = $ID<int>(path/id))
#define($_ = $Fields<[]string>(query/_fields).Optional().QuerySelector('RecordDownload'))
`
			outputType := reflect.TypeFor[*encodingRow]()
			if named {
				source += fmt.Sprintf("#define($_ = $Record<?>(output/%s))\n", slot)
				outputType = reflect.TypeFor[envelope]()
			}
			source += "SELECT id,name,secret FROM records WHERE id = $ID"
			compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Scope: "example.com/download", Name: "RecordDownload", Text: source})
			require.NoError(t, err)
			compiled.Component.RootView.Cardinality = spec.CardinalityOne
			compiled.Component.RootView.Selector = &spec.Selector{AllowFields: true}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: reflect.TypeFor[input](), OutputType: outputType})
			require.NoError(t, err)
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: outputType, Reader: reader}})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(ctx)) })
			handler := NewHandler(rt, nil, "test")
			for _, tc := range []struct {
				name, path, content, disposition, want string
				records                                [][]string
			}{
				{name: "download", path: "/records/7/download", content: "text/csv", disposition: `attachment; filename=Record.csv`, records: [][]string{{"id", "name"}, {"7", "Alpha"}}},
				{name: "selected download", path: "/records/7/download?_fields=name", content: "text/csv", disposition: `attachment; filename=Record.csv`, records: [][]string{{"name"}, {"Alpha"}}},
				{name: "later row", path: "/records/8/download", content: "text/csv", disposition: `attachment; filename=Record.csv`, records: [][]string{{"id", "name"}, {"8", "Beta"}}},
				{name: "empty download", path: "/records/99/download", content: "text/csv", disposition: `attachment; filename=Record.csv`, records: [][]string{{"id", "name"}}},
				{name: "tabular", path: "/records/7/download?_format=tabular", content: "application/json", want: `[["id","name"],[7,"Alpha"]]`},
				{name: "selected tabular", path: "/records/7/download?_format=tabular&_fields=name", content: "application/json", want: `[["name"],["Alpha"]]`},
				{name: "JSON shape", path: "/records/7/download?_format=json", content: "application/json", want: `{"id":7,"name":"Alpha"}`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					response := httptest.NewRecorder()
					handler.ServeHTTP(response, httptest.NewRequest("GET", tc.path, nil))
					require.Equal(t, 200, response.Code, response.Body.String())
					require.Equal(t, tc.content, response.Header().Get("Content-Type"))
					require.Equal(t, tc.disposition, response.Header().Get("Content-Disposition"))
					if tc.records != nil {
						actual, err := csv.NewReader(strings.NewReader(response.Body.String())).ReadAll()
						require.NoError(t, err)
						require.Equal(t, tc.records, actual)
					} else {
						want := tc.want
						if named {
							want = `{"data":` + want + `}`
						}
						require.JSONEq(t, want, response.Body.String())
					}
				})
			}
		})
	}
}
