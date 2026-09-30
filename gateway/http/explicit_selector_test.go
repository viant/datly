package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/generate"
)

// A declared body selector field is both the binding and permission to use it.
// set_limit remains the base limit; no selector_limit directive is needed.
func TestHTTPExplicitBodyLimitAutoEnablesSelector(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE connectors(id INTEGER PRIMARY KEY)",
		"INSERT INTO connectors(id) VALUES(1),(2),(3)"))
	compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{
		Scope: "example.com/connectors", Name: "connector",
		Text: `#setting($_ = $route('/connectors', 'POST'))
#define($_ = $Limit<int>(body/limit).WithTag('json:"limit"').Optional().QuerySelector('connector'))
#define($_ = $Rows<[]*Row>(output/view))
SELECT connector.id, set_limit(connector,25) FROM connectors connector`,
	})
	require.NoError(t, err)
	generated := generate.New(generate.Input{Component: compiled.Component})
	inputType, err := generated.RuntimeInputType()
	require.NoError(t, err)
	outputType, err := generated.RuntimeOutputType()
	require.NoError(t, err)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component,
		InputType: inputType, OutputType: outputType, DirectViewField: "Rows"})
	require.NoError(t, err)
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &sql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	rt, err := runtime.NewRuntime([]*runtime.RegisteredComponent{{Component: artifact.Component,
		Input: artifact.Input, OutputType: outputType, Reader: reader}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(ctx)) })
	handler := NewHandler(rt, nil, "test")
	for _, test := range []struct {
		body string
		rows int
	}{{body: `{}`, rows: 3}, {body: `{"limit":1}`, rows: 1}} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/connectors", strings.NewReader(test.body))
		request.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var payload map[string][]map[string]any
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
		require.Len(t, payload["Rows"], test.rows, response.Body.String())
	}
}
