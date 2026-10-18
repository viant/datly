package runtime_test

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/postselectors"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestSelectorCompanionPostBodyAndQuerySQLite(t *testing.T) {
	input, output := reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]()
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	require.Len(t, found.Components, 1)
	component, err := found.Components[0].Resolve(input, output)
	require.NoError(t, err)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: input, OutputType: output})
	require.NoError(t, err)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two'),(3,'three')"))
	execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: input, OutputType: output, Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: output, Reader: execution}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	handler := gateway.NewHandler(rt, nil, "post-selectors")
	for _, tc := range []struct {
		query, body string
		ids         []int
	}{
		{"?id=1&sort=id%20DESC&limit=1&page=2", `{"min":1}`, []int{2}},
		{"?id=3", `{"min":0}`, []int{3}},
		{"?id=1&sort=id%20DESC&limit=1&offset=1", `{"min":0}`, []int{2}},
		{"?id=1&sort=id%20DESC&limit=1&page=1", `{"min":1}`, []int{3}},
	} {
		request := httptest.NewRequest("POST", "/post-records"+tc.query, strings.NewReader(tc.body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		require.Equal(t, 200, response.Code, response.Body.String())
		var actual fixture.Output
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &actual))
		var ids []int
		for _, row := range actual.Data {
			ids = append(ids, row.ID)
		}
		require.Equal(t, tc.ids, ids)
	}
	for _, name := range []string{"Page", "Sort", "Limit", "Offset"} {
		_, present := input.FieldByName(name)
		require.False(t, present)
	}
}
