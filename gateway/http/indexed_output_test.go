package http

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

type indexedOutputLoader struct {
	registered *druntime.RegisteredComponent
	calls      int
	failAfter  int
}

func (l *indexedOutputLoader) LoadComponent(ctx context.Context, key spec.Key) (*druntime.RegisteredComponent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.calls++
	if l.failAfter > 0 && l.calls > l.failAfter {
		return nil, errors.New("private loader failure")
	}
	if key != l.registered.Component.Key {
		return nil, errors.New("unexpected component")
	}
	return l.registered, nil
}

type indexedOutputInput struct {
	Fields []string `parameter:"Fields,kind=query,in=fields" querySelector:"records"`
}
type indexedOutputRow struct {
	ID       int     `sqlx:"id" json:"id"`
	Nullable *string `sqlx:"nullable" json:"nullable"`
	Zero     int     `sqlx:"zero" json:"zero"`
	Enabled  bool    `sqlx:"enabled" json:"enabled"`
	Empty    string  `sqlx:"empty" json:"empty"`
	Extra    string  `sqlx:"extra" json:"extra"`
	Secret   string  `sqlx:"secret" json:"secret"`
}
type indexedOutputEnvelope struct {
	Rows []indexedOutputRow `parameter:"Rows,kind=output,in=view" json:"data"`
}

func TestIndexedHTTPOutputContract(t *testing.T) {
	for _, tc := range []struct {
		name, format, query string
		failAfter           int
	}{
		{"projection", "", "?fields=id&fields=nullable&fields=zero&fields=enabled&fields=empty", 0},
		{"authored exclusion", "", "", 0},
		{"authored CSV", "csv", "", 0},
		{"loader failure after execution", "", "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sqlite.New(t)
			_, err := db.DB.Exec("CREATE TABLE records (id INTEGER, nullable TEXT, zero INTEGER, enabled BOOLEAN, empty TEXT, extra TEXT, secret TEXT)")
			require.NoError(t, err)
			_, err = db.DB.Exec("INSERT INTO records VALUES (1, NULL, 0, FALSE, '', 'unselected-value', 'private-value')")
			require.NoError(t, err)
			component := &spec.Component{
				Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}},
				Settings: &spec.Settings{Format: tc.format, Output: &spec.OutputSettings{Exclude: []string{"Secret"}}},
				RootView: &spec.View{Name: "records", Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: "SELECT id, nullable, zero, enabled, empty, extra, secret FROM records"}},
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[indexedOutputInput](), OutputType: reflect.TypeFor[indexedOutputEnvelope](), DirectViewField: "Rows"})
			require.NoError(t, err)
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			reg := &druntime.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[indexedOutputEnvelope](), Reader: reader}
			loader := &indexedOutputLoader{registered: reg, failAfter: tc.failAfter}
			rt, err := druntime.NewIndexedRuntime([]*spec.Component{artifact.Component}, nil, loader)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
			res := httptest.NewRecorder()
			NewHandler(rt, nil, "test").ServeHTTP(res, httptest.NewRequest("GET", "/records"+tc.query, nil))
			if tc.failAfter > 0 {
				require.Equal(t, 500, res.Code, res.Body.String())
				require.Equal(t, 2, loader.calls)
				require.NotContains(t, res.Body.String(), "private")
				require.NotContains(t, res.Body.String(), "unselected-value")
				return
			}
			require.Equal(t, 200, res.Code, res.Body.String())
			require.NotContains(t, res.Body.String(), "private-value")
			if tc.format == "csv" {
				require.Contains(t, res.Header().Get("Content-Type"), "text/csv")
			} else if tc.query != "" {
				require.JSONEq(t, `{"data":[{"id":1,"nullable":null,"zero":0,"enabled":false,"empty":""}]}`, res.Body.String())
			} else {
				require.JSONEq(t, `{"data":[{"id":1,"nullable":null,"zero":0,"enabled":false,"empty":"","extra":"unselected-value"}]}`, res.Body.String())
			}
		})
	}
}
