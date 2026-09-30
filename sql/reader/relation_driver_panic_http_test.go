package reader_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/testutil/sqlfault"
)

type driverPanicChild struct {
	ID int `sqlx:"id"`
}
type driverPanicParent struct {
	ID       int                 `sqlx:"id"`
	Children []*driverPanicChild `view:"children,batch=1,batchConcurrency=2" on:"ID:id=ID:id" sql:"SELECT id FROM children WHERE $COLUMN_IN" json:"children"`
}
type driverPanicOutput struct {
	Data []*driverPanicParent `parameter:",kind=output,in=view" view:"parents" sql:"SELECT id FROM parents ORDER BY id" json:"data"`
}

func TestRelationDriverPanicHTTPServerSurvives(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER)", "INSERT INTO parents VALUES(1),(2)", "CREATE TABLE children(id INTEGER)", "INSERT INTO children VALUES(1),(2)"))
	var fail atomic.Bool
	fail.Store(true)
	wrapped := *db
	wrapped.DB = db.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if call.Phase == "query" && strings.Contains(call.SQL, "FROM children") && fail.CompareAndSwap(true, false) {
			panic("private driver parameter failure")
		}
		return nil
	})
	f := typedGraphFixture{db: &wrapped, component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "PanicRead"}, Name: "PanicRead", Routes: []*spec.Route{{Method: "GET", Path: "/graph"}}}, output: reflect.TypeFor[driverPanicOutput]()}
	app, _, err := f.compile()
	require.NoError(t, err)
	handler, err := (gateway.Config{}).NewHandler(app, nil, "test")
	require.NoError(t, err)
	for _, status := range []int{500, 200} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/graph", nil))
		require.Equal(t, status, response.Code, response.Body.String())
		if status == 500 {
			require.Contains(t, response.Body.String(), "Internal Server Error")
			require.NotContains(t, response.Body.String(), "private driver parameter failure")
		} else {
			var result driverPanicOutput
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
			require.Len(t, result.Data, 2)
			for _, row := range result.Data {
				require.Len(t, row.Children, 1)
				require.Equal(t, row.ID, row.Children[0].ID)
			}
		}
	}
}
