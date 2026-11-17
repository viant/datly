//go:build integration

package predicate

import (
	"context"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "github.com/viant/bigquery"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestDateRangeUTCNativeBigQuery(t *testing.T) {
	endpoint := os.Getenv("DATLY_DATE_RANGE_BQ_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires owned BigQuery emulator")
	}
	parsed, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", parsed.Hostname())
	project := os.Getenv("DATLY_DATE_RANGE_BQ_PROJECT")
	require.NotEmpty(t, project, "set the owned emulator project")
	ctx := context.Background()
	db, err := sql.Open("bigquery", "bigquery://"+project+"/events?emulator=true&endpoint="+url.QueryEscape(endpoint))
	require.NoError(t, err)
	defer db.Close()
	table := fmt.Sprintf("events.readings_%d", time.Now().UnixNano())
	_, err = db.ExecContext(ctx, "CREATE TABLE "+table+"(id INT64,recorded_at TIMESTAMP,event_day DATE)")
	require.NoError(t, err)
	defer func() {
		_, cleanupErr := db.ExecContext(context.Background(), "DROP TABLE "+table)
		require.NoError(t, cleanupErr)
	}()
	_, err = db.ExecContext(ctx, "INSERT INTO "+table+` VALUES (1,TIMESTAMP('2026-11-01T08:00:00Z'),DATE('2026-11-01')),(2,TIMESTAMP('2026-11-01T08:59:59Z'),DATE('2026-11-01')),(3,TIMESTAMP('2026-11-01T09:00:00Z'),DATE('2026-11-01'))`)
	require.NoError(t, err)
	types := typecatalog.NewCatalog()
	pkg := reflect.TypeFor[DateRange]().PkgPath()
	for _, typ := range []reflect.Type{reflect.TypeFor[DateRange](), reflect.TypeFor[DateRangeInput]()} {
		require.NoError(t, types.Register(typecatalog.TypeOriginPackage, x.NewType(typ, x.WithPkgPath(pkg), x.WithName(typ.Name()))))
	}
	source := `#import('window','` + pkg + `')
#setting($_ = $route('/readings','GET'))
#define($_ = $Window<*window.DateRangeInput>(body/window).Required().WithPredicate(0,'handler','window.DateRange','Alias','f','TimeColumn','recorded_at','PartitionColumn','event_day','Mode','utc','MaxDuration','24h'))
SELECT id FROM events.readings f
${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")}
ORDER BY id`
	source = strings.ReplaceAll(source, "events.readings", table)
	compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Scope: "example.com/datefixture", Name: "Readings", Types: types, Text: source})
	require.NoError(t, err)
	type input struct {
		Window *DateRangeInput `json:"window"`
	}
	type row struct {
		ID int `sqlx:"id" json:"id"`
	}
	type output struct {
		Rows []row `json:"rows"`
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, Types: types, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output](), DirectViewField: "Rows"})
	require.NoError(t, err)
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db}})
	require.NoError(t, err)
	native, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[output](), Reader: reader}})
	require.NoError(t, err)
	defer native.Shutdown(ctx)
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"window":{"from":"2026-11-01T01:00:00-07:00","to":"2026-11-01T01:00:00-08:00"}}`, true},
		{`{"window":{"from":"2026-11-01T08:00:00Z","to":"2026-11-03T08:00:00Z"}}`, false},
		{`{"window":{"from":"2026-11-01T09:00:00Z","to":"2026-11-01T08:00:00Z"}}`, false},
	} {
		req := httptest.NewRequest("GET", "/readings", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		scope, e := requestprovider.New(req)
		require.NoError(t, e)
		got, e := native.ExecuteRoute(ctx, "GET", "/readings", scope)
		scope.Close()
		if tc.valid {
			require.NoError(t, e)
			require.Equal(t, []row{{1}, {2}}, got.(*output).Rows)
		} else {
			require.Error(t, e)
		}
	}
}
