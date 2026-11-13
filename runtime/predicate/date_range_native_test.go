package predicate

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
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

func TestDateRangeNativeReader(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE facts(id INTEGER, local_day TEXT, local_time TEXT, utc_day TEXT);
 INSERT INTO facts VALUES(1,'2026-11-01','2026-11-01 09:00:00','2026-10-31'),(2,'2026-11-01','2026-11-01 10:00:00','2026-11-02'),(3,'2026-11-02','2026-11-02 09:00:00','2026-11-02'),(4,'2026-11-01','2026-11-01 09:30:00','2026-10-28')`)
	require.NoError(t, err)
	types := typecatalog.NewCatalog()
	pkg := reflect.TypeFor[DateRange]().PkgPath()
	for _, typ := range []reflect.Type{reflect.TypeFor[DateRange](), reflect.TypeFor[DateRangeInput]()} {
		require.NoError(t, types.Register(typecatalog.TypeOriginPackage, x.NewType(typ, x.WithPkgPath(pkg), x.WithName(typ.Name()))))
	}
	source := `#import('window','` + pkg + `')
#setting($_ = $route('/date-range-fixture','GET'))
#define($_ = $Window<*window.DateRangeInput>(body/window).Required().WithPredicate(0,'handler','window.DateRange','Alias','f','DateColumn','local_day','TimeColumn','local_time','PartitionColumn','utc_day'))
SELECT id FROM facts f
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}
ORDER BY id`
	compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Scope: "example.com/datefixture", Name: "Facts", Types: types, Text: source})
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
		body string
		want []row
	}{
		{`{"window":{"from":"2026-11-01","to":"2026-11-01"}}`, []row{{1}, {2}}},
		{`{"window":{"from":"2026-11-01T09:00","to":"2026-11-01T10:00"}}`, []row{{1}}},
	} {
		req := httptest.NewRequest("GET", "/date-range-fixture", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		scope, e := requestprovider.New(req)
		require.NoError(t, e)
		got, e := native.ExecuteRoute(ctx, "GET", "/date-range-fixture", scope)
		scope.Close()
		require.NoError(t, e)
		require.Equal(t, tc.want, got.(*output).Rows)
	}
}
