package transcribe

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xhandler "github.com/viant/xdatly/handler"
)

type ReaderInputRules struct{}

func (*ReaderInputRules) Init(ctx context.Context, input any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value := reflect.ValueOf(input).Elem().FieldByName("Minimum")
	value.SetInt(value.Int() + 1)
	return nil
}

func TestDynamicReaderLifecycleDeclaration(t *testing.T) {
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7),(8),(9)"))
	types := typecatalog.NewCatalog()
	typ := reflect.TypeFor[ReaderInputRules]()
	require.NoError(t, types.Register(typecatalog.TypeOriginPackage, x.NewType(typ)))
	source := `#package('example.com/dynamic/records')
#import('hooks','` + typ.PkgPath() + `')
#setting($_ = $route('/records','GET'))
#define($_ = $Minimum<int>(query/minimum).Required().WithPredicate(0,'greater_or_equal','r','id'))
#define($_ = $Rows<[]*Record>(output/view))
SELECT r.*,type(r,'Record'),CAST(r.id AS int),lifecycle_type(r,'hooks.ReaderInputRules')
FROM (SELECT r.id FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")} ORDER BY r.id) r`
	contract, err := NewCompiler().RuntimeContractsInModule(t.Context(), "example.com/dynamic", &Source{Scope: "example.com/dynamic/records", Name: "Records", Text: source, Types: types, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})})
	require.NoError(t, err)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: contract.Component, InputType: contract.InputType, OutputType: contract.OutputType, Types: contract.Types, Resources: contract.Resources})
	require.NoError(t, err)
	route, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/records"})
	require.True(t, ok)
	input := reflect.New(contract.InputType)
	input.Elem().FieldByName("Minimum").SetInt(7)
	initializer, ok := route.InitializerTarget(input.Interface()).(xhandler.Initializer)
	require.True(t, ok)
	require.NoError(t, initializer.Init(t.Context()))
	require.EqualValues(t, 8, input.Elem().FieldByName("Minimum").Int())
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: contract.InputType, OutputType: contract.OutputType, Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: contract.OutputType, Reader: reader}})
	require.NoError(t, err)
	defer runtime.Shutdown(t.Context())
	scope, err := requestprovider.New(httptest.NewRequest("GET", "/records?minimum=7", nil))
	require.NoError(t, err)
	defer scope.Close()
	result, err := runtime.ExecuteRoute(t.Context(), "GET", "/records", scope)
	require.NoError(t, err)
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.JSONEq(t, `{"Rows":[{"Id":8},{"Id":9}]}`, string(encoded), "native predicate must consume the lifecycle-normalized input")
}
