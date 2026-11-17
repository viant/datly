package transcribe

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
)

func TestDynamicInputViewStructQLProjection(t *testing.T) {
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE scopes(id INTEGER)", "INSERT INTO scopes VALUES(2),(3)", "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2),(3)"))
	for _, visibility := range []string{"", `.Tag('internal:"true"')`} {
		source := `#package('example.com/dynamic/records')
#setting($_ = $route('/records','GET'))
#define($_ = $ScopeRows<?>(view/ScopeRows).Required().Cardinality('Many')` + visibility + ` /* SELECT id AS pixel_id FROM scopes */)
#define($_ = $ScopeIds<?>(param/ScopeRows).Required()` + visibility + ` /* SELECT ARRAY_AGG(PixelId) AS IDs FROM ` + "`/`" + ` LIMIT 1 */)
#define($_ = $ResolvedIds<[]int>(input/ScopeIds.IDs).Required().WithPredicate(0,'in','r','id'))
#define($_ = $Rows<[]*Record>(output/view))
SELECT r.*,type(r,'Record'),CAST(r.id AS int) FROM (SELECT r.id FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")} ORDER BY r.id) r`
		contract, err := NewCompiler().RuntimeContractsInModule(t.Context(), "example.com/dynamic", &Source{Scope: "example.com/dynamic/records", Name: "Records", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})})
		require.NoError(t, err)
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: contract.Component, InputType: contract.InputType, OutputType: contract.OutputType, Types: contract.Types, Resources: contract.Resources})
		require.NoError(t, err)
		sql := &dsql.SQLComponent{DB: db.DB}
		require.NoError(t, sql.RegisterConnector("main", db.DB))
		config := bootstrap.ReaderRuntimeConfig{SQL: sql}
		reader, err := artifact.ReaderCompilation().NewExecution(config)
		require.NoError(t, err)
		providers, err := artifact.ReaderCompilation().NewProviders(config)
		require.NoError(t, err)
		registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: reader, Providers: providers})
		require.NoError(t, err)
		runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
		require.NoError(t, err)
		scope, err := requestprovider.New(httptest.NewRequest("GET", "/records", nil))
		require.NoError(t, err)
		result, err := runtime.ExecuteRoute(t.Context(), "GET", "/records", scope)
		scope.Close()
		runtime.Shutdown(t.Context())
		require.NoError(t, err, "visibility=%s", visibility)
		encoded, err := json.Marshal(result)
		require.NoError(t, err)
		require.JSONEq(t, `{"Rows":[{"Id":2},{"Id":3}]}`, string(encoded))
	}
}
