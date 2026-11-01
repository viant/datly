package report_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/report"
	druntime "github.com/viant/datly/runtime"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
)

func TestComposeProjectionRelationsDynamicDQLHTTP(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE facts(tenant TEXT,entity_id INTEGER,amount REAL)", "INSERT INTO facts VALUES('allowed',1,9),('allowed',1,3),('allowed',2,5),('foreign',1,900)", "CREATE TABLE directory(tenant TEXT,id INTEGER,name TEXT)", "INSERT INTO directory VALUES('allowed',1,'First'),('allowed',2,'Second'),('foreign',1,'Foreign')"))
	text := `#package('example.com/inventory/read')
#setting($_ = $route('/inventory','GET'))
#setting($_ = $case_format('lc'))
#setting($_ = $cube())
#setting($_ = $cubeCompose(true,false,1,20,30000,'struct { Mode string }',true))
#define($_ = $Tenant<string>(query/tenant).Required().WithPredicate(0,'equal','f','tenant'))
#define($_ = $Data<?>(output/view))
SELECT facts.*,entity.*,CAST(facts.entity_id AS *int),CAST(facts.amount AS *float64),
 tag(facts.tenant,'groupable:"true"'),tag(facts.entity_id,'groupable:"true"'),tag(facts.amount,'groupable:"false"'),
 grouping_enabled(facts),selector_fields(facts,true),selector_order_by(facts,true),selector_limit(facts,true),selector_offset(facts,true)
FROM (SELECT f.tenant,f.entity_id,SUM(f.amount) AS amount FROM facts f
${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")}
GROUP BY f.tenant,f.entity_id) facts
LEFT JOIN (SELECT tenant,id,name FROM directory) entity ON entity.tenant=facts.tenant AND entity.id=facts.entity_id AND 1=1`
	contract, err := transcribe.NewCompiler().RuntimeContractsInModule(ctx, "example.com/inventory", &transcribe.Source{Scope: "example.com/inventory/read", Name: "Inventory", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})})
	require.NoError(t, err)
	compiled, err := report.NewProjectCompiler(report.ProjectConfig{Types: contract.Types}).CompileArtifacts([]bootstrap.ArtifactInput{{Component: contract.Component, InputType: contract.InputType, OutputType: contract.OutputType, Types: contract.Types, Resources: contract.Resources}})
	require.NoError(t, err)
	registered, err := compiled.RuntimeComponents(ctx, report.RuntimeConfigureFunc(func(_ context.Context, artifact *report.ComponentArtifact) (report.RuntimeCapabilities, error) {
		if artifact.IsReport() {
			return report.RuntimeCapabilities{}, nil
		}
		execution, e := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
		return report.RuntimeCapabilities{Reader: execution}, e
	}))
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime(registered)
	require.NoError(t, err)
	t.Cleanup(func() { runtime.Shutdown(ctx) })
	request := `{"cubes":[{"filters":{"tenant":"allowed"}}],"sql":"SELECT t1.tenant AS owner,t1.entity_id AS entityId,t1.amount AS value FROM $CubeSQL1 t1 ORDER BY value DESC LIMIT 1","relations":["entity"],"values":{"Mode":"safe"}}`
	req := httptest.NewRequest("POST", "/inventory/cube/compose", strings.NewReader(request))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	httpgateway.NewHandler(runtime, nil, "").ServeHTTP(recorder, req)
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
	var result struct {
		Data []struct {
			ID     int     `json:"entityId"`
			Value  float64 `json:"value"`
			Entity struct {
				Name string `json:"name"`
			} `json:"entity"`
		} `json:"data"`
		Relations []report.CompositionRelation `json:"relations"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
	require.Len(t, result.Data, 1)
	require.Equal(t, 1, result.Data[0].ID)
	require.Equal(t, 12.0, result.Data[0].Value)
	require.Equal(t, "First", result.Data[0].Entity.Name)
	require.Equal(t, []report.CompositionRelation{{Name: "entity", Keys: []string{"owner", "entityId"}}}, result.Relations)
}
