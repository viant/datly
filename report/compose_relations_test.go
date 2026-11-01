package report

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	httpgateway "github.com/viant/datly/gateway/http"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xhandler "github.com/viant/xdatly/handler"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type graphSpendRow struct {
	Tenant     string           `sqlx:"tenant"`
	AccountID  int              `sqlx:"account_id"`
	Region     string           `sqlx:"region"`
	TotalSpend float64          `sqlx:"total_spend"`
	OrderCount int              `sqlx:"order_count"`
	Label      *graphSpendLabel `sqlx:"-" json:"label"`
}

type graphSpendLabel struct {
	Tenant    string `sqlx:"tenant"`
	AccountID int    `sqlx:"account_id"`
	Label     string `sqlx:"label"`
	Secret    string `sqlx:"secret" json:"-" internal:"true"`
}

func (*graphSpendRow) OnFetch(context.Context) error {
	return fmt.Errorf("source root hook must not execute for composition")
}
func (*graphSpendRow) OnRelation(context.Context) {
	panic("source root hook must not execute for composition")
}

type graphSpendOutput struct{ Rows []*graphSpendRow }

func newComposeGraphHarness(t *testing.T, enabled bool) *groupedReportHarness {
	return (groupedReportHarnessConfig{output: reflect.TypeFor[graphSpendOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) {
		s.Compose = &spec.CubeComposeSettings{Enabled: true, ProjectionRelations: enabled}
	}}, source: func(c *spec.Component, _ *typecatalog.Catalog) {
		relation := c.RootView.Relations[0]
		relation.Name = "Label"
		relation.Holder = "Label"
		relation.Cardinality = spec.CardinalityOne
		// Preserve the bound source filter in the child SQL as well as the complete
		// native composite parent key. Foreign dictionary rows share the same ID.
		relation.View.Source.SQL = "SELECT tenant,account_id,label,'do-not-emit' AS secret FROM report_detail WHERE tenant=$Tenant AND $COLUMN_IN"
	}}).build(t)
}

func graphComposeCall(t *testing.T, h *groupedReportHarness, request map[string]any) (int, []byte) {
	t.Helper()
	body, err := json.Marshal(request)
	require.NoError(t, err)
	req := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(recorder, req)
	return recorder.Code, recorder.Body.Bytes()
}
func graphComposeRequest() map[string]any {
	return map[string]any{"cubes": []any{map[string]any{"filters": map[string]any{"tenant": "acme", "region": "EU", "channel": "web", "status": "active"}}}, "sql": "SELECT t1.tenant AS owner,t1.account_id AS key,t1.total_spend AS amount FROM $CubeSQL1 t1 ORDER BY amount DESC,key ASC LIMIT 1", "relations": []string{"label"}}
}

func TestComposeProjectionRelationsPagingKeysAndCompatibility(t *testing.T) {
	ctx := context.Background()
	h := newComposeGraphHarness(t, true)
	_, err := h.sql.DB.ExecContext(ctx, "DELETE FROM report_detail WHERE tenant='acme' AND label='beta'")
	require.NoError(t, err)
	request := graphComposeRequest()
	status, body := graphComposeCall(t, h, request)
	require.Equal(t, 200, status, string(body))
	require.NotContains(t, string(body), "do-not-emit")
	var result struct {
		Columns   []any                 `json:"columns"`
		Relations []CompositionRelation `json:"relations"`
		Data      []struct {
			Owner  string              `json:"owner"`
			Key    int                 `json:"key"`
			Amount float64             `json:"amount"`
			Label  *groupedSpendDetail `json:"label"`
		} `json:"data"`
		HasMore bool `json:"hasMore"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Len(t, result.Columns, 3)
	require.Equal(t, []CompositionRelation{{Name: "label", Keys: []string{"owner", "key"}}}, result.Relations)
	require.Len(t, result.Data, 1)
	require.Equal(t, 1, result.Data[0].Key)
	require.Equal(t, "alpha", result.Data[0].Label.Label)
	require.Equal(t, "acme", result.Data[0].Label.Tenant)
	require.True(t, result.HasMore)
	// A duplicate only on the lookahead row must not be read before page trimming.
	_, err = h.sql.DB.ExecContext(ctx, "INSERT INTO report_detail VALUES('acme',2,'ambiguous lookahead')")
	require.NoError(t, err)
	status, body = graphComposeCall(t, h, request)
	require.Equal(t, 200, status, string(body))
	request["page"] = map[string]any{"limit": 1, "offset": 1}
	status, _ = graphComposeCall(t, h, request)
	require.NotEqual(t, 200, status)
	_, err = h.sql.DB.ExecContext(ctx, "DELETE FROM report_detail WHERE tenant='acme' AND account_id=2")
	require.NoError(t, err)
	status, body = graphComposeCall(t, h, request)
	require.Equal(t, 200, status, string(body))
	require.NoError(t, json.Unmarshal(body, &result))
	require.Len(t, result.Data, 1)
	require.Equal(t, 2, result.Data[0].Key)
	require.Nil(t, result.Data[0].Label)
	request["resultLimit"] = 1
	status, body = graphComposeCall(t, h, request)
	require.Equal(t, 200, status, string(body))
	require.NoError(t, json.Unmarshal(body, &result))
	require.Empty(t, result.Data)
	// No requested relation preserves the exact scalar response even when the
	// source declaration enables the capability; invalid dictionary SQL is idle.
	delete(request, "relations")
	delete(request, "resultLimit")
	delete(request, "page")
	_, err = h.sql.DB.ExecContext(ctx, "DROP TABLE report_detail")
	require.NoError(t, err)
	status, enabledScalar := graphComposeCall(t, h, request)
	require.Equal(t, 200, status, string(enabledScalar))
	require.NotContains(t, string(enabledScalar), "relations")
	off := newComposeGraphHarness(t, false)
	status, disabledScalar := graphComposeCall(t, off, request)
	require.Equal(t, 200, status, string(disabledScalar))
	require.JSONEq(t, string(disabledScalar), string(enabledScalar))
}

func TestComposeProjectionRelationsRejectUnsupportedKeys(t *testing.T) {
	h := newComposeGraphHarness(t, true)
	for _, sql := range []string{
		"SELECT t1.account_id AS key,t1.total_spend AS amount FROM $CubeSQL1 t1 LIMIT 2",
		"SELECT t1.tenant AS owner,t1.account_id+0 AS key,t1.total_spend AS amount FROM $CubeSQL1 t1 LIMIT 2",
		"SELECT t1.tenant AS owner,t1.account_id AS key,t1.account_id AS duplicate FROM $CubeSQL1 t1 LIMIT 2",
		"SELECT t1.tenant AS owner,t1.account_id AS key,SUM(t1.total_spend) AS amount FROM $CubeSQL1 t1 LIMIT 2",
	} {
		request := graphComposeRequest()
		request["sql"] = sql
		status, _ := graphComposeCall(t, h, request)
		require.NotEqual(t, 200, status, sql)
	}
	for _, names := range [][]string{{"missing"}, {"label", "label"}} {
		request := graphComposeRequest()
		request["relations"] = names
		status, _ := graphComposeCall(t, h, request)
		require.NotEqual(t, 200, status)
	}
}

func TestPreparedCompositionRelationMetadataAndDefaultOff(t *testing.T) {
	h := newComposeGraphHarness(t, true)
	request := graphComposeRequest()
	prepared, err := PrepareComposition(h.compose, CompositionDefinition{SQL: request["sql"].(string), FrameCount: 1, Relations: []string{"label"}})
	require.NoError(t, err)
	want := []CompositionRelation{{Name: "label", Keys: []string{"owner", "key"}}}
	require.Equal(t, want, prepared.Relations())
	copy := prepared.Relations()
	copy[0].Keys[0] = "changed"
	require.Equal(t, want, prepared.Relations())
	off := newComposeGraphHarness(t, false)
	_, err = PrepareComposition(off.compose, CompositionDefinition{SQL: request["sql"].(string), FrameCount: 1, Relations: []string{"label"}})
	require.ErrorContains(t, err, "not enabled")
	// UNION/join provenance is deliberately unsupported rather than inferred.
	for _, sql := range []string{
		"SELECT t1.tenant AS owner,t1.account_id AS key FROM $CubeSQL1 t1 UNION ALL SELECT t2.tenant AS owner,t2.account_id AS key FROM $CubeSQL2 t2 LIMIT 2",
		"SELECT t1.tenant AS owner,t1.account_id AS key FROM $CubeSQL1 t1 JOIN $CubeSQL2 t2 ON t1.account_id=t2.account_id LIMIT 2",
	} {
		_, err = PrepareComposition(h.compose, CompositionDefinition{SQL: sql, FrameCount: 2, Relations: []string{"label"}})
		require.ErrorContains(t, err, "one non-UNION source frame")
	}
	many := newGroupedReportHarness(t, false, func(s *spec.ReportSettings) {
		s.Compose = &spec.CubeComposeSettings{Enabled: true, ProjectionRelations: true}
	})
	_, err = PrepareComposition(many.compose, CompositionDefinition{SQL: request["sql"].(string), FrameCount: 1, Relations: []string{"details"}})
	require.ErrorContains(t, err, "not an exposed to-one relation")
}

func TestComposeProjectionRelationsRejectSourceEnvelopePolicies(t *testing.T) {
	for _, settings := range []*spec.Settings{
		{Output: &spec.OutputSettings{Exclude: []string{"Rows.Label.Secret"}}},
		{Output: &spec.OutputSettings{OmitEmpty: true}},
		{Output: &spec.OutputSettings{NilSlicePolicy: "empty_array"}},
		{JSONMarshalType: "CustomRedactor"},
		{DateFormat: "YYYY-MM-DD"},
	} {
		component := groupedReportComponent(&spec.ReportSettings{Enabled: true})
		component.Settings = settings
		_, err := composeRelations(component, reflect.TypeFor[graphSpendRow]())
		require.ErrorContains(t, err, "source-envelope output policies")
	}
}

type projectionNoInvokeBinder struct{ lookups int }

func (*projectionNoInvokeBinder) Bind(context.Context, any) error { return nil }
func (b *projectionNoInvokeBinder) Lookup(context.Context, xhandler.ValueKey) (any, bool, error) {
	b.lookups++
	return nil, false, fmt.Errorf("invoker must not be reached")
}

func TestComposeProjectionRelationsRejectHiddenAggregatesBeforeInvocation(t *testing.T) {
	h := newComposeGraphHarness(t, true)
	for _, expression := range []string{
		"CASE WHEN t1.total_spend BETWEEN SUM(t1.total_spend) AND 100 THEN 1 ELSE 0 END",
		"CASE WHEN t1.total_spend IN (SUM(t1.total_spend),100) THEN 1 ELSE 0 END",
		"CASE WHEN t1.total_spend>0 THEN CASE WHEN t1.total_spend IN (SUM(t1.total_spend),100) THEN 1 ELSE 0 END ELSE 0 END",
	} {
		sql := "SELECT t1.tenant AS owner,t1.account_id AS key," + expression + " AS flag FROM $CubeSQL1 t1 LIMIT 5"
		_, err := PrepareComposition(h.compose, CompositionDefinition{SQL: sql, FrameCount: 1, Relations: []string{"label"}})
		require.ErrorContains(t, err, "requires direct dimension", sql)
		input := reflect.New(h.compose.inputType)
		input.Elem().FieldByName("Cubes").Set(reflect.MakeSlice(input.Elem().FieldByName("Cubes").Type(), 1, 1))
		input.Elem().FieldByName("SQL").SetString(sql)
		input.Elem().FieldByName("Relations").Set(reflect.ValueOf([]string{"label"}))
		binder := &projectionNoInvokeBinder{}
		_, err = h.compose.Execute(context.Background(), rhandler.Invocation{Input: input.Interface(), Binder: binder})
		require.ErrorContains(t, err, "requires direct dimension", sql)
		require.Zero(t, binder.lookups, "rejection must precede invoker lookup, source query preparation and SQL reads")
	}
}

func TestComposeProjectionRelationsRejectGroupingAliasBeforeInvocation(t *testing.T) {
	h := newComposeGraphHarness(t, true)
	sql := "SELECT t1.tenant AS owner,t1.account_id AS total_spend,SUM(t1.total_spend) AS total FROM $CubeSQL1 t1 GROUP BY t1.tenant,total_spend LIMIT 5"
	_, err := PrepareComposition(h.compose, CompositionDefinition{SQL: sql, FrameCount: 1, Relations: []string{"label"}})
	require.ErrorContains(t, err, "explicit qualified dimension GROUP BY")
	input := reflect.New(h.compose.inputType)
	input.Elem().FieldByName("Cubes").Set(reflect.MakeSlice(input.Elem().FieldByName("Cubes").Type(), 1, 1))
	input.Elem().FieldByName("SQL").SetString(sql)
	input.Elem().FieldByName("Relations").Set(reflect.ValueOf([]string{"label"}))
	binder := &projectionNoInvokeBinder{}
	_, err = h.compose.Execute(context.Background(), rhandler.Invocation{Input: input.Interface(), Binder: binder})
	require.ErrorContains(t, err, "explicit qualified dimension GROUP BY")
	require.Zero(t, binder.lookups)
	// The same colliding output alias is valid when grouping uses exact source
	// dimensions. Existing source grain and label identity remain explicit.
	qualified := strings.Replace(sql, "GROUP BY t1.tenant,total_spend", "GROUP BY t1.tenant,t1.account_id", 1)
	prepared, err := PrepareComposition(h.compose, CompositionDefinition{SQL: qualified, FrameCount: 1, Relations: []string{"label"}})
	require.NoError(t, err)
	require.Equal(t, []CompositionRelation{{Name: "label", Keys: []string{"owner", "total_spend"}}}, prepared.Relations())
	_, err = h.sql.DB.ExecContext(context.Background(), "DELETE FROM report_detail WHERE tenant='acme' AND label='beta'")
	require.NoError(t, err)
	request := graphComposeRequest()
	request["sql"] = qualified
	status, body := graphComposeCall(t, h, request)
	require.Equal(t, 200, status, string(body))
	var result struct {
		Data []struct {
			Key   int                 `json:"total_spend"`
			Label *groupedSpendDetail `json:"label"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Len(t, result.Data, 2)
	for _, row := range result.Data {
		require.NotNil(t, row.Label)
		require.Equal(t, row.Key, row.Label.AccountID)
	}
}
