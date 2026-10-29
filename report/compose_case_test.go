package report

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/output"
	"github.com/viant/datly/report/cubecompose"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
)

func TestComposeCaseDivisionDynamicOutputContract(t *testing.T) {
	catalog, e := cubecompose.NewCatalog(cubecompose.Field{Name: "count", Type: reflect.TypeFor[int64](), Role: cubecompose.Measure})
	require.NoError(t, e)
	for _, hasElse := range []bool{false, true} {
		expression := "CASE WHEN t1.count > 0 THEN t1.count / 2"
		if hasElse {
			expression += " ELSE 0"
		}
		expression += " END"
		plan, e := cubecompose.Compile("SELECT "+expression+" AS ratio FROM $CubeSQL1 t1 LIMIT 1", catalog, 1, 1)
		require.NoError(t, e)
		rowType, e := (&composeHandler{}).rowType(plan)
		require.NoError(t, e)
		field, _ := rowType.FieldByName("Ratio")
		require.Equal(t, reflect.TypeFor[*float64](), field.Type)
		encoding, e := (output.Compiler{}).Compile(output.CompileInput{Type: rowType, Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
		require.NoError(t, e)
		doc, e := encoding.JSONSchema()
		require.NoError(t, e)
		schemaBytes, e := json.Marshal(doc)
		require.NoError(t, e)
		var schemaDoc struct {
			Properties map[string]struct {
				Type any `json:"type"`
			}
		}
		require.NoError(t, json.Unmarshal(schemaBytes, &schemaDoc))
		require.Contains(t, schemaDoc.Properties["ratio"].Type, "number")
		row := reflect.New(rowType)
		value := row.Elem().FieldByName("Ratio")
		value.Set(reflect.New(value.Type().Elem()))
		value = value.Elem()
		value.SetFloat(1.5)
		wire, e := encoding.Encode(context.Background(), "json", row.Interface())
		require.NoError(t, e)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(wire.Data, &decoded))
		require.Equal(t, 1.5, decoded["ratio"])
		row.Elem().FieldByName("Ratio").SetZero()
		wire, e = encoding.Encode(context.Background(), "json", row.Interface())
		require.NoError(t, e)
		require.NoError(t, json.Unmarshal(wire.Data, &decoded))
		require.Nil(t, decoded["ratio"])
	}
}

func TestComposeCaseFractionAndNullNativeMCP(t *testing.T) {
	h := newGroupedReportHarness(t, false, func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} })
	tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	require.True(t, ok)
	for _, tc := range []struct {
		expression string
		want       any
	}{
		{"CASE WHEN t1.OrderCount > 0 THEN t1.OrderCount / 2.0 ELSE 0 END", float64(1.5)},
		{"CASE WHEN t1.OrderCount < 0 THEN t1.OrderCount / 2 END", nil},
		// SQLite returns integer division1, but its wire contract must remain
		// fraction-capable for warehouse execution of the same integer expression.
		{"CASE WHEN t1.OrderCount > 0 THEN t1.OrderCount / 2 ELSE 0 END", float64(1)},
	} {
		args := map[string]any{"sql": "SELECT " + tc.expression + " AS ratio FROM $CubeSQL1 t1 LIMIT 1", "cubes": []any{map[string]any{"filters": map[string]any{"accountIDs": "1", "tenant": "acme", "channel": "web", "status": "active"}}}}
		result, rpcErr := tool.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args}})
		require.Nil(t, rpcErr)
		require.NotNil(t, result)
		if result.IsError != nil {
			require.False(t, *result.IsError, "%+v", result)
		}
		raw, e := json.Marshal(result.StructuredContent)
		require.NoError(t, e)
		var response struct {
			Columns []struct{ Type string }
			Data    []map[string]any
		}
		require.NoError(t, json.Unmarshal(raw, &response))
		require.Len(t, response.Columns, 1)
		require.Equal(t, "float64", response.Columns[0].Type)
		require.Len(t, response.Data, 1)
		require.Equal(t, tc.want, response.Data[0]["ratio"])
		t.Log(string(raw))
	}
}

type caseNarrowRow struct {
	RowID  int64 `sqlx:"row_id" groupable:"true"`
	Amount int8  `sqlx:"amount" groupable:"false"`
}
type caseNarrowOutput struct{ Rows []*caseNarrowRow }

func TestComposeCaseNarrowComputationNativeMCP(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE case_entries(row_id INTEGER,amount INTEGER)", "INSERT INTO case_entries VALUES(9007199254740993,100),(9007199254740995,100)"))
	on := true
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/numbers", Name: "Narrow"}, Name: "Narrow", Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true, Compose: &spec.CubeComposeSettings{Enabled: true, MaxLimit: 10}}}, Routes: []*spec.Route{{Method: "GET", Path: "/narrow", Name: "Narrow"}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}, RootView: &spec.View{Name: "numbers", Groupable: &on, Selector: &spec.Selector{AllowFields: true, AllowLimit: true}, Source: &spec.ViewSource{SQL: "SELECT n.row_id, SUM(n.amount) AS amount FROM case_entries n GROUP BY n.row_id"}, Columns: []*spec.Column{{Name: "RowID", Source: "row_id", Groupable: &on}, {Name: "Amount", Source: "amount"}}}}
	compilation, e := NewProjectCompiler(ProjectConfig{}).CompileArtifacts([]bootstrap.ArtifactInput{{Component: c, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[caseNarrowOutput](), DirectViewField: "Rows"}})
	require.NoError(t, e)
	registered, e := compilation.RuntimeComponents(ctx, RuntimeConfigureFunc(func(_ context.Context, a *ComponentArtifact) (RuntimeCapabilities, error) {
		if a.IsReport() {
			return RuntimeCapabilities{}, nil
		}
		reader, e := a.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
		return RuntimeCapabilities{Reader: reader}, e
	}))
	require.NoError(t, e)
	runtime, e := druntime.NewRuntime(registered)
	require.NoError(t, e)
	service, e := mcp.New(mcp.Config{Components: registered, Invoker: runtime})
	require.NoError(t, e)
	tool, ok := service.Registry().ToolRegistry.Get("NarrowCubeCompose")
	require.True(t, ok)
	for _, tc := range []struct {
		name, expression, typ string
		rows                  int
		want                  any
	}{
		{"generic wide arithmetic", "t1.Amount + t1.Amount", "int64", 2, float64(200)},
		{"generic sum", "SUM(t1.Amount)", "int64", 1, float64(200)},
		{"generic count", "COUNT(t1.Amount)", "int64", 1, float64(2)},
		{"generic average", "AVG(t1.Amount)", "float64", 1, float64(100)},
		{"generic nullif", "NULLIF(t1.Amount, 100)", "int8", 2, nil},
		{"generic coalesce", "COALESCE(NULLIF(t1.Amount, 100), 7)", "int64", 2, float64(7)},
		{"generic null arithmetic", "NULLIF(t1.Amount, 100) + t1.Amount", "int64", 2, nil},
		{"generic null sum", "SUM(NULLIF(t1.Amount, 100))", "int64", 1, nil},
		{"generic null avg", "AVG(NULLIF(t1.Amount, 100))", "float64", 1, nil},
		{"generic null count", "COUNT(NULLIF(t1.Amount, 100))", "int64", 1, float64(0)},
		{"generic comparison", "SUM(t1.Amount) > 0", "bool", 1, true},
		{"generic unknown comparison", "NULLIF(t1.Amount, 100) > 0", "bool", 2, nil},
		{"generic zero divisor", "t1.Amount / 0", "float64", 2, nil},
		{"generic fraction", "t1.Amount / 8.0", "float64", 2, float64(12.5)},
		{"direct narrow field", "CASE WHEN t1.RowID > 0 THEN t1.Amount ELSE t1.Amount END", "int8", 2, float64(100)},
		{"wide arithmetic", "CASE WHEN t1.RowID > 0 THEN t1.Amount + t1.Amount ELSE t1.Amount END", "int64", 2, float64(200)},
		{"wide aggregate", "CASE WHEN MAX(t1.RowID) > 0 THEN SUM(t1.Amount) ELSE 0 END", "int64", 1, float64(200)},
		{"zero divisor", "CASE WHEN t1.RowID > 0 THEN t1.Amount / 0 ELSE 0 END", "float64", 2, nil},
		{"computed zero divisor", "CASE WHEN t1.RowID > 0 THEN t1.Amount / (t1.Amount - t1.Amount) ELSE 0 END", "float64", 2, nil},
		{"nonnull division", "CASE WHEN t1.RowID > 0 THEN t1.Amount / 8.0 ELSE 0 END", "float64", 2, float64(12.5)},
		{"null arithmetic", "CASE WHEN t1.RowID < 0 THEN t1.Amount + t1.Amount END", "int64", 2, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "NarrowCubeCompose", Arguments: map[string]any{"sql": "SELECT t1.RowID, " + tc.expression + " AS value FROM $CubeSQL1 t1 LIMIT 2", "cubes": []any{map[string]any{}}}}})
			require.Nil(t, rpcErr)
			require.NotNil(t, result)
			if result.IsError != nil {
				require.False(t, *result.IsError, "%+v", result)
			}
			wire, e := json.Marshal(result.StructuredContent)
			require.NoError(t, e)
			var decoded struct {
				Columns []struct{ Type string }
				Data    []map[string]any
			}
			require.NoError(t, json.Unmarshal(wire, &decoded))
			require.Len(t, decoded.Columns, 2)
			require.Equal(t, tc.typ, decoded.Columns[1].Type)
			require.Len(t, decoded.Data, tc.rows)
			for _, row := range decoded.Data {
				require.Equal(t, tc.want, row["value"])
			}
			t.Log(string(wire))
		})
	}
	for _, tc := range []struct{ expression, want string }{
		{"t1.RowID + 0", "9007199254740993"},
		{"CASE WHEN t1.RowID > 0 THEN t1.RowID ELSE 0 END", "9007199254740993"},
		{"SUM(t1.RowID)", "18014398509481988"},
	} {
		t.Run("exact integer "+tc.expression, func(t *testing.T) {
			result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "NarrowCubeCompose", Arguments: map[string]any{"sql": "SELECT " + tc.expression + " AS value FROM $CubeSQL1 t1 ORDER BY value ASC LIMIT 1", "cubes": []any{map[string]any{}}}}})
			require.Nil(t, rpcErr)
			require.NotNil(t, result)
			if result.IsError != nil {
				require.False(t, *result.IsError, "%+v", result)
			}
			content, ok := result.Content[0].(schema.TextContent)
			require.True(t, ok)
			wire := []byte(content.Text)
			var decoded struct {
				Columns []struct{ Type string }
				Data    []map[string]any
			}
			decoder := json.NewDecoder(bytes.NewReader(wire))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&decoded))
			require.Len(t, decoded.Data, 1)
			require.Equal(t, "int64", decoded.Columns[0].Type)
			require.Equal(t, json.Number(tc.want), decoded.Data[0]["value"])
		})
	}

}
