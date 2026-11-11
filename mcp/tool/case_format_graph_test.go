package tool

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestExposureCaseFormatRootAndChildSelectors(t *testing.T) {
	type request struct {
		Fields        []string
		Limit         int
		Offset        int
		Page          int
		OrderBy       string
		Criteria      string
		ChildFields   []string
		ChildLimit    int
		ChildOffset   int
		ChildPage     int
		ChildOrderBy  string
		ChildCriteria string
		Explicit      int `json:"Exact_Name"`
		Named         int `mcp:"AccountingId"`
	}
	typ := reflect.TypeFor[request]()
	bindings := []bindly.BindingSpec{}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		bindings = append(bindings, bindly.BindingSpec{Path: f.Name, Location: bindstate.Location{Kind: "query", In: f.Name}})
	}
	contract := testRouteContract(t, typ, bindings)
	for _, tc := range []struct {
		format string
		names  []string
	}{{"lc", []string{"fields", "limit", "offset", "page", "orderBy", "criteria", "childFields", "childLimit", "childOffset", "childPage", "childOrderBy", "childCriteria", "Exact_Name", "AccountingId"}}, {"", []string{"fields", "limit", "offset", "page", "orderBy", "criteria", "childFields", "childLimit", "childOffset", "childPage", "childOrderBy", "childCriteria", "Exact_Name", "AccountingId"}}} {
		plan, err := NewCompiler().Compile(Input{CaseFormat: tc.format, Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "GraphSelectors"}})
		require.NoError(t, err)
		values := map[string]any{}
		for i, name := range tc.names {
			if typ.Field(i).Type.Kind() == reflect.Slice {
				values[name] = []string{"Id"}
			} else if typ.Field(i).Type.Kind() == reflect.Int {
				values[name] = 7
			} else {
				values[name] = "value"
			}
		}
		scope, err := plan.Scope(values)
		require.NoError(t, err)
		defer scope.Close()
		for i, name := range tc.names {
			value, found, err := scope.Query().Locate(nil).Value(context.Background(), typ.Field(i).Type, typ.Field(i).Name)
			require.NoError(t, err)
			require.True(t, found)
			require.NotNil(t, value)
			require.Contains(t, plan.Metadata().InputSchema.Properties, name)
			var selected *Argument
			for j := range plan.args {
				if plan.args[j].publicName == name {
					selected = &plan.args[j]
					break
				}
			}
			require.NotNil(t, selected)
			require.Equal(t, typ.Field(i).Name, selected.sourceName)
			require.Equal(t, typ.Field(i).Name, selected.path)
		}
	}
}

func TestExposureCaseFormatCollisionAfterNormalization(t *testing.T) {
	type request struct {
		AccountID int
		Other     int `json:"accountId"`
	}
	contract := testRouteContract(t, reflect.TypeFor[request](), []bindly.BindingSpec{{Path: "AccountID", Location: bindstate.Location{Kind: "query", In: "AccountID"}}, {Path: "Other", Location: bindstate.Location{Kind: "query", In: "Other"}}})
	_, err := NewCompiler().Compile(Input{CaseFormat: "lc", Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "Collision"}})
	require.ErrorContains(t, err, "duplicate public argument")
}

func TestExposureCaseFormatNamedBodyRecursiveBinding(t *testing.T) {
	type child struct {
		MetricID int
		Enabled  bool
		Note     *string
	}
	type request struct{ Body struct{ ChildRows []*child } }
	contract := testRouteContract(t, reflect.TypeFor[request](), []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body", In: "Body"}}})
	plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "Nested"}})
	require.NoError(t, err)
	require.Contains(t, plan.Metadata().InputSchema.Properties, "body")
	args := map[string]any{"body": map[string]any{"childRows": []any{map[string]any{"metricId": float64(0), "enabled": false, "note": nil}}}}
	scope, err := plan.Scope(args)
	require.NoError(t, err)
	defer scope.Close()
	typ := reflect.TypeFor[request]().Field(0).Type
	value, found, err := scope.Body().Locate(nil).Value(context.Background(), typ, "Body")
	require.NoError(t, err)
	require.True(t, found)
	rows := reflect.ValueOf(value).FieldByName("ChildRows")
	require.Equal(t, 1, rows.Len())
	require.Equal(t, int64(0), rows.Index(0).Elem().FieldByName("MetricID").Int())
	require.False(t, rows.Index(0).Elem().FieldByName("Enabled").Bool())
	require.True(t, rows.Index(0).Elem().FieldByName("Note").IsNil())
}

func TestExposureCaseFormatOutputTypeWithoutPrecompiledPlan(t *testing.T) {
	type response struct{ ChildRows []struct{ MetricID int } }
	contract := testRouteContract(t, reflect.TypeFor[struct{}](), nil)
	plan, err := NewCompiler().Compile(Input{Contract: contract, OutputType: reflect.TypeFor[response](), Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "TypedOutput"}})
	require.NoError(t, err)
	require.NotNil(t, plan.OutputPlan())
	encoded, err := plan.OutputPlan().Encode(context.Background(), "json", response{})
	require.NoError(t, err)
	require.Contains(t, string(encoded.Data), `"childRows"`)
}

func TestExposureCaseFormatJSONTaggedBodyWinner(t *testing.T) {
	type row struct {
		Id    int
		Other string `json:"id"`
	}
	type request struct{ Body row }
	contract := testRouteContract(t, reflect.TypeFor[request](), []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body", In: "Body"}}})
	plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "TaggedBody"}})
	require.NoError(t, err)
	properties := plan.Metadata().InputSchema.Properties["body"]["properties"].(map[string]any)
	require.Len(t, properties, 1)
	require.Equal(t, "string", properties["id"].(map[string]any)["type"])
	scope, err := plan.Scope(map[string]any{"body": map[string]any{"id": "winner"}})
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Body().Locate(nil).Value(context.Background(), reflect.TypeFor[row](), "Body")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, row{Other: "winner"}, value)
}
