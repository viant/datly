package report

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
)

type projectedMCPRow struct {
	ID      *int     `sqlx:"id" json:"id" groupable:"true"`
	Enabled *bool    `sqlx:"enabled" json:"enabled" groupable:"true"`
	Count   *int     `sqlx:"count" json:"count"`
	Price   *float64 `sqlx:"price" json:"price"`
	Label   *string  `sqlx:"label" json:"label"`
	Unused  *int     `sqlx:"unused" json:"unused"`
}
type projectedMCPOutput struct {
	Rows     []*projectedMCPRow `parameter:",kind=output,in=view" json:"data"`
	Status   string             `json:"status"`
	Metadata map[string]string  `json:"metadata"`
}

func (o *projectedMCPOutput) Finalize(context.Context) error {
	o.Status = "ok"
	o.Metadata = map[string]string{"source": "fixture"}
	return nil
}

func TestReportMCPOutputSelectionPreservesNullZeroAndMetadata(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE projected (id INTEGER, enabled BOOLEAN, count INTEGER, price REAL, label TEXT, unused INTEGER)",
		"INSERT INTO projected VALUES (0, 0, 0, NULL, '', 42)",
	))
	on := true
	compilation, err := NewProjectCompiler(ProjectConfig{}).CompileArtifacts([]bootstrap.ArtifactInput{{
		Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Projected"}, Name: "Projected",
			Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}},
			Routes:   []*spec.Route{{Method: "GET", Path: "/projected"}},
			RootView: &spec.View{Name: "projected", Groupable: &on, AllowNulls: &on, Selector: &spec.Selector{AllowFields: true},
				Source: &spec.ViewSource{SQL: "SELECT id, enabled, SUM(count) AS count, SUM(price) AS price, MAX(label) AS label, MAX(unused) AS unused FROM projected GROUP BY id, enabled"}},
		}, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[projectedMCPOutput](), DirectViewField: "Rows",
	}})
	require.NoError(t, err)
	registered, err := compilation.RuntimeComponents(ctx, RuntimeConfigureFunc(func(_ context.Context, artifact *ComponentArtifact) (RuntimeCapabilities, error) {
		if artifact.IsReport() {
			return RuntimeCapabilities{}, nil
		}
		reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
		return RuntimeCapabilities{Reader: reader}, err
	}))
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime(registered)
	require.NoError(t, err)
	service, err := mcp.New(mcp.Config{Components: registered, Invoker: runtime})
	require.NoError(t, err)
	tool, found := service.Registry().ToolRegistry.Get("ProjectedCube")
	require.True(t, found)
	result, protocolErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "ProjectedCube", Arguments: map[string]any{
		"dimensions": map[string]any{"id": true, "enabled": true},
		"measures":   map[string]any{"count": true, "price": true, "label": true},
	}}})
	require.Nil(t, protocolErr)
	require.NotNil(t, result)
	if result.IsError != nil {
		require.False(t, *result.IsError, "%+v", result)
	}
	encoded, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	want := `{"data":[{"id":0,"enabled":false,"count":0,"price":null,"label":""}],"status":"ok","metadata":{"source":"fixture"}}`
	require.JSONEq(t, want, string(encoded))
	require.NotEmpty(t, result.Content)
	text, ok := result.Content[0].(schema.TextContent)
	require.True(t, ok)
	require.JSONEq(t, want, text.Text)
}
