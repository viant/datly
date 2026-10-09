package report

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
)

func TestCubeTransientColumnsBeforeProjectionValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		column    *spec.Column
		wantError string
	}{
		{name: "measure", column: &spec.Column{Name: "DaysRemaining", Source: "days_remaining", Tag: `sqlx:"-"`}},
		{name: "dimension", column: &spec.Column{Name: "DaysRemaining", Source: "days_remaining", Groupable: boolPointer(true), Tag: `sqlx:"-"`}},
		{name: "SQL collision", column: &spec.Column{Name: "DaysRemaining", Output: "total_spend", Tag: `sqlx:"-"`}},
		{name: "Go field collision", column: &spec.Column{Name: "TotalSpend", Source: "days_remaining", Tag: `sqlx:"-"`}},
		{name: "invalid identity", column: &spec.Column{Name: "DaysRemaining", Output: "not an identifier", Selector: "invalid selector", Tag: `sqlx:"-"`}},
		{name: "selectable collision", column: &spec.Column{Name: "Other", Output: "total_spend"}, wantError: "duplicate projected column"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := reportSource(t, &spec.ReportSettings{Enabled: true})
			source.Component.RootView.Columns = append(source.Component.RootView.Columns, tc.column)
			before := source.Component.Clone()
			contract, ok := source.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/spend"})
			require.True(t, ok)
			metadata, err := compileMetadata(source.Component, contract)
			require.Equal(t, before, source.Component.Clone(), "report analysis mutated reader metadata")
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Len(t, metadata.dimensions, 1)
			require.Len(t, metadata.measures, 1)
			require.Equal(t, "TotalSpend", metadata.measures[0].fieldName)
		})
	}
}

type transientReportRow struct {
	AccountId         int    `sqlx:"account_id" groupable:"true" json:"accountId"`
	FlightId          int    `sqlx:"flight_id" json:"flightId"`
	DaysRemaining     int    `sqlx:"-" json:"daysRemaining"`
	ComputedDimension string `sqlx:"-" groupable:"true" json:"computedDimension"`
}

type transientReportOutput struct {
	Rows []*transientReportRow `parameter:",kind=output,in=view" json:"data"`
}

func (o *transientReportOutput) Finalize(context.Context) error {
	for _, row := range o.Rows {
		row.DaysRemaining = 7
		row.ComputedDimension = "computed"
	}
	return nil
}

func TestCubeTransientSuppliedAndInferredMetadata(t *testing.T) {
	for _, mode := range []string{"supplied", "inferred"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			require.NoError(t, db.ExecStatements(ctx,
				"CREATE TABLE flights (account_id INTEGER, flight_id INTEGER)",
				"INSERT INTO flights VALUES (1, 11), (1, 22)"))
			component := &spec.Component{
				Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/reports", Name: "Flights"},
				Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}},
				Routes:   []*spec.Route{{Method: "GET", Path: "/flights"}},
				RootView: &spec.View{Name: "flights", Groupable: boolPointer(true),
					Selector: &spec.Selector{AllowFields: true},
					Source:   &spec.ViewSource{SQL: "SELECT account_id, MAX(flight_id) AS flight_id FROM flights GROUP BY account_id"}},
			}
			if mode == "supplied" {
				for i, name := range []string{"account_id", "flight_id", "days_remaining", "computed_dimension"} {
					field := reflect.TypeFor[transientReportRow]().Field(i)
					component.RootView.Columns = append(component.RootView.Columns, &spec.Column{
						Name: field.Name, Source: name, Tag: string(field.Tag), Groupable: boolPointer(i == 0 || i == 3),
					})
				}
			}
			before := component.Clone()
			compilation, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).CompileArtifacts([]bootstrap.ArtifactInput{{
				Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[transientReportOutput](),
			}})
			require.NoError(t, err)
			require.Equal(t, before, component.Clone())
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
			t.Cleanup(func() { runtime.Shutdown(ctx) })
			for _, artifact := range compilation.Artifacts() {
				if !artifact.IsReport() {
					continue
				}
				for _, section := range []string{"Dimensions", "Measures"} {
					field, ok := artifact.InputType().FieldByName(section)
					require.True(t, ok)
					require.Equal(t, 1, field.Type.NumField())
					_, found := field.Type.FieldByName("DaysRemaining")
					require.False(t, found)
					_, found = field.Type.FieldByName("ComputedDimension")
					require.False(t, found)
				}
			}
			service, err := mcp.New(mcp.Config{Components: registered, Invoker: runtime})
			require.NoError(t, err)
			tool, ok := service.Catalog().Tool("FlightsCube")
			require.True(t, ok)
			for section, selected := range map[string]string{"dimensions": "accountId", "measures": "flightId"} {
				properties := tool.Metadata().InputSchema.Properties[section]["properties"]
				require.Contains(t, properties, selected)
				require.NotContains(t, properties, "daysRemaining")
				require.NotContains(t, properties, "computedDimension")
			}
			for _, request := range []struct{ method, path, body, expected string }{
				{"GET", "/flights", "", `{"data":[{"accountId":1,"flightId":22,"daysRemaining":7,"computedDimension":"computed"}]}`},
				{"POST", "/flights/cube", `{"dimensions":{"accountId":true},"measures":{"flightId":true}}`, `{"data":[{"accountId":1,"flightId":22}]}`},
			} {
				req := httptest.NewRequest(request.method, request.path, strings.NewReader(request.body))
				req.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				httpgateway.NewHandler(runtime, nil, "").ServeHTTP(response, req)
				require.Equal(t, 200, response.Code, response.Body.String())
				require.JSONEq(t, request.expected, response.Body.String())
			}
			cube, ok := service.Registry().ToolRegistry.Get("FlightsCube")
			require.True(t, ok)
			result, protocolErr := cube.Handler(ctx, &schema.CallToolRequest{
				Method: schema.MethodToolsCall,
				Params: schema.CallToolRequestParams{Name: "FlightsCube", Arguments: map[string]any{
					"dimensions": map[string]any{"accountId": true}, "measures": map[string]any{"flightId": true},
				}},
			})
			require.Nil(t, protocolErr)
			require.NotNil(t, result)
			require.False(t, result.IsError != nil && *result.IsError)
			require.JSONEq(t, `{"data":[{"accountId":1,"flightId":22}]}`, result.Content[0].(schema.TextContent).Text)
		})
	}
}
