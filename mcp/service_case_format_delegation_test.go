package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	rh "github.com/viant/datly/runtime/handler"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
	xh "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/state"
	"github.com/xeipuuv/gojsonschema"
)

type delegationCaseInput struct {
	Fields    []string `parameter:"Fields,kind=query,in=fields"`
	OrderBy   string   `parameter:"OrderBy,kind=query,in=sort"`
	Criteria  string   `parameter:"Criteria,kind=query,in=criteria"`
	Limit     int      `parameter:"Limit,kind=query,in=limit"`
	Offset    int      `parameter:"Offset,kind=query,in=offset"`
	Page      int      `parameter:"Page,kind=query,in=page"`
	Reentered bool     `internal:"true" json:"-"`
}
type delegationCaseRow struct {
	ID              int    `sqlx:"id"`
	SampleSeen_1Day int    `sqlx:"sample_seen_1_day"`
	Fixed           string `sqlx:"fixed" json:"Exact_Name"`
	Secret          string `sqlx:"secret" internal:"true" json:"-"`
}
type delegationCaseOutput struct {
	Data []*delegationCaseRow `parameter:",kind=output,in=view"`
}

// This fixture deliberately uses the existing framework-aware invocation API.
// The handler authors the exact parent Data -> child Data row association; no
// transport name, similar field, or ambient parent request infers delegation.
type delegationParentOutput struct{ Data []*delegationCaseRow }

func TestExposureCaseFormatExplicitTypedDelegation(t *testing.T) {
	for _, mode := range []string{"explicit", "unmapped", "sibling", "reentrant"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,sample_seen_1_day INTEGER,fixed TEXT,secret TEXT)", "INSERT INTO records VALUES(1,10,'one','private'),(2,20,'two','private'),(3,30,'three','private')"))
			childSpec := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "DelegatedChild"}, RootView: &spec.View{Name: "records", Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowCriteria: true, AllowLimit: true, AllowOffset: true, AllowPage: true, Orderable: []spec.FieldPath{"sample_seen_1_day"}, Filterable: []spec.FieldPath{"SampleSeen_1Day"}}, Source: &spec.ViewSource{SQL: "SELECT id,sample_seen_1_day,fixed FROM records"}}, Routes: []*spec.Route{{Method: "GET", Path: "/delegated-child"}}}
			childArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: childSpec, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[delegationCaseOutput](), DirectViewField: "Data"})
			require.NoError(t, err)
			reader, err := childArtifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			child, err := childArtifact.Registration(druntime.RegisteredComponent{Reader: reader})
			require.NoError(t, err)
			childTarget := exec.ComponentTarget{Component: childSpec.Key, Route: spec.RouteRef{Method: "GET", Path: "/delegated-child"}}
			parentSpec := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "DelegatingParent"}, Settings: &spec.Settings{CaseFormat: "uc"}, Routes: []*spec.Route{{Method: "GET", Path: "/delegating-parent", Handler: "test.Parent", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "DelegatingCamel", CaseFormat: "lc"}, {Kind: spec.MCPExposureTool, Name: "DelegatingSnake", CaseFormat: "lu"}}}}}
			parentTarget := exec.ComponentTarget{Component: parentSpec.Key, Route: spec.RouteRef{Method: "GET", Path: "/delegating-parent"}}
			parentArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: parentSpec, InputType: reflect.TypeFor[delegationCaseInput](), OutputType: reflect.TypeFor[delegationParentOutput]()})
			require.NoError(t, err)
			var invocationFailure error
			parent, err := parentArtifact.Registration(druntime.RegisteredComponent{Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				invocationFailure = nil
				input := inv.Input.(*delegationCaseInput)
				value, ok, err := inv.Binder.Lookup(ctx, exec.ComponentInvokerKey)
				if err != nil {
					return nil, err
				}
				if !ok {
					return nil, fmt.Errorf("scoped invoker unavailable")
				}
				native := value.(exec.ComponentInvoker)
				if mode == "reentrant" && !input.Reentered {
					next := *input
					next.Reentered = true
					result, err := native.InvokeComponent(ctx, exec.ComponentRequest{Target: parentTarget, Input: &next})
					invocationFailure = err
					return result, err
				}
				fields, err := exec.PresentedSelectorFields(ctx, parentSpec.Key, reflect.TypeFor[delegationCaseRow](), "Data")
				if err != nil {
					return nil, err
				}
				var presentation *exec.SelectorPresentation
				if mode != "unmapped" && fields != nil {
					// Metadata is copied by PresentedSelectorFields and validated against the
					// child's exact canonical row index by the native reader.
					presentation = exec.NewSelectorPresentation(childTarget, func(row reflect.Type, path string) ([]exec.SelectorFieldName, error) {
						if row != reflect.TypeFor[delegationCaseRow]() || path != "Data" {
							return nil, fmt.Errorf("undeclared delegated row mapping")
						}
						return fields, nil
					})
				}
				selectors := state.Selectors{&state.NamedSelector{Name: "records", Selector: state.Selector{Fields: input.Fields, OrderBy: input.OrderBy, Criteria: input.Criteria, Limit: input.Limit, Offset: input.Offset, Page: input.Page}}}
				childCtx := exec.CaptureChildOutputSelection(ctx)
				request := exec.ComponentRequest{Target: childTarget, Input: &struct{}{}, Providers: []locator.Provider{hp.Static(xh.SelectorsKey, selectors)}, SelectorPresentation: presentation}
				actual, err := native.InvokeComponent(childCtx, request)
				invocationFailure = err
				if err != nil {
					return nil, err
				}
				if mode == "sibling" {
					// A second call receives neither the first child's capability nor any
					// automatically inherited parent presentation.
					request.SelectorPresentation = nil
					_, err = native.InvokeComponent(childCtx, request)
					invocationFailure = err
					if err != nil {
						return nil, err
					}
				}
				output := &delegationParentOutput{Data: actual.(*delegationCaseOutput).Data}
				exec.PublishOutputSelection(ctx, output, exec.SelectedOutputFields(childCtx, actual))
				return output, nil
			})})
			require.NoError(t, err)
			runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{parent, child})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, runtime.Shutdown(ctx)) })
			service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{parent, child}, Invoker: runtime})
			require.NoError(t, err)
			nativeClient := mcpclient.New(t, service, schema.LatestProtocolVersion)
			listed, err := nativeClient.ListTools(ctx, nil)
			require.NoError(t, err)
			require.Len(t, listed.Tools, 2)
			for _, format := range []struct{ name, metric, order string }{{"DelegatingCamel", "sampleSeen1Day", "orderBy"}, {"DelegatingSnake", "sample_seen_1_day", "order_by"}} {
				require.NoError(t, service.PrepareTool(ctx, format.name))
				tool, ok := service.Registry().ToolRegistry.Get(format.name)
				require.True(t, ok)
				inputSchema, err := json.Marshal(tool.Metadata.InputSchema)
				require.NoError(t, err)
				require.Contains(t, string(inputSchema), `"`+format.order+`"`)
				for _, property := range []string{"fields", "limit", "offset", "page", "criteria"} {
					require.Contains(t, string(inputSchema), `"`+property+`"`)
				}
				require.NotContains(t, string(inputSchema), "Reentered")
				schemaBytes, err := json.Marshal(tool.Metadata.OutputSchema)
				require.NoError(t, err)
				require.Contains(t, string(schemaBytes), `"`+format.metric+`"`)
				require.Contains(t, string(schemaBytes), `"Exact_Name"`)
				require.NotContains(t, string(schemaBytes), `"Secret"`)
				for _, tc := range []struct {
					name string
					args map[string]any
					want string
					bad  bool
				}{
					{"all selectors offset", map[string]any{"fields": []string{format.metric, "Exact_Name"}, format.order: format.metric + " DESC", "criteria": format.metric + " >= 20", "limit": 1, "offset": 1}, fmt.Sprintf(`{"data":[{"%s":20,"Exact_Name":"two"}]}`, format.metric), false},
					{"page", map[string]any{"fields": []string{format.metric}, format.order: format.metric + " DESC", "limit": 1, "page": 2}, fmt.Sprintf(`{"data":[{"%s":20}]}`, format.metric), false},
					{"hidden projection", map[string]any{"fields": []string{"secret"}}, "", true},
					{"denied ordering", map[string]any{format.order: "Exact_Name DESC"}, "", true},
					{"denied filtering", map[string]any{"criteria": "id = 1"}, "", true},
				} {
					t.Run(format.name+"/"+tc.name, func(t *testing.T) {
						result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: format.name, Arguments: tc.args}})
						// lc numeric names intentionally differ from native empty-format names;
						// the isolated unmapped/reentrant call must not acquire that mapping.
						bad := tc.bad || mode == "reentrant" || mode != "explicit" && (format.name == "DelegatingCamel" || tc.name == "all selectors offset")
						if bad {
							if mode == "reentrant" {
								require.ErrorContains(t, invocationFailure, "component dependency cycle")
							} else if !tc.bad {
								require.Error(t, invocationFailure, "unmapped selectors must not acquire parent naming authority")
							}
							require.True(t, rpcErr != nil || result != nil && result.IsError != nil && *result.IsError)
							return
						}
						require.Nil(t, rpcErr)
						body, err := json.Marshal(result.StructuredContent)
						require.NoError(t, err)
						require.JSONEq(t, tc.want, string(body))
						validation, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(schemaBytes), gojsonschema.NewBytesLoader(body))
						require.NoError(t, err)
						require.True(t, validation.Valid(), "%v", validation.Errors())
					})
				}
			}
			// Ordinary HTTP uses its original parent presentation and native selector.
			recorder := httptest.NewRecorder()
			gateway.NewHandler(runtime, nil, "delegation").ServeHTTP(recorder, httptest.NewRequest("GET", "/delegating-parent?fields=SampleSeen_1Day&sort=SampleSeen_1Day%20DESC&limit=1", nil))
			if mode == "reentrant" {
				require.Equal(t, 500, recorder.Code)
				require.ErrorContains(t, invocationFailure, "component dependency cycle")
				return
			}
			require.Equal(t, 200, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), `"Data"`)
			require.NotContains(t, recorder.Body.String(), `"sampleSeen1Day"`)
		})
	}
}
