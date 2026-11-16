package mcp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	selectorModel "github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp/invocation"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
	"github.com/viant/xdatly"
	xcontract "github.com/viant/xdatly/contract"
	"github.com/viant/xdatly/handler"
	legacyresponse "github.com/viant/xdatly/handler/response"
	xreader "github.com/viant/xdatly/reader"
	"github.com/xuri/excelize/v2"
)

type convertedRootInput struct {
	Fields []string `parameter:"Fields,kind=query,in=fields" querySelector:"view=records,property=fields"`
	Limit  int      `parameter:"Limit,kind=query,in=limit" querySelector:"view=records,property=limit"`
	Page   int      `parameter:"Page,kind=query,in=page" querySelector:"view=records,property=page"`
}
type convertedRootSQLRow struct {
	Internal string `sqlx:"internal"`
	ID       int    `sqlx:"id"`
	Value    string `sqlx:"value"`
}
type convertedRootPublicRow struct {
	Computed     string `json:"computed" mcp:"name=display"`
	Label        string `json:"label"`
	ID           int    `json:"id"`
	Secret       string `json:"secret" mcp:"-"`
	Hidden       string `json:"-"`
	Internal     string `internal:"true"`
	Present      *bool  `json:"present"`
	Zero         int    `json:"zero"`
	FormatHidden string `format:"ignore=true"`
	BackingState string `json:"-" choice:"READY" choice:"PENDING" enumAccept:"literal" enumStyle:"literal"`
}
type convertedRootOutput struct {
	legacyresponse.Status
	Rows []*convertedRootSQLRow    `json:"-"`
	Data []*convertedRootPublicRow `json:"data"`
}

type convertedRootDetailInput struct{}
type convertedRootDetailSelectors struct {
	Fields []string `parameter:"Fields,kind=query,in=_fields,required=false" querySelector:"view=detail,property=fields"`
}
type convertedRootDetailComponent struct {
	Contract xdatly.Component[convertedRootDetailInput, convertedRootDetailOutput] `component:"ConvertedDetail,path=/converted/detail,method=GET"`
}

func (convertedRootDetailComponent) DatlyQuerySelectors() convertedRootDetailSelectors {
	panic("metadata only")
}

type convertedRootDetailOutput struct {
	Rows *convertedRootSQLRow    `json:"-"`
	Data *convertedRootPublicRow `json:"data"`
}

func (o *convertedRootDetailOutput) Finalize(context.Context, handler.ComponentInvoker) error {
	if o.Rows == nil || o.Rows.Internal != "backing" || o.Rows.Value == "" {
		return fmt.Errorf("detail Finalize received incomplete internal row")
	}
	present := false
	o.Data = &convertedRootPublicRow{ID: o.Rows.ID, Label: o.Rows.Value, Computed: o.Rows.Value + "!", Secret: "secret", Present: &present}
	return nil
}

var convertedRootContractCalls atomic.Int64

func (*convertedRootOutput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	convertedRootContractCalls.Add(1)
	var state struct {
		Mode string `parameter:",kind=header,in=X-Converted-Policy"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	if state.Mode == "" {
		return nil, nil
	}
	return &xcontract.Policy{Name: "converted:" + state.Mode, NoCache: state.Mode == "nocache", Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[convertedRootPublicRow]() && field.Name == "BackingState" && state.Mode == "reveal" {
			return field.SetTag("json", "publicState").SetMCPName("publicState"), nil
		}
		if parent == reflect.TypeFor[convertedRootPublicRow]() && field.Name == "Computed" {
			if state.Mode == "hide" {
				return field.Hide(), nil
			}
			if state.Mode == "rename" {
				return field.SetTag("mcp", "name=alternate"), nil
			}
		}
		return nil, nil
	}}, nil
}

func (o *convertedRootOutput) Finalize(context.Context, handler.ComponentInvoker) error {
	o.Extras = map[string]any{"data": "must be overwritten by typed Data"}
	o.Data = make([]*convertedRootPublicRow, 0, len(o.Rows))
	for _, row := range o.Rows {
		if row.Internal != "backing" || row.Value == "" {
			return fmt.Errorf("Finalize received incomplete internal row")
		}
		present := false
		o.Data = append(o.Data, &convertedRootPublicRow{BackingState: "READY", ID: row.ID, Label: row.Value, Computed: row.Value + "!", Secret: "secret", Hidden: "hidden", Internal: "private", Present: &present})
	}
	return nil
}

func convertedRootExpectedWithStatus(expected string) string { return `{"status":"ok",` + expected[1:] }

func TestConvertedOutputRootMCPAndNativeFormatsSQLite(t *testing.T) {
	ctx := t.Context()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,value TEXT,internal TEXT,tenant INTEGER)", "INSERT INTO records VALUES(1,'one','backing',1),(2,'two','backing',1),(3,'denied','backing',2)"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ConvertedRoot"}, Routes: []*spec.Route{{Method: "GET", Path: "/converted", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "converted"}}}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}}}, RootView: &spec.View{Name: "records", Selector: &spec.Selector{AllowFields: true, AllowLimit: true, AllowOffset: true}, Source: &spec.ViewSource{SQL: "SELECT id,value,internal FROM records WHERE tenant=1 ORDER BY id"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[convertedRootInput](), OutputType: reflect.TypeFor[convertedRootOutput](), DirectViewField: "Rows"})
	require.NoError(t, err)
	var queries atomic.Int64
	execution, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeFor[convertedRootInput](), OutputType: reflect.TypeFor[convertedRootOutput](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	guarded := execution.WithQueryGuard(func(context.Context, *sql.DB, *xreader.ParameterizedQuery) error { queries.Add(1); return nil })
	registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: guarded})
	require.NoError(t, err)
	detailComponent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ConvertedDetail"}, Routes: []*spec.Route{{Method: "GET", Path: "/converted/detail", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "converted.detail"}}}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}}}, RootView: &spec.View{Name: "detail", Cardinality: spec.CardinalityOne, Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: "SELECT id,value,internal FROM records WHERE tenant=1 AND id=2"}}}
	detailComponent.Name = "ConvertedDetail"
	detailComponent.Key.Scope = reflect.TypeFor[convertedRootDetailComponent]().PkgPath()
	detailComponent.SelectorCompanion, err = selectorModel.New(x.NewType(reflect.TypeFor[convertedRootDetailComponent]()), "Contract", x.NewType(reflect.TypeFor[convertedRootDetailSelectors]()))
	require.NoError(t, err)
	detailArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: detailComponent, InputType: reflect.TypeFor[convertedRootDetailInput](), OutputType: reflect.TypeFor[convertedRootDetailOutput](), DirectViewField: "Rows"})
	require.NoError(t, err)
	detailReader, err := detailArtifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	detail, err := detailArtifact.Registration(druntime.RegisteredComponent{Reader: detailReader})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered, detail})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	start := time.Now()
	service, err := New(Config{Components: []*druntime.RegisteredComponent{registered, detail}, Invoker: rt})
	require.NoError(t, err)
	native := mcpclient.New(t, service, schema.LatestProtocolVersion)
	_, err = native.ListTools(ctx, nil)
	require.NoError(t, err)
	require.Zero(t, queries.Load(), "discovery performs zero business reads")
	t.Logf("cold discovery=%s business_queries=%d", time.Since(start), queries.Load())
	entry, exists := service.Registry().ToolRegistry.Get("converted")
	require.True(t, exists)
	for _, mode := range []string{"show", "hide", "rename", "nocache", "show", "nocache", "reveal"} {
		policyContext := invocation.WithHTTPHeaders(ctx, http.Header{"X-Converted-Policy": {mode}})
		_, _, err := service.DescribeTool(policyContext, entry.Metadata)
		require.NoError(t, err)
		name := "display"
		expected := `{"data":[{"display":"two!"}]}`
		if mode == "reveal" {
			name = "id"
			expected = `{"data":[{"id":2}]}`
		}
		if mode == "rename" {
			name = "alternate"
			expected = `{"data":[{"alternate":"two!"}]}`
		}
		before := convertedRootContractCalls.Load()
		reads := queries.Load()
		result, rpcErr := entry.Handler(policyContext, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "converted", Arguments: map[string]any{"fields": []string{name}, "limit": 1, "page": 2}}})
		require.Nil(t, rpcErr)
		require.EqualValues(t, 1, convertedRootContractCalls.Load()-before, "selection and encoding reuse one published Contract decision")
		if mode == "hide" {
			require.NotNil(t, result.IsError)
			require.True(t, *result.IsError)
			require.Equal(t, reads, queries.Load())
			continue
		}
		require.True(t, result.IsError == nil || !*result.IsError)
		encoded, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		require.JSONEq(t, convertedRootExpectedWithStatus(expected), string(encoded))
		require.EqualValues(t, 1, queries.Load()-reads)
	}
	for _, tc := range []struct {
		name     string
		fields   any
		expected string
		bad      bool
	}{
		{"id", []string{"id"}, `{"data":[{"id":2}]}`, false},
		{"zero", []string{"zero"}, `{"data":[{"zero":0}]}`, false},
		{"computed", []string{"display"}, `{"data":[{"display":"two!"}]}`, false},
		{"public indexes", []string{"id", "display", "present"}, `{"data":[{"id":2,"display":"two!","present":false}]}`, false},
		{"empty", []string{}, `{"data":[{"id":2,"display":"two!","label":"two","present":false,"zero":0}]}`, false},
		{"omitted", nil, `{"data":[{"id":2,"display":"two!","label":"two","present":false,"zero":0}]}`, false},
		{"hidden", []string{"secret"}, "", true},
		{"json hidden", []string{"Hidden"}, "", true},
		{"internal", []string{"Internal"}, "", true},
		{"format ignored", []string{"formatHidden"}, "", true},
		{"sql only", []string{"value"}, "", true},
		{"nested", []string{"Data.ID"}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := map[string]any{"limit": 1, "page": 2}
			if tc.fields != nil {
				args["fields"] = tc.fields
			}
			before := queries.Load()
			started := time.Now()
			result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "converted", Arguments: args})
			require.NoError(t, err)
			if tc.bad {
				require.NotNil(t, result.IsError)
				require.True(t, *result.IsError)
				require.Equal(t, before, queries.Load())
				return
			}
			require.True(t, result.IsError == nil || !*result.IsError)
			encoded, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, convertedRootExpectedWithStatus(tc.expected), string(encoded))
			require.EqualValues(t, 1, queries.Load()-before)
			t.Logf("warm native call=%s queries=%d", time.Since(started), queries.Load()-before)
		})
	}
	empty, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "converted", Arguments: map[string]any{"fields": []string{"id"}, "limit": 1, "page": 100}})
	require.NoError(t, err)
	encodedEmpty, err := json.Marshal(empty.StructuredContent)
	require.NoError(t, err)
	require.JSONEq(t, convertedRootExpectedWithStatus(`{"data":[]}`), string(encodedEmpty))
	detailResult, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "converted.detail"})
	require.NoError(t, err)
	require.True(t, detailResult.IsError == nil || !*detailResult.IsError)
	detailJSON, err := json.Marshal(detailResult.StructuredContent)
	require.NoError(t, err)
	require.JSONEq(t, `{"data":{"display":"two!","label":"two","id":2,"present":false,"zero":0}}`, string(detailJSON))
	detailSelected, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "converted.detail", Arguments: map[string]any{"fields": []string{"id"}}})
	require.NoError(t, err)
	require.True(t, detailSelected.IsError == nil || !*detailSelected.IsError)
	detailSelectedJSON, err := json.Marshal(detailSelected.StructuredContent)
	require.NoError(t, err)
	require.JSONEq(t, string(detailJSON), string(detailSelectedJSON), "singleton's existing Fields conversion gap remains unchanged")

	// The REST plan remains the original SQL-root plan and compatibility mode.
	rec := httptest.NewRecorder()
	gateway.NewHandler(rt, nil, "").ServeHTTP(rec, httptest.NewRequest("GET", "/converted?limit=1&page=2", nil))
	require.Equal(t, 200, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"secret":"secret"`)
	require.Contains(t, rec.Body.String(), `"computed":"two!"`)

	plan, err := registered.Output.WithMCPOutputRoot()
	require.NoError(t, err)
	plan, err = plan.WithCaseFormat("lc")
	require.NoError(t, err)
	plan, err = plan.WithMCPVisibility()
	require.NoError(t, err)
	target := exec.ComponentTarget{Component: registered.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/converted"}}
	presentation := exec.NewSelectorPresentation(target, func(typ reflect.Type, path string) ([]exec.SelectorFieldName, error) {
		fields, err := plan.SelectorFieldNames(typ, path)
		result := make([]exec.SelectorFieldName, len(fields))
		for i, field := range fields {
			result[i] = exec.SelectorFieldName{GoName: field.GoName, PublicName: field.PublicName, Index: field.Index}
		}
		return result, err
	}).WithFieldSelection(spec.FieldSelectionOutputOnly).WithOutputRoot(plan.SelectorOutputRoot())
	call := exec.CaptureOutputSelection(ctx)
	scope, err := requestprovider.New(httptest.NewRequest("GET", "/converted?fields=id&limit=1&page=2", nil))
	require.NoError(t, err)
	defer scope.Close()
	value, err := rt.InvokeComponent(call, exec.ComponentRequest{Target: target, Providers: scope.Providers(), SelectorPresentation: presentation})
	require.NoError(t, err)
	out := value.(*convertedRootOutput)
	require.Equal(t, "backing", out.Rows[0].Internal)
	require.Equal(t, "two!", out.Data[0].Computed)
	for _, format := range []string{"json", "csv", "xlsx"} {
		result, err := plan.Encode(call, format, value)
		require.NoError(t, err)
		switch format {
		case "json":
			require.JSONEq(t, convertedRootExpectedWithStatus(`{"data":[{"id":2}]}`), string(result.Data))
		case "csv":
			records, err := csv.NewReader(bytes.NewReader(result.Data)).ReadAll()
			require.NoError(t, err)
			require.Equal(t, [][]string{{"ID"}, {"2"}}, records)
		case "xlsx":
			book, err := excelize.OpenReader(bytes.NewReader(result.Data))
			require.NoError(t, err)
			defer book.Close()
			require.Contains(t, book.GetSheetList(), "Data")
			records, err := book.GetRows("Data")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"ID"}, {"2"}}, records)
		}
	}
	require.Equal(t, "two!", out.Data[0].Computed, "all formats leave application values complete")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "converted", Arguments: map[string]any{"fields": []string{"id"}, "limit": 1, "page": 2}})
			require.NoError(t, err)
			encoded, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, convertedRootExpectedWithStatus(`{"data":[{"id":2}]}`), string(encoded))
		}()
	}
	wg.Wait()
}
