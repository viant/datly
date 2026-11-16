package report

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/mcp/invocation"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	xcontract "github.com/viant/xdatly/contract"
)

type sourcePolicyAuthority struct{ Show bool }
type sourcePolicyAuthorityInput struct {
	Token  string `parameter:",kind=header,in=Authorization,required=true"`
	Region string `parameter:",kind=header,in=X-Region"`
}
type sourceAuthPolicyRow groupedSpendRow
type sourceAuthPolicyOutput struct{ Rows []*sourceAuthPolicyRow }

var sourcePolicyDependencyError = errors.New("source policy dependency unavailable")

func (*sourceAuthPolicyRow) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct {
		Auth *sourcePolicyAuthority `parameter:",kind=component,in=GET:/source-policy-auth,required=true"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	if state.Auth == nil {
		return nil, sourcePolicyDependencyError
	}
	return &xcontract.Policy{Name: fmt.Sprintf("source-authority:%t", state.Auth.Show), Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if !state.Auth.Show && parent == reflect.TypeFor[sourceAuthPolicyRow]() && field.Name == "TotalSpend" {
			return field.SetTag("mcp", "-"), nil
		}
		return nil, nil
	}}, nil
}
func sourceAuthContractHarness(t *testing.T) (*groupedReportHarness, *atomic.Int64) {
	t.Helper()
	h := (groupedReportHarnessConfig{output: reflect.TypeFor[sourceAuthPolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} }}}).build(t)
	auth, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "SourcePolicyAuthority"}, Routes: []*spec.Route{{Method: "GET", Path: "/source-policy-auth"}}}, InputType: reflect.TypeFor[sourcePolicyAuthorityInput](), OutputType: reflect.TypeFor[sourcePolicyAuthority]()})
	require.NoError(t, err)
	calls := &atomic.Int64{}
	registered, err := auth.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc[sourcePolicyAuthorityInput, sourcePolicyAuthority](func(ctx context.Context, input *sourcePolicyAuthorityInput) (*sourcePolicyAuthority, error) {
		calls.Add(1)
		if strings.TrimPrefix(input.Token, "Bearer ") == "unavailable" {
			return nil, &bindly.BindingError{Path: "SourceAuthority", Code: 503, Cause: sourcePolicyDependencyError}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return &sourcePolicyAuthority{Show: strings.TrimPrefix(input.Token, "Bearer ") == "allowed" && input.Region != "US"}, nil
	})})
	require.NoError(t, err)
	entries := append(append([]*druntime.RegisteredComponent(nil), h.registered...), registered)
	rt, err := druntime.NewRuntime(entries)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	service, err := mcp.New(mcp.Config{Components: entries, Invoker: rt})
	require.NoError(t, err)
	h.runtime, h.service = rt, service
	return h, calls
}
func TestSourceContractDependencyScopeLateFrameAndRevocation(t *testing.T) {
	h, calls := sourceAuthContractHarness(t)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	require.True(t, ok)
	headers := http.Header{"Authorization": {"allowed"}, "X-Region": {"EU"}}
	ctx := invocation.WithHTTPHeaders(context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: "allowed"}), headers)
	filters := map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}
	late := map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "US", "channel": "web", "status": "active"}
	args := map[string]any{"cubes": []any{map[string]any{"filters": filters}, map[string]any{"filters": late}}, "sql": "SELECT t1.AccountID AS key,NULL AS harmless FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.AccountID AS key,t2.TotalSpend AS harmless FROM $CubeSQL2 AS t2 LIMIT 5"}
	before := calls.Load()
	result, protocolErr := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args}})
	require.Nil(t, protocolErr)
	require.True(t, result.IsError != nil && *result.IsError)
	require.Zero(t, h.codec.calls.Load(), "forbidden second frame must prevent first query preparation")
	t.Logf("late frame result=%+v auth=%d", result, calls.Load()-before)
	require.EqualValues(t, 2, calls.Load()-before, "equivalent initial/EU scope reuses auth; distinct US scope rebinds it")
	cube, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	for _, role := range []string{"allowed", "denied", "allowed", "denied"} {
		ctx := invocation.WithHTTPHeaders(context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: role}), http.Header{"X-Region": {"EU"}})
		metadata, _, err := h.service.DescribeTool(ctx, cube.Metadata)
		require.NoError(t, err)
		raw, err := json.Marshal(metadata.InputSchema)
		require.NoError(t, err)
		require.Equal(t, role == "allowed", strings.Contains(string(raw), "totalSpend"))
	}
}
func TestSourceContractDependencyErrorsAndCanceledDiscovery(t *testing.T) {
	h, _ := sourceAuthContractHarness(t)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	ctx := invocation.WithHTTPHeaders(context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: "unavailable"}), http.Header{"X-Region": {"EU"}})
	_, _, err := h.service.DescribeTool(ctx, entry.Metadata)
	require.ErrorIs(t, err, sourcePolicyDependencyError)
	result, protocolErr := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: map[string]any{}}})
	require.Nil(t, protocolErr)
	require.True(t, result.IsError != nil && *result.IsError)
	encoded, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, json.Unmarshal(encoded, &body))
	require.EqualValues(t, 503, body["status"])
	require.NotContains(t, string(encoded), sourcePolicyDependencyError.Error())
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = h.service.DescribeTool(canceled, entry.Metadata)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, h.codec.calls.Load())
}
func TestSourceContractConcurrentWarmCallersAndFullPathTiming(t *testing.T) {
	h, calls := sourceAuthContractHarness(t)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	args := groupedReportRequest{Dimensions: []string{"Tenant", "AccountID"}, Measures: []string{"OrderCount"}, Filters: groupedReportFilters{AccountIDs: "1,2", Tenant: "acme", Region: "EU", Channel: "web", Status: "active"}}.mcpArguments()
	durations := make([]time.Duration, 12)
	before := calls.Load()
	for i := range durations {
		ctx := invocation.WithHTTPHeaders(context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: "allowed"}), http.Header{"X-Region": {"EU"}})
		started := time.Now()
		result, e := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: args}})
		durations[i] = time.Since(started)
		require.Nil(t, e)
		require.False(t, result.IsError != nil && *result.IsError)
	}
	require.EqualValues(t, len(durations), h.codec.calls.Load())
	require.EqualValues(t, len(durations), calls.Load()-before, "one fresh auth dependency per equivalent operation")
	var total time.Duration
	for _, d := range durations[1:] {
		total += d
	}
	t.Logf("complete native tool invocation cold=%s warm_mean=%s auth=%d source_preparations=%d", durations[0], total/time.Duration(len(durations)-1), calls.Load()-before, h.codec.calls.Load())
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			role := "allowed"
			if i%2 != 0 {
				role = "denied"
			}
			ctx := invocation.WithHTTPHeaders(context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: role}), http.Header{"X-Region": {"EU"}})
			selected, _, err := h.service.DescribeTool(ctx, entry.Metadata)
			if err != nil {
				t.Error(err)
				return
			}
			raw, _ := json.Marshal(selected.InputSchema)
			if strings.Contains(string(raw), "totalSpend") != (role == "allowed") {
				t.Error("caller metadata contamination")
			}
		}(i)
	}
	group.Wait()

}
