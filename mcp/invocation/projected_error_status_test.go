package invocation

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/output"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

type projectedStatusEnvelope struct {
	legacy.Status
	Data     []int `json:"data"`
	Zero     int   `json:"zero"`
	Disabled bool  `json:"disabled"`
}
type projectedStatusInvoker struct{ failure error }

func (i projectedStatusInvoker) InvokeComponent(context.Context, exec.ComponentRequest) (any, error) {
	return nil, i.failure
}

func TestProjectedLegacyErrorPreservesTypedStatusAndCompiledPublicBody(t *testing.T) {
	plan, err := (output.Compiler{}).Compile(output.CompileInput{Type: reflect.TypeFor[projectedStatusEnvelope](), Component: &spec.Component{Parameters: []*spec.Parameter{{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}}, Settings: &spec.Settings{Output: &spec.OutputSettings{NilSlicePolicy: "empty_array"}}}})
	require.NoError(t, err)
	for _, status := range []int{403, 422, 500} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			private := errors.New("private storage cause")
			cause := &legacy.Error{Code: status, Message: "access denied", Err: private}
			_, explicit := modern.ErrorBody(cause)
			require.False(t, explicit, "the nominal error must exercise declared projection, not explicit ErrorBody")
			calls := 0
			var projected []byte
			invoker := New(Config{Invoker: projectedStatusInvoker{cause}, DeclaredError: func(exec.ComponentTarget) ErrorProjector {
				return func(ctx context.Context, code int, failure error) ([]byte, bool, error) {
					calls++
					require.Equal(t, status, code)
					require.Same(t, cause, failure)
					body, applies, err := plan.ErrorEnvelopeWithCause(exec.ErrorMessage(failure, code), code, failure)
					if err != nil || !applies {
						return nil, applies, err
					}
					encoded, err := plan.Encode(ctx, "json", body)
					projected = append([]byte(nil), encoded.Data...)
					return encoded.Data, true, err
				}
			}})
			execution, rpcErr := invoker.Execute(t.Context(), Request{Target: exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/fixture", Name: "read"}, Route: spec.RouteRef{Method: "GET", Path: "/rows"}}, Method: "tools/call", URI: "/rows"})
			require.Nil(t, rpcErr)
			require.Equal(t, status, execution.StatusCode())
			result := execution.ToolResult()
			require.Equal(t, 1, calls)
			require.True(t, *result.IsError)
			baseline := textToolResult(projected, result.IsError)
			require.Equal(t, baseline.Content, result.Content, "the compiled public text bytes must remain identical")
			require.Equal(t, baseline.StructuredContent, result.StructuredContent, "numeric/zero/false/nil-slice projection must retain existing conversion")
			raw, err := json.Marshal(result)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "private storage cause")
			code, found := schema.ToolErrorHTTPStatus(result)
			require.True(t, found)
			require.Equal(t, status, code)
			require.Same(t, cause, execution.Error())
			require.Same(t, private, cause.Err)
			require.Nil(t, cause.Object)
			require.Empty(t, cause.View)
			require.Empty(t, cause.Parameter)
		})
	}
}
