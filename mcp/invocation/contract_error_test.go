package invocation

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly/response"
)

type contractFailureInvoker struct {
	calls   int
	failure error
}

func (i *contractFailureInvoker) InvokeComponent(context.Context, exec.ComponentRequest) (any, error) {
	i.calls++
	return nil, nil
}
func (i *contractFailureInvoker) ResolveContract(context.Context, exec.ComponentTarget, exec.ProviderScope) (*exec.EffectiveContract, error) {
	return nil, i.failure
}
func TestContractDeclaredFailureUsesToolErrorResult(t *testing.T) {
	body := map[string]any{"status": "error", "message": "credentials required", "data": []any{}}
	cause := errors.New("private verifier detail")
	backend := &contractFailureInvoker{failure: fmt.Errorf("contract: %w", &response.Error{Code: 401, Payload: body, Cause: cause})}
	invoker := New(Config{Invoker: backend})
	prepared := false
	execution, rpc := invoker.Execute(t.Context(), Request{Target: exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "Reader"}, Route: spec.RouteRef{Method: "GET", Path: "/reader"}}, Method: "GET", URI: "/reader", PrepareScope: func(*exec.EffectiveContract) (*requestprovider.Scope, error) { prepared = true; return nil, nil }})
	require.Nil(t, rpc)
	require.NotNil(t, execution)
	require.ErrorIs(t, execution.Error(), cause)
	require.Equal(t, 401, execution.StatusCode())
	result := execution.ToolResult()
	require.True(t, *result.IsError)
	require.Equal(t, body, testharness.StructuredObject(t, result.StructuredContent))
	require.Zero(t, backend.calls)
	require.False(t, prepared)
}
