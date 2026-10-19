package invocation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/mcp-protocol/schema"
	xexec "github.com/viant/xdatly/exec"
)

func TestOrdinaryUnauthorizedToolPresentation(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		for _, projected := range []bool{false, true} {
			t.Run(fmt.Sprintf("wrapped=%t/projected=%t", wrapped, projected), func(t *testing.T) {
				cause := errors.New("private credential diagnostic")
				var invokeErr error = cause
				if wrapped {
					invokeErr = fmt.Errorf("private wrapper: %w", cause)
				}
				execution := &Execution{err: invokeErr, context: &xexec.Context{StatusCode: http.StatusUnauthorized}, value: map[string]any{"private": true}}
				calls := 0
				if projected {
					execution.projectError = func(context.Context, int, error) ([]byte, bool, error) {
						calls++
						return []byte(`{"status":"error","private":true}`), true, nil
					}
				}
				isError := true
				expected := &schema.CallToolResult{
					IsError:           &isError,
					Content:           []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: http.StatusText(http.StatusUnauthorized)}},
					StructuredContent: map[string]any{"error": true, "code": schema.Unauthorized, "message": http.StatusText(http.StatusUnauthorized)},
				}
				require.Equal(t, expected, execution.ToolResult())
				require.Zero(t, calls)
				require.Same(t, invokeErr, execution.Error())
				require.Equal(t, http.StatusUnauthorized, execution.StatusCode())
				require.Equal(t, map[string]any{"private": true}, execution.Value())
			})
		}
	}
}

func TestOrdinaryForbiddenStillUsesDeclaredProjector(t *testing.T) {
	calls := 0
	execution := &Execution{err: errors.New("private forbidden cause"), context: &xexec.Context{StatusCode: http.StatusForbidden}}
	execution.projectError = func(_ context.Context, status int, _ error) ([]byte, bool, error) {
		calls++
		require.Equal(t, http.StatusForbidden, status)
		return []byte(`{"message":"public forbidden"}`), true, nil
	}
	result := execution.ToolResult()
	require.Equal(t, 1, calls)
	require.True(t, *result.IsError)
	require.Equal(t, `{"message":"public forbidden"}`, result.Content[0].(schema.TextContent).Text)
}
