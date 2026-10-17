package invocation

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
)

func TestDeclaredToolErrorProjection(t *testing.T) {
	cause := errors.New("private failure")
	for _, mode := range []string{"declared", "inapplicable", "failed", "explicit", "explicit null"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			e := &Execution{value: map[string]any{"secret": true}, err: cause}
			e.projectError = func(_ context.Context, status int, err error) ([]byte, bool, error) {
				calls++
				require.Equal(t, 500, status)
				require.Same(t, cause, err)
				if mode == "failed" {
					return nil, true, errors.New("private projector diagnostic")
				}
				return []byte(`{"status":"error","data":[]}`), mode != "inapplicable", nil
			}
			if mode == "explicit" {
				e.err = &response.Error{Code: 401, Payload: map[string]any{"public": true}, Cause: cause}
			}
			if mode == "explicit null" {
				e.err = &response.Error{Code: 401, Payload: (*struct{})(nil), Cause: cause}
			}
			original := e.Error()
			result := e.ToolResult()
			require.True(t, *result.IsError)
			require.Same(t, original, e.Error())
			require.Equal(t, map[string]any{"secret": true}, e.Value())
			text := result.Content[0].(schema.TextContent).Text
			require.NotContains(t, text, "private")
			require.NotContains(t, text, "secret")
			if mode == "explicit null" {
				require.Zero(t, calls)
				require.Equal(t, "null", text)
				return
			}
			object := testharness.StructuredObject(t, result.StructuredContent)
			switch mode {
			case "declared":
				require.Equal(t, 1, calls)
				require.JSONEq(t, `{"status":"error","data":[]}`, text)
				require.Equal(t, []any{}, object["data"])
			case "explicit":
				require.Zero(t, calls)
				require.Equal(t, true, object["public"])
			default:
				require.Equal(t, 1, calls)
				require.EqualValues(t, 500, object["status"])
			}
		})
	}
}
