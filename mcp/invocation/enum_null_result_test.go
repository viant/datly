package invocation

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/exec"
	"github.com/viant/tagly/choice"
	"github.com/viant/xdatly/response"
)

func TestEnumNullFallbackRetainsErrorPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		err     error
		message string
	}{
		{"safe reason", 400, fmt.Errorf("PRIVATE: %w", choice.ErrNullInput), choice.ErrNullInput.Error()},
		{"server failure", 500, fmt.Errorf("PRIVATE: %w", choice.ErrNullInput), "Internal Server Error"},
		{"authored binding", 400, &bindly.BindingError{Code: 400, Message: "authored message", Cause: choice.ErrNullInput}, "authored message"},
		{"canceled", 400, errors.Join(choice.ErrNullInput, context.Canceled), ""},
		{"deadline", 400, errors.Join(choice.ErrNullInput, context.DeadlineExceeded), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.message
			if want == "" {
				want = exec.ErrorMessage(tc.err, tc.status)
			}
			result := errorResult(tc.status, tc.err)
			require.Equal(t, want, result.StructuredContent.(map[string]any)["message"])
			require.ErrorIs(t, tc.err, choice.ErrNullInput)
		})
	}
	explicit := &response.Error{Code: 400, Payload: map[string]any{"message": "explicit body"}, Cause: choice.ErrNullInput}
	execution := &Execution{err: explicit}
	result := execution.ToolResult()
	require.Equal(t, "explicit body", result.StructuredContent.(map[string]any)["message"])
	require.Same(t, explicit, execution.Error())
	original := &bindly.BindingError{Code: 400, Cause: choice.ErrNullInput}
	execution = &Execution{err: original, encodingContext: t.Context(), projectError: func(_ context.Context, _ int, err error) ([]byte, bool, error) {
		require.Same(t, original, err)
		return []byte(`{"message":"declared envelope"}`), true, nil
	}}
	result = execution.ToolResult()
	require.Equal(t, "declared envelope", result.StructuredContent.(map[string]any)["message"])
	require.Same(t, original, execution.Error())
}
