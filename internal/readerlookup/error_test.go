package readerlookup

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/xdatly/response"
)

func TestClassificationFallbackAuthority(t *testing.T) {
	for _, exists := range []bool{false, true} {
		err := EmptyResult(exists)
		code := http.StatusNotFound
		if exists {
			code = http.StatusForbidden
		}
		require.Equal(t, code, response.ErrorStatusCode(err, 500))
		_, explicit := response.ErrorBody(err)
		require.False(t, explicit)
		body, ok := Fallback(err, code)
		require.True(t, ok)
		require.Equal(t, response.Status{Status: "error", Message: http.StatusText(code)}, body)
		for _, other := range []error{nil, errors.New(err.Error()), fmt.Errorf("wrapped: %w", err), errors.Join(err, errors.New("private failure")), &response.Error{Code: code, Payload: map[string]any{"owned": true}, Cause: err}} {
			_, ok := Fallback(other, code)
			require.False(t, ok)
		}
		_, ok = Fallback(err, 500)
		require.False(t, ok)
	}
}
