package http

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/capturederror"
	dr "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
)

func TestCapturedNativeWriterHTTPFinalizerBodyPolicy(t *testing.T) {
	for _, remap := range []bool{false, true} {
		t.Run(map[bool]string{false: "original", true: "fresh finalizer body"}[remap], func(t *testing.T) {
			registered, h, err := capturederror.New(t, remap)
			require.NoError(t, err)
			runtime, err := dr.NewRuntime([]*registry.RegisteredComponent{registered})
			require.NoError(t, err)
			request := httptest.NewRequest("PATCH", "/captured-error", strings.NewReader(`{"data":[{"id":7}]}`))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			NewHandler(runtime, nil, "test").ServeHTTP(recorder, request)
			status, body := 400, `{"data":[{"id":7}],"status":"business"}`
			if remap {
				status, body = 409, `{"data":[{"id":7}],"status":"finalized"}`
			}
			require.Equal(t, status, recorder.Code, recorder.Body.String())
			require.JSONEq(t, body, recorder.Body.String())
			h.AssertState(t)
			evidence := h.Observe()
			require.Equal(t, 1, evidence.Captures)
			require.Equal(t, 0, evidence.Executes)
			require.Equal(t, 1, evidence.Bridges)
			require.Equal(t, 1, evidence.Finalizes)
			require.Same(t, evidence.Canonical, evidence.Finalized)
			require.NotSame(t, evidence.Payload, evidence.Canonical)
			require.Same(t, evidence.Trusted, evidence.Canonical.Logger)
			require.NotSame(t, evidence.Trusted, evidence.Payload.Logger)
			require.Equal(t, "business", evidence.Payload.Status)
			require.Equal(t, "finalized", evidence.Canonical.Status)
			require.Same(t, evidence.Payload.Data[0], evidence.Canonical.Data[0])
			require.True(t, errors.Is(evidence.Err, capturederror.BusinessCause))
			require.NotContains(t, recorder.Body.String(), "PRIVATE")
			require.NotContains(t, recorder.Body.String(), "Logger")
		})
	}
}
