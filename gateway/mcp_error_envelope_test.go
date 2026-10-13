package gateway

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/router/proxy"
)

func TestRouterBuildToolCallResultMarksDatlyErrorEnvelope(t *testing.T) {
	writer := proxy.NewWriter()
	writer.Code = http.StatusOK
	writer.HeaderMap.Set("Content-Type", "application/json")
	_, err := writer.Body.Write([]byte(`{"status":"error","message":"context canceled","errors":["context canceled"],"data":[]}`))
	require.NoError(t, err)

	result := (&Router{}).buildToolCallResult(writer, "http://localhost/test", http.MethodPost)
	require.NotNil(t, result)
	require.NotNil(t, result.IsError)
	assert.True(t, *result.IsError)
	assert.Equal(t, map[string]interface{}{
		"status":  "error",
		"message": "context canceled",
		"errors":  []interface{}{"context canceled"},
		"data":    []interface{}{},
	}, result.StructuredContent)
}

func TestRouterBuildToolCallResultKeepsDatlySuccessEnvelopeSuccessful(t *testing.T) {
	writer := proxy.NewWriter()
	writer.Code = http.StatusOK
	writer.HeaderMap.Set("Content-Type", "application/json")
	_, err := writer.Body.Write([]byte(`{"status":"ok","data":[]}`))
	require.NoError(t, err)

	result := (&Router{}).buildToolCallResult(writer, "http://localhost/test", http.MethodPost)
	require.NotNil(t, result)
	assert.Nil(t, result.IsError)
}
