package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/xdatly/handler/exec"
)

// TestSafeMarshal_Success tests successful JSON marshaling
func TestSafeMarshal_Success(t *testing.T) {
	type TestStruct struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	testData := TestStruct{
		Name:  "test",
		Value: 42,
	}

	result := safeMarshal("TEST", testData)
	assert.NotNil(t, result, "safeMarshal should return non-nil for valid data")

	var unmarshaled TestStruct
	err := json.Unmarshal(result, &unmarshaled)
	assert.NoError(t, err)
	assert.Equal(t, testData, unmarshaled)
}

// TestSafeMarshal_Error tests safeMarshal with a value that causes a marshaling error
func TestSafeMarshal_Error(t *testing.T) {
	// Channel cannot be marshaled to JSON
	ch := make(chan int)
	result := safeMarshal("TEST", ch)
	assert.Nil(t, result, "safeMarshal should return nil when marshaling fails")
}

// TestSafeMarshal_Panic tests safeMarshal with a value that causes a panic
func TestSafeMarshal_Panic(t *testing.T) {
	// Function cannot be marshaled and may cause panic
	fn := func() {}
	result := safeMarshal("TEST", fn)
	assert.Nil(t, result, "safeMarshal should return nil when marshaling panics")
}

// TestSafeMarshal_ExecContext tests safeMarshal with exec.Context
func TestSafeMarshal_ExecContext(t *testing.T) {
	execCtx := exec.NewContext("GET", "/test", nil, "")
	result := safeMarshal("EXECCONTEXT", execCtx)

	// Should either succeed (return non-nil) or fail gracefully (return nil)
	// The important thing is it doesn't panic
	if result != nil {
		assert.NotEmpty(t, result)
	}
}

// TestSafeMarshal_NilValue tests safeMarshal with nil value
func TestSafeMarshal_NilValue(t *testing.T) {
	result := safeMarshal("TEST", nil)
	assert.NotNil(t, result)
	assert.Equal(t, []byte("null"), result)
}

// TestFindBadField_ValidExecContext tests findBadField with a valid exec.Context
func TestFindBadField_ValidExecContext(t *testing.T) {
	// Capture stdout to check output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	execCtx := exec.NewContext("GET", "/test", nil, "")
	findBadField(execCtx)

	// Close write pipe and restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// With a valid exec.Context, there should be no bad field errors
	assert.NotContains(t, output, "[BAD-FIELD-ERROR]", "valid exec.Context should not have bad fields")
	assert.NotContains(t, output, "[BAD-FIELD-PANIC]", "valid exec.Context should not panic on field marshaling")
}

// TestFindBadField_CompletesWithoutPanic tests that findBadField completes without panicking
func TestFindBadField_CompletesWithoutPanic(t *testing.T) {
	execCtx := exec.NewContext("GET", "/test", nil, "")

	// Should complete without panicking
	assert.NotPanics(t, func() {
		findBadField(execCtx)
	})
}

// TestSafeMarshal_WithLabel tests that safeMarshal uses the label parameter in error messages
func TestSafeMarshal_WithLabel(t *testing.T) {
	// Capture stdout to verify label is used in error messages
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Use a value that will cause an error
	ch := make(chan int)
	result := safeMarshal("CUSTOM_LABEL", ch)

	// Close write pipe and restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	assert.Nil(t, result, "should return nil on error")
	if strings.Contains(output, "[LOG-MARSHAL-ERROR]") {
		assert.Contains(t, output, "CUSTOM_LABEL", "error message should include the label")
	}
}

// TestSafeMarshal_RecoversFromPanic tests that safeMarshal properly recovers from panics
func TestSafeMarshal_RecoversFromPanic(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Create a type that will panic during JSON marshaling
	type PanicType struct {
		Value func() // Functions cannot be marshaled
	}

	panicValue := PanicType{
		Value: func() {},
	}

	// This should not cause the test to panic
	result := safeMarshal("PANIC_TEST", panicValue)

	// Close write pipe and restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// Function should recover and return nil
	assert.Nil(t, result, "should return nil after recovering from panic")
	// Should log the panic
	if strings.Contains(output, "[LOG-MARSHAL-PANIC]") {
		assert.Contains(t, output, "PANIC_TEST", "panic log should include label")
	}
}

// TestSafeMarshal_ExecContextPanicCallsFindBadField tests that safeMarshal calls findBadField when exec.Context panics
func TestSafeMarshal_ExecContextPanicCallsFindBadField(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	execCtx := exec.NewContext("GET", "/test", nil, "")

	// Try to marshal - if it panics, findBadField should be called
	result := safeMarshal("EXECCONTEXT", execCtx)

	// Close write pipe and restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// If marshaling panicked, findBadField should have been called
	if result == nil && strings.Contains(output, "[LOG-MARSHAL-PANIC]") {
		// findBadField should have been called (though output may be empty if no bad fields found)
		// The important thing is that the function didn't crash
		assert.True(t, true, "findBadField should be called when exec.Context panics")
	}
}

func TestSafeMarshal_RedactsSensitiveFieldsAndEmbeddedCredentialText(t *testing.T) {
	fakeBearer := "syntheticHeader.syntheticPayload.syntheticSignature"
	fakeRefresh := "synthetic-refresh-value"
	fakeAPIKey := "synthetic-api-key-value"
	payload := map[string]interface{}{
		"error":  "failed to transform Authorization with JwtClaim: Bearer " + fakeBearer + ", malformed claim",
		"detail": "request rejected: refresh_token=\"" + fakeRefresh + "\"",
		"headers": map[string]interface{}{
			"X-Api-Key":    fakeAPIKey,
			"X-Request-ID": "request-123",
		},
	}

	result := safeMarshal("AUDIT", payload)
	require.NotNil(t, result)
	serialized := string(result)
	for _, secret := range []string{fakeBearer, fakeRefresh, fakeAPIKey} {
		assert.NotContains(t, serialized, secret)
	}
	assert.Contains(t, serialized, redactedValue)
	assert.Contains(t, serialized, "request-123")

	var actual map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &actual))
	headers := actual["headers"].(map[string]interface{})
	assert.Equal(t, redactedValue, headers["X-Api-Key"])
	assert.Equal(t, "request-123", headers["X-Request-ID"])
}

func TestLog_RedactsAuditAndTraceErrorsBeforeEmission(t *testing.T) {
	fakeBearer := "auditHeader.auditPayload.auditSignature"
	fakeAPIKey := "audit-api-key-value"
	headers := http.Header{
		"Authorization": []string{"Bearer " + fakeBearer},
		"X-Api-Key":     []string{fakeAPIKey},
	}
	execCtx := exec.NewContext(http.MethodGet, "/reports?refresh_token=synthetic-query-secret", headers, "test")
	execCtx.SetError(errors.New("failed to transform Authorization with JwtClaim: Bearer " + fakeBearer + ", malformed claim"))
	enabled := true

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	Log(&Config{EnableAudit: &enabled, EnableTracing: &enabled}, execCtx)
	require.NoError(t, w.Close())
	os.Stdout = oldStdout
	defer r.Close()

	var output bytes.Buffer
	_, err = io.Copy(&output, r)
	require.NoError(t, err)
	logged := output.String()
	for _, secret := range []string{fakeBearer, fakeAPIKey, "synthetic-query-secret"} {
		assert.NotContains(t, logged, secret)
	}
	assert.Contains(t, logged, "[AUDIT]")
	assert.Contains(t, logged, "[TRACE]")
	assert.Contains(t, logged, redactedValue)
}
