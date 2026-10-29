package invocation

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/viant/datly/exec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
)

func (e *Execution) ToolResult() *schema.CallToolResult {
	status := e.StatusCode()
	if invokeErr := e.Error(); invokeErr != nil {
		if body, explicit := response.ErrorBody(invokeErr); explicit {
			payload, err := e.errorPayload(body)
			if err != nil {
				return errorResult(http.StatusInternalServerError, err)
			}
			if transport, ok := body.(response.Response); ok {
				return e.transportResult(transport, payload, true)
			}
			isError := true
			return textToolResult(payload, &isError)
		}
		// Preserve ordinary authentication failures at the tool presentation boundary.
		// Explicit public bodies above retain priority; private causes and declared
		// HTTP error envelopes must not replace the original MCP unauthorized result.
		if status == http.StatusUnauthorized {
			message := http.StatusText(status)
			isError := true
			return &schema.CallToolResult{
				Content:           []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: message}},
				IsError:           &isError,
				StructuredContent: map[string]interface{}{"error": true, "code": schema.Unauthorized, "message": message},
			}
		}
		// Project only tool presentation; resources and the original execution
		// retain their existing error semantics. Explicit public bodies win above.
		if e.projectError != nil {
			payload, applicable, err := e.projectError(e.encodingContext, status, invokeErr)
			if err != nil {
				return errorResult(http.StatusInternalServerError, err)
			}
			if applicable {
				isError := true
				return textToolResult(payload, &isError)
			}
		}
		return errorResult(status, invokeErr)
	}
	payload, err := e.Payload()
	if err != nil {
		return errorResult(http.StatusInternalServerError, fmt.Errorf("convert component result: %w", err))
	}
	if transport, ok := e.Value().(response.Response); ok {
		if status >= http.StatusBadRequest {
			return errorResult(status, errors.New(http.StatusText(status)))
		}
		return e.transportResult(transport, payload, false)
	}
	object := objectPayload(payload)
	if status >= http.StatusBadRequest {
		message := http.StatusText(status)
		if value, ok := object["message"].(string); status < http.StatusInternalServerError && ok && strings.TrimSpace(value) != "" {
			message = value
		}
		return errorResult(status, errors.New(message))
	}
	result := &schema.CallToolResult{
		Content: []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: string(payload)}},
	}
	if object != nil {
		result.StructuredContent = object
	}
	return result
}

// structuredContent is optional but, when present, must be an object. A typed
// nil map stored in its interface would serialize as null and break MCP clients.
func textToolResult(payload []byte, isError *bool) *schema.CallToolResult {
	result := &schema.CallToolResult{Content: []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: string(payload)}}, IsError: isError}
	if object := objectPayload(payload); object != nil {
		result.StructuredContent = object
	}
	return result
}

func encodePayload(result interface{}) ([]byte, error) {
	if transport, ok := result.(response.Response); ok {
		payload, err := readTransportResponse(transport)
		if err != nil {
			return nil, err
		}
		return payload, nil
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func readTransportResponse(result response.Response) ([]byte, error) {
	if result == nil {
		return nil, nil
	}
	reader := result.Body()
	if reader == nil {
		return nil, nil
	}
	if compressed, ok := result.(response.Compressed); ok {
		var closer io.ReadCloser
		var err error
		switch strings.ToLower(strings.TrimSpace(compressed.CompressionType())) {
		case "", "identity":
		case "gzip":
			closer, err = gzip.NewReader(reader)
		case "deflate", "zlib":
			closer, err = zlib.NewReader(reader)
		default:
			return nil, fmt.Errorf("unsupported response compression %q", compressed.CompressionType())
		}
		if err != nil {
			return nil, err
		}
		if closer != nil {
			defer closer.Close()
			reader = closer
		}
	}
	return io.ReadAll(reader)
}

func objectPayload(payload []byte) map[string]interface{} {
	if len(payload) == 0 {
		return nil
	}
	var result map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if decoder.Decode(&result) != nil {
		return nil
	}
	// Decode must consume one complete JSON value, as json.Unmarshal did.
	var trailing interface{}
	if decoder.Decode(&trailing) != io.EOF {
		return nil
	}
	return result
}

func errorResult(status int, err error) *schema.CallToolResult {
	if status == 0 {
		status = http.StatusInternalServerError
	}
	message := exec.ErrorMessage(err, status)
	payload := map[string]interface{}{"status": status, "error": true, "message": message}
	encoded, _ := json.Marshal(payload)
	isError := true
	return &schema.CallToolResult{
		Content: []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: string(encoded)}},
		IsError: &isError, StructuredContent: payload,
	}
}

func (e *Execution) errorPayload(body any) ([]byte, error) {
	if _, raw := body.(response.Response); raw {
		return encodePayload(body)
	}
	if e.encodeErrorOutput != nil {
		return e.encodeErrorOutput(e.encodingContext, body)
	}
	return encodePayload(body)
}
