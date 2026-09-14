package invocation

import (
	"errors"
	"fmt"
	"testing"

	"github.com/viant/datly/internal/testharness"
	xexec "github.com/viant/xdatly/exec"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

func TestExplicitToolErrorBodyAndStatus(t *testing.T) {
	body := map[string]any{"message": "", "error": nil, "violations": []any{map[string]any{"field": "Name", "message": "denied"}}}
	execution := &Execution{value: map[string]any{"secret": "discard"}, err: fmt.Errorf("internal wrapper: %w", &response.Error{Code: 401, Payload: body, Cause: errors.New("private DB detail")}), context: &xexec.Context{StatusCode: 500}}
	result := execution.ToolResult()
	if result.IsError == nil || !*result.IsError || execution.StatusCode() != 401 {
		t.Fatalf("result=%+v", result)
	}
	actual := testharness.StructuredObject(t, result.StructuredContent)
	if message, present := actual["message"]; !present || message != "" {
		t.Fatal("explicit empty message changed")
	}
	if value, present := actual["error"]; !present || value != nil {
		t.Fatal("explicit null error changed")
	}
	if _, present := actual["secret"]; present {
		t.Fatal("success/internal payload leaked")
	}
	validation := &xhandler.Validation{Code: 401, Violations: []*xhandler.Violation{{Field: "Name", Message: "denied"}}}
	execution = &Execution{err: fmt.Errorf("validate: %w", validation.Err())}
	result = execution.ToolResult()
	if execution.StatusCode() != 401 || testharness.StructuredObject(t, result.StructuredContent)["violations"] == nil {
		t.Fatal("typed validation was flattened")
	}
}
