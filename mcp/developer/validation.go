package developer

import (
	"context"
	"encoding/json"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

func (s *Service) validate(ctx context.Context, request *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
	if request == nil || request.Params.Name != ValidationTool {
		name := ""
		if request != nil {
			name = request.Params.Name
		}
		return nil, schema.NewUnknownTool(name)
	}
	if ctx == nil {
		return nil, jsonrpc.NewInternalError("developer validation context is required", nil)
	}
	// Restrict argument authority at execution as well as declaring the supported
	// property in the tool schema. No caller-supplied authority reaches Validator.
	if len(request.Params.Arguments) != 1 {
		return nil, jsonrpc.NewInvalidParamsError("validation accepts only a configured target", nil)
	}
	name, ok := request.Params.Arguments["target"].(string)
	if !ok {
		return nil, jsonrpc.NewInvalidParamsError("validation target must be a string", nil)
	}
	validator, ok := s.targets[name]
	if !ok {
		return nil, jsonrpc.NewInvalidParamsError("unknown validation target", nil)
	}
	if ctx.Err() == nil {
		operation, finish, err := s.operation(ctx)
		if err != nil {
			return toolResult(nil, err)
		}
		defer finish()
		ctx = operation
	}
	report, validationErr := validator.Validate(ctx)
	if report == nil {
		return nil, jsonrpc.NewInternalError("validation returned no report", nil)
	}
	payload, err := json.Marshal(report)
	if err != nil {
		return nil, jsonrpc.NewInternalError("encode validation report", nil)
	}
	failed := validationErr != nil || !report.Valid
	return &schema.CallToolResult{
		Content:           []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: string(payload)}},
		StructuredContent: report, IsError: &failed, ResultType: schema.ResultTypeComplete,
	}, nil
}
