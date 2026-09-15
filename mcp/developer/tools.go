package developer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

type arguments struct {
	Target     string `json:"target"`
	Source     string `json:"source,omitempty"`
	Port       *int   `json:"port,omitempty"`
	Address    string `json:"address,omitempty"`
	InstanceID string `json:"instanceId,omitempty"`
	Component  string `json:"component,omitempty"`
	Prefix     string `json:"prefix,omitempty"`
}
type Transcription struct {
	Target    string   `json:"target"`
	Mode      string   `json:"mode,omitempty"`
	Operation string   `json:"operation,omitempty"`
	Language  string   `json:"language,omitempty"`
	Files     []string `json:"files"`
}

func (s *Service) toolMetadata(name string, names []string) (schema.Tool, error) {
	properties := schema.ToolInputSchemaProperties{"target": {"type": "string", "enum": names}}
	required := []string{"target"}
	description := ""
	var meta map[string]interface{}
	destructive, readOnly, openWorld := false, false, false
	var output any
	switch name {
	case ComponentsTool, InspectTool, ReverseTool:
		readOnly = true
		properties["instanceId"] = map[string]any{"type": "string"}
		if name == ComponentsTool {
			description = "List canonical current component identities and project hash or owned application revision without invoking handlers."
			properties["prefix"] = map[string]any{"type": "string"}
			output = &ComponentList{}
		} else {
			properties["component"] = map[string]any{"type": "string"}
			required = append(required, "component")
			if name == InspectTool {
				description = "Inspect canonical metadata including routes, contracts, views, selectors, cache and declared authorization. Large metadata has an immutable resource link under public/global authorization."
				output = &Inspection{}
			} else {
				description = "Read retained authored DQL, or bounded canonical reconstruction with explicit limitations. Never invokes handlers or rewrites authored files."
				output = &dql.SourceExport{}
			}
		}
	case TranscribeTool:
		description = "Transcribe source into an operator-configured target using canonical generation and authored-file protection. No client paths, shell or compiler overrides."
		capabilities, summary := s.authoringCapabilities(names)
		meta = map[string]interface{}{"datly.authoringTargets": capabilities}
		properties["target"]["description"] = "Operator-configured authoring target. " + summary
		properties["source"] = map[string]any{"type": "string", "maxLength": 1048576}
		required = append(required, "source")
		output = &Transcription{}
		destructive = true
	case RunTool:
		description = "Start a configured linked standalone application. Returns a server-owned instance ID only after successful publication and listener readiness. Only configured loopback addresses and ports are allowed."
		properties["port"] = map[string]any{"type": "integer", "minimum": 0, "maximum": 65535}
		properties["address"] = map[string]any{"type": "string"}
		output = &Instance{}
		openWorld = true
	case StopTool:
		description = "Cancel and drain only an instance owned by this developer server. Recent completed instance IDs are idempotent. No PID, shell or port-based process control."
		properties = schema.ToolInputSchemaProperties{"instanceId": {"type": "string"}}
		required = []string{"instanceId"}
		output = &Instance{}
	}
	schemaOutput := &schema.ToolOutputSchema{}
	if err := schemaOutput.Load(output); err != nil {
		return schema.Tool{}, err
	}
	return schema.Tool{Meta: meta, Name: name, Description: &description, Annotations: &schema.ToolAnnotations{ReadOnlyHint: &readOnly, DestructiveHint: &destructive, OpenWorldHint: &openWorld}, InputSchema: schema.ToolInputSchema{Type: "object", Properties: properties, Required: required}, OutputSchema: schemaOutput}, nil
}

func (s *Service) authoringCapabilities(names []string) (map[string]any, string) {
	capabilities := make(map[string]any, len(names))
	summary := make([]string, 0, len(names))
	for _, name := range names {
		request, ok := s.authoring[name]
		if !ok {
			capabilities[name] = map[string]any{"enabled": false}
			summary = append(summary, name+"=disabled")
			continue
		}
		if request.Generation.Enabled() {
			operation := request.Generation.Operation
			language := string(request.Generation.Language)
			capabilities[name] = map[string]any{"enabled": true, "mode": "generation", "operation": operation, "language": language}
			summary = append(summary, name+"=generation:"+language+":"+operation)
			continue
		}
		capabilities[name] = map[string]any{"enabled": true, "mode": "transcribe"}
		summary = append(summary, name+"=transcribe")
	}
	return capabilities, strings.Join(summary, "; ")
}

func (s *Service) execute(ctx context.Context, request *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
	if request == nil {
		return nil, jsonrpc.NewInvalidParamsError("tool request is required", nil)
	}
	allowed := map[string]bool{}
	switch request.Params.Name {
	case ComponentsTool:
		allowed = map[string]bool{"target": true, "instanceId": true, "prefix": true}
	case InspectTool, ReverseTool:
		allowed = map[string]bool{"target": true, "instanceId": true, "component": true}
	case TranscribeTool:
		allowed = map[string]bool{"target": true, "source": true}
	case RunTool:
		allowed = map[string]bool{"target": true, "port": true, "address": true}
	case StopTool:
		allowed = map[string]bool{"instanceId": true}
	default:
		return nil, schema.NewUnknownTool(request.Params.Name)
	}
	for key := range request.Params.Arguments {
		if !allowed[key] {
			return nil, jsonrpc.NewInvalidParamsError("unsupported tool argument", nil)
		}
	}
	data, err := json.Marshal(request.Params.Arguments)
	if err != nil {
		return nil, jsonrpc.NewInvalidParamsError("invalid arguments", nil)
	}
	var args arguments
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&args); err != nil {
		return nil, jsonrpc.NewInvalidParamsError("invalid arguments", nil)
	}
	if ctx == nil {
		return nil, jsonrpc.NewInvalidParamsError("context is required", nil)
	}
	if err := ctx.Err(); err != nil {
		return toolResult(nil, err)
	}
	if request.Params.Name == StopTool {
		if args.InstanceID == "" {
			return nil, jsonrpc.NewInvalidParamsError("instanceId is required", nil)
		}
		result, err := s.stop(ctx, args.InstanceID)
		return toolResult(result, err)
	}
	if _, ok := s.targets[args.Target]; !ok {
		return nil, jsonrpc.NewInvalidParamsError("unknown configured target", nil)
	}
	ctx, finish, err := s.operation(ctx)
	if err != nil {
		return toolResult(nil, err)
	}
	defer finish()
	if request.Params.Name == ComponentsTool || request.Params.Name == InspectTool || request.Params.Name == ReverseTool {
		if request.Params.Name != ComponentsTool && args.Component == "" {
			return nil, jsonrpc.NewInvalidParamsError("component is required", nil)
		}
		result, err := s.inspect(ctx, args, request.Params.Name)
		return toolResult(result, err)
	}
	if request.Params.Name == TranscribeTool {
		_, provided := request.Params.Arguments["source"].(string)
		if !provided || len(args.Source) > 1<<20 || args.Source == "" && s.authoring[args.Target].Component == nil {
			return nil, jsonrpc.NewInvalidParamsError("source text is required; empty is allowed only for a configured linked Go component", nil)
		}
		result, err := s.transcribe(ctx, args)
		return toolResult(result, err)
	}
	result, err := s.run(ctx, args)
	return toolResult(result, err)
}

func toolResult(value any, err error) (*schema.CallToolResult, *jsonrpc.Error) {
	failed := err != nil
	if err != nil {
		value = map[string]string{"error": err.Error()}
	}
	data, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return nil, jsonrpc.NewInternalError(fmt.Sprint(marshalErr), nil)
	}
	if failed {
		value = nil
	}
	return &schema.CallToolResult{Content: []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: string(data)}}, StructuredContent: value, IsError: &failed, ResultType: schema.ResultTypeComplete}, nil
}
