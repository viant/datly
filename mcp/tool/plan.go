// Package tool compiles route-effective component contracts into MCP tools.
package tool

import (
	"encoding/json"
	"fmt"
	docs "github.com/viant/datly/documentation"
	"reflect"

	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/exec"
	mcpinput "github.com/viant/datly/mcp/input"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/mcp-protocol/schema"
)

// Argument is an immutable public projection of one canonical input field.
type Argument struct {
	documentation   *docs.Snapshot
	publicName      string
	aliases         []string
	path            string
	sourceKind      string
	sourceName      string
	sourceType      reflect.Type
	destinationType reflect.Type
	required        bool
	description     string
	example         string
}

func (a Argument) PublicName() string            { return a.publicName }
func (a Argument) Aliases() []string             { return append([]string(nil), a.aliases...) }
func (a Argument) Path() string                  { return a.path }
func (a Argument) SourceKind() string            { return a.sourceKind }
func (a Argument) SourceName() string            { return a.sourceName }
func (a Argument) SourceType() reflect.Type      { return a.sourceType }
func (a Argument) DestinationType() reflect.Type { return a.destinationType }
func (a Argument) Required() bool                { return a.required }
func (a Argument) Description() string           { return a.description }
func (a Argument) Example() string               { return a.example }

// Plan is one immutable exact-route MCP tool plan.
type Plan struct {
	metadata schema.Tool
	target   exec.ComponentTarget
	input    *registry.RouteInputContract
	args     []Argument
	binding  *mcpinput.Plan
}

func (p *Plan) Metadata() schema.Tool {
	if p == nil {
		return schema.Tool{}
	}
	return cloneTool(p.metadata)
}

func (p *Plan) Target() exec.ComponentTarget {
	if p == nil {
		return exec.ComponentTarget{}
	}
	return p.target
}

func (p *Plan) Input() *registry.RouteInputContract {
	if p == nil {
		return nil
	}
	return p.input
}

func (p *Plan) Arguments() []Argument {
	if p == nil {
		return nil
	}
	result := make([]Argument, len(p.args))
	for i, argument := range p.args {
		result[i] = cloneArgument(argument)
	}
	return result
}

func (p *Plan) Scope(arguments map[string]interface{}) (*requestprovider.Scope, error) {
	if p == nil {
		return nil, fmt.Errorf("MCP tool plan is required")
	}
	normalized, err := p.binding.NormalizeArguments(arguments)
	if err != nil {
		return nil, err
	}
	for _, argument := range p.args {
		if _, ok := normalized[argument.publicName]; argument.required && !ok {
			return nil, fmt.Errorf("missing required MCP argument %q", argument.publicName)
		}
	}
	return p.binding.Scope(mcpinput.Arguments(normalized))
}

func cloneArgument(argument Argument) Argument {
	argument.aliases = append([]string(nil), argument.aliases...)
	return argument
}

func cloneTool(source schema.Tool) schema.Tool {
	result := source
	if source.Meta != nil {
		data, _ := json.Marshal(source.Meta)
		result.Meta = nil
		_ = json.Unmarshal(data, &result.Meta)
	}
	if source.Description != nil {
		value := *source.Description
		result.Description = &value
	}
	result.InputSchema.Properties = make(map[string]map[string]interface{}, len(source.InputSchema.Properties))
	for name, property := range source.InputSchema.Properties {
		result.InputSchema.Properties[name] = cloneSchemaMap(property)
	}
	result.InputSchema.Required = append([]string(nil), source.InputSchema.Required...)
	if source.OutputSchema != nil {
		output := *source.OutputSchema
		output.Properties = make(map[string]map[string]interface{}, len(source.OutputSchema.Properties))
		for name, property := range source.OutputSchema.Properties {
			output.Properties[name] = cloneSchemaMap(property)
		}
		output.Required = append([]string(nil), source.OutputSchema.Required...)
		result.OutputSchema = &output
	}
	return result
}

func cloneSchemaMap(source map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(source))
	for key, value := range source {
		result[key] = cloneSchemaValue(value)
	}
	return result
}

func cloneSchemaValue(value interface{}) interface{} {
	switch actual := value.(type) {
	case map[string]interface{}:
		return cloneSchemaMap(actual)
	case []interface{}:
		result := make([]interface{}, len(actual))
		for index, item := range actual {
			result[index] = cloneSchemaValue(item)
		}
		return result
	case []string:
		return append([]string(nil), actual...)
	default:
		return actual
	}
}
