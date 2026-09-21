package tool

import (
	"fmt"
	documentation "github.com/viant/datly/documentation"
	"reflect"
	"sort"
	"strings"

	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	mcpinput "github.com/viant/datly/mcp/input"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	xshape "github.com/viant/x/shape"
)

type Input struct {
	Fields         []registry.InputField
	Example        string
	TransportReady bool
	Documentation  *documentation.Snapshot
	Component      spec.Key
	Exposure       *spec.MCPExposure
	Contract       *registry.RouteInputContract
	// OutputType is the execution result shape. MCP output schemas are only
	// emitted for object-shaped structuredContent, as required by the protocol.
	OutputType reflect.Type
}

type fieldCompiler func(registry.InputField, reflect.StructField) (Argument, bool, error)

type Compiler struct {
	binding *mcpinput.Compiler
	fields  map[string]fieldCompiler
}

func NewCompiler() *Compiler {
	result := &Compiler{
		binding: mcpinput.NewCompiler(),
		fields:  make(map[string]fieldCompiler, 7),
	}
	for _, kind := range []string{"query", "path", "cookie", "form", "body"} {
		result.fields[kind] = result.compileExternal
	}
	result.fields["header"] = result.compileHeader
	result.fields["http_request"] = result.compileHTTPRequest
	return result
}

func (c *Compiler) Compile(input Input) (*Plan, error) {
	if c == nil {
		return nil, fmt.Errorf("MCP tool compiler is required")
	}
	if input.Exposure == nil || input.Exposure.Kind != spec.MCPExposureTool {
		return nil, fmt.Errorf("MCP tool exposure is required")
	}
	name := strings.TrimSpace(input.Exposure.Name)
	if err := validateToolName(name); err != nil {
		return nil, err
	}
	if input.Contract == nil || input.Contract.Type() == nil || input.Contract.Route().String() == "" {
		return nil, fmt.Errorf("MCP tool %q requires an exact route input contract", name)
	}
	arguments := make([]Argument, 0, len(input.Contract.Fields()))
	publicNames := map[string]bool{}
	fields := input.Fields
	if fields == nil {
		fields = input.Contract.Fields()
	}
	for _, inputField := range fields {
		binding := inputField.Binding()
		compileField := c.fields[strings.ToLower(strings.TrimSpace(binding.Location.Kind))]
		if compileField == nil {
			continue
		}
		if inputField.Anonymous() {
			flattened, err := c.compileAnonymousBody(inputField)
			if err != nil {
				return nil, fmt.Errorf("compile MCP tool %q: %w", name, err)
			}
			for _, argument := range flattened {
				if publicNames[argument.publicName] {
					return nil, fmt.Errorf("compile MCP tool %q: duplicate public argument %q", name, argument.publicName)
				}
				publicNames[argument.publicName] = true
				arguments = append(arguments, argument)
			}
			continue
		}
		structField, err := inputField.StructField()
		if err != nil {
			return nil, fmt.Errorf("compile MCP tool %q: %w", name, err)
		}
		argument, include, err := compileField(inputField, structField)
		if err != nil {
			return nil, fmt.Errorf("compile MCP tool %q: %w", name, err)
		}
		if !include {
			continue
		}
		if publicNames[argument.publicName] {
			return nil, fmt.Errorf("compile MCP tool %q: duplicate public argument %q", name, argument.publicName)
		}
		publicNames[argument.publicName] = true
		owner := inputField.Documentation()
		if owner == nil {
			owner = input.Documentation
		}
		argument.documentation = owner
		annotation := owner.Parameter(inputField.Binding().Name, documentation.Annotation{Description: argument.description, Example: argument.example})
		argument.description, argument.example = annotation.Description, annotation.Example
		arguments = append(arguments, argument)
	}
	sort.SliceStable(arguments, func(i, j int) bool { return arguments[i].publicName < arguments[j].publicName })
	bindingArguments := make([]mcpinput.Argument, len(arguments))
	inputSchema := schema.ToolInputSchema{Type: "object", Properties: map[string]map[string]interface{}{}}
	for index, argument := range arguments {
		owner := argument.documentation
		if owner == nil {
			owner = input.Documentation
		}
		annotation := owner.Parameter(argument.path, documentation.Annotation{Description: argument.description, Example: argument.example})
		argument.description, argument.example = annotation.Description, annotation.Example
		arguments[index] = argument
		sourceType := argument.sourceType
		if argument.required {
			sourceType = (xshape.Runtime{}).Indirect(sourceType)
		}
		property, err := (&schemaProjector{docs: owner}).argument(sourceType, argument.path, argument.publicName)
		if err != nil {
			return nil, fmt.Errorf("compile MCP tool %q argument %q: %w", name, argument.publicName, err)
		}
		if argument.required {
			if kinds, ok := property["type"].([]string); ok {
				allowed := make([]string, 0, len(kinds))
				for _, kind := range kinds {
					if kind != "null" {
						allowed = append(allowed, kind)
					}
				}
				if len(allowed) == 1 {
					property["type"] = allowed[0]
				} else {
					property["type"] = allowed
				}
			}
		}
		if argument.description != "" {
			property["description"] = argument.description
		}
		if argument.example != "" {
			property["examples"] = []interface{}{argument.example}
		}
		inputSchema.Properties[argument.publicName] = property
		if argument.required {
			inputSchema.Required = append(inputSchema.Required, argument.publicName)
		}
		bindingArguments[index] = mcpinput.Argument{
			PublicName: argument.publicName,
			Source:     bindstate.Location{Kind: argument.sourceKind, In: argument.sourceName},
			SourceType: argument.sourceType,
		}
	}
	binding, err := c.binding.Compile(bindingArguments)
	if err != nil {
		return nil, fmt.Errorf("compile MCP tool %q binding: %w", name, err)
	}
	description := strings.TrimSpace(input.Exposure.Description)
	var metadata map[string]interface{}
	if input.Example != "" {
		metadata = map[string]interface{}{"datly/example": input.Example}
	}
	if input.TransportReady {
		if responses := input.Documentation.Responses(input.Contract.Route().Path); len(responses) > 0 {
			if metadata == nil {
				metadata = map[string]interface{}{}
			}
			metadata["datly/httpResponses"] = responses
			metadata["datly/httpSchemas"] = input.Documentation.Schemas()
		}
	}
	outputSchema, outputErr := outputContractSchema(input.OutputType)
	if outputErr != nil {
		return nil, fmt.Errorf("compile MCP tool %q output schema: %w", name, outputErr)
	}
	return &Plan{
		metadata: schema.Tool{Meta: metadata, Name: name, Description: &description, InputSchema: inputSchema, OutputSchema: outputSchema},
		target:   exec.ComponentTarget{Component: input.Component, Route: input.Contract.Route()},
		input:    input.Contract, args: arguments, binding: binding,
	}, nil
}

func outputContractSchema(source reflect.Type) (*schema.ToolOutputSchema, error) {
	if source == nil {
		return nil, nil
	}
	for source.Kind() == reflect.Pointer {
		source = source.Elem()
	}
	// A non-object result is transported as text by the MCP invoker and cannot
	// truthfully be advertised as structuredContent.
	if source.Kind() != reflect.Struct {
		return nil, nil
	}
	result := &schema.ToolOutputSchema{}
	if err := result.Load(reflect.New(source).Interface()); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Compiler) compileExternal(inputField registry.InputField, field reflect.StructField) (Argument, bool, error) {
	binding := inputField.Binding()
	publicName, hidden := publicFieldName(binding.Name, binding.Location.In, field)
	required := binding.Required != nil && *binding.Required
	if hidden {
		if required {
			return Argument{}, false, fmt.Errorf("required field %q is hidden by json:\"-\"", inputField.Path())
		}
		return Argument{}, false, nil
	}
	if publicName == "" {
		return Argument{}, false, fmt.Errorf("field %q has no canonical public name", inputField.Path())
	}
	argument := Argument{
		publicName: publicName, path: inputField.Path(),
		sourceKind: strings.ToLower(strings.TrimSpace(binding.Location.Kind)), sourceName: binding.Location.In,
		sourceType: inputField.SourceType(), destinationType: inputField.DestinationType(), required: required,
	}
	if param, ok := binding.Extension.(*spec.Parameter); ok && param != nil {
		argument.description = strings.TrimSpace(param.Description)
		argument.example = strings.TrimSpace(param.Example)
	}
	return argument, true, nil
}

func (c *Compiler) compileHeader(inputField registry.InputField, field reflect.StructField) (Argument, bool, error) {
	if strings.EqualFold(strings.TrimSpace(inputField.Binding().Location.In), "Authorization") {
		return Argument{}, false, nil
	}
	return c.compileExternal(inputField, field)
}

func (c *Compiler) compileHTTPRequest(inputField registry.InputField, _ reflect.StructField) (Argument, bool, error) {
	binding := inputField.Binding()
	if binding.Required != nil && *binding.Required {
		return Argument{}, false, fmt.Errorf("required http_request field %q cannot be supplied by MCP", inputField.Path())
	}
	return Argument{}, false, nil
}

func validateToolName(name string) error {
	if name == "" {
		return fmt.Errorf("MCP tool name is required")
	}
	if len(name) > 128 {
		return fmt.Errorf("MCP tool name %q exceeds 128 characters", name)
	}
	for _, char := range name {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' || char == '.' {
			continue
		}
		return fmt.Errorf("MCP tool name %q contains unsupported character %q", name, char)
	}
	return nil
}
