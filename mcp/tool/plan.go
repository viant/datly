// Package tool compiles route-effective component contracts into MCP tools.
package tool

import (
	"context"
	"encoding/json"
	"fmt"
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/runtime/output"
	"reflect"

	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/exec"
	mcpinput "github.com/viant/datly/mcp/input"
	intent "github.com/viant/datly/mcp/internal/association"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp-protocol/schema/fieldname"
	"github.com/viant/tagly/choice"
	xshape "github.com/viant/x/shape"
)

// Argument is an immutable public projection of one canonical input field.
type Argument struct {
	nullable          *bool
	enumeration       *choice.Enum
	schemaField       *xshape.JSONField
	representation    *xshape.Representation
	normalization     *xshape.Representation
	documentation     *docs.Snapshot
	associationIndex  []int
	associationTag    reflect.StructTag
	defaultPublicName string
	publicName        string
	aliases           []string
	path              string
	sourceKind        string
	sourceName        string
	sourceType        reflect.Type
	querySelectorCSV  bool
	acceptScalar      bool
	acceptText        bool
	destinationType   reflect.Type
	wireSchema        *spec.WireSchema
	wireSchemas       map[string]*spec.WireSchema
	wireType          reflect.Type
	// Anonymous JSON fields retain their whole-body owner for XLS decoding.
	parentBodyType   reflect.Type
	customJSONBody   bool
	parentBodySource string
	anonymousGoName  string
	bodyNullPolicy   string
	bodyInputTag     reflect.StructTag
	required         bool
	// Schema parity must not introduce a new runtime missing-argument check.
	// schemaRequired is the complete advertised value, separate from runtime required.
	schemaRequired bool
	description    string
	example        string
}

func (a Argument) PublicName() string            { return a.publicName }
func (a Argument) Aliases() []string             { return append([]string(nil), a.aliases...) }
func (a Argument) Path() string                  { return a.path }
func (a Argument) SourceKind() string            { return a.sourceKind }
func (a Argument) SourceName() string            { return a.sourceName }
func (a Argument) SourceType() reflect.Type      { return a.sourceType }
func (a Argument) DestinationType() reflect.Type { return a.destinationType }
func (a Argument) WireSchema() *spec.WireSchema  { return a.wireSchema.Clone() }
func (a Argument) WireSchemas() map[string]*spec.WireSchema {
	return cloneWireSchemas(a.wireSchemas)
}
func (a Argument) Required() bool      { return a.required }
func (a Argument) Description() string { return a.description }
func (a Argument) Example() string     { return a.example }

// Plan is one immutable exact-route MCP tool plan.
type Plan struct {
	primitivePairs           []primitivePair
	hiddenAssociationMembers []hiddenAssociationMember
	associations             []associationRule
	variants                 []PathVariant
	compilerInput            Input
	strictArguments          bool
	metadata                 schema.Tool
	target                   exec.ComponentTarget
	input                    *registry.RouteInputContract
	args                     []Argument
	binding                  *mcpinput.Plan
	caseFormat               string
}

func (p *Plan) Metadata() schema.Tool {
	if p == nil {
		return schema.Tool{}
	}
	result := cloneTool(p.metadata)
	for _, variant := range p.variants {
		result, _ = MergePathMetadata(result, variant.Plan.Metadata())
	}
	return result
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

// Scope returns request providers owned by the caller; call Close after consuming results.
func (p *Plan) Scope(arguments map[string]interface{}) (*requestprovider.Scope, error) {
	if p == nil {
		return nil, fmt.Errorf("MCP tool plan is required")
	}
	return p.scope(context.Background(), arguments)
}
func (p *Plan) scope(ctx context.Context, arguments map[string]interface{}) (*requestprovider.Scope, error) {
	for _, hidden := range p.hiddenAssociationMembers {
		if _, err := transformAssociationPath(hidden.parents, "", arguments, hidden.reject); err != nil {
			return nil, err
		}
	}
	var edits intent.Edits
	if len(p.associations) > 0 {
		edits = make(intent.Edits)
	}
	for _, pair := range p.primitivePairs {
		updated, err := transformAssociationPath(pair.parents, "", arguments, pair.translate)
		if err != nil {
			return nil, err
		}
		if updated != nil {
			arguments = updated.(map[string]any)
		}
	}
	arguments, err := p.translateAssociations(edits, arguments)
	if err != nil {
		return nil, err
	}
	if len(edits) > 0 && !intent.Available(ctx) {
		return nil, fmt.Errorf("MCP association edits require an invocation context; use AssociationScope")
	}
	scope, err := p.requestScope(ctx, arguments, len(edits) != 0)
	if err == nil && len(edits) > 0 {
		intent.Publish(ctx, edits)
	}
	return scope, err
}

func (p *Plan) requestScope(ctx context.Context, arguments map[string]any, haveAssociationEdits bool) (*requestprovider.Scope, error) {
	normalized, synthetic, err := p.binaryArguments(arguments)
	if err != nil {
		return nil, err
	}
	if haveAssociationEdits && synthetic["Blob"] != nil {
		return nil, fmt.Errorf("MCP binary body cannot be combined with association edits")
	}
	// Normalize only explicitly named ordinary JSON subtrees before the existing
	// request/body providers and Bindly decoder. Wire representations stay owned
	// by their declared authority; unannotated arguments retain their exact path.
	for _, argument := range p.args {
		if argument.enumeration != nil {
			if value, present := normalized[argument.publicName]; present {
				raw, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				converted, err := argument.enumeration.DecodeJSON(raw)
				if err != nil {
					return nil, fmt.Errorf("MCP argument %s: %w", argument.publicName, err)
				}
				// Non-body collections use ordinary typed request providers. Admit
				// their compiled enum representation before those providers decode
				// storage values; body codecs retain their original JSON ownership.
				if argument.sourceKind != "body" && value != nil && argument.enumeration.CollectionItems() != nil {
					normalized[argument.publicName] = json.RawMessage(converted)
				}
			}
		}
		if argument.wireType != nil || argument.wireSchema != nil || (hasWireRepresentation(argument.wireSchemas) && p.caseFormat == "") {
			continue
		}
		value, present := normalized[argument.publicName]
		if !present {
			continue
		}
		format := p.caseFormat
		if argument.customJSONBody {
			format = ""
		}
		recognition := argument.representation
		if argument.normalization != nil {
			recognition = argument.normalization
		}
		names, e := fieldname.HasNames(argument.sourceType, representationVisibility(recognition, format))
		if e != nil {
			return nil, e
		}
		if !names {
			continue
		}
		value, e = fieldname.Normalize(argument.sourceType, value, representationVisibility(recognition, format))
		if e != nil {
			return nil, fmt.Errorf("normalize MCP argument %q: %w", argument.publicName, e)
		}
		normalized[argument.publicName] = value
	}
	request, options, err := p.binaryRequest(ctx, normalized, synthetic)
	if err != nil {
		return nil, err
	}
	for _, argument := range p.args {
		if request != nil && argument.sourceKind == "body" {
			continue
		}
		if _, ok := normalized[argument.publicName]; argument.required && !ok {
			return nil, fmt.Errorf("missing required MCP argument %q", argument.publicName)
		}
	}
	if request != nil {
		// Original raw-Blob precedence still consumes supplied multipart form
		// arguments, while raw-only calls retain ordinary form projection.
		multipartSupplied := false
		for _, argument := range p.args {
			multipartSupplied = multipartSupplied || fileArgument(argument) && normalized[argument.publicName] != nil
		}
		if multipartSupplied {
			for _, argument := range p.args {
				if argument.sourceKind == "form" {
					delete(normalized, argument.publicName)
				}
			}
		}
		return p.binding.ScopeRequest(mcpinput.Arguments(normalized), request, options...)
	}
	request, err = p.targetRequest(ctx, nil)
	if err != nil {
		return nil, err
	}
	return p.binding.ScopeRequest(mcpinput.Arguments(normalized), request)
}

func cloneArgument(argument Argument) Argument {
	argument.aliases = append([]string(nil), argument.aliases...)
	argument.wireSchema = argument.wireSchema.Clone()
	argument.wireSchemas = cloneWireSchemas(argument.wireSchemas)
	return argument
}

func cloneWireSchemas(source map[string]*spec.WireSchema) map[string]*spec.WireSchema {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]*spec.WireSchema, len(source))
	for key, schema := range source {
		result[key] = schema.Clone()
	}
	return result
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
	if vocabulary, ok := source.InputSchema.AdditionalProperties.(map[string]interface{}); ok {
		result.InputSchema.AdditionalProperties = cloneSchemaMap(vocabulary)
	}
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

// Requiredness metadata does not replace the reflected wire representation.
func hasWireRepresentation(schemas map[string]*spec.WireSchema) bool {
	for _, schema := range schemas {
		if wireSchemaReplacesShape(schema) {
			return true
		}
	}
	return false
}

func wireSchemaReplacesShape(schema *spec.WireSchema) bool {
	if schema == nil {
		return false
	}
	if schema.Type != "" {
		return true
	}
	for _, child := range schema.AnyOf {
		if wireSchemaReplacesShape(child) {
			return true
		}
	}
	return wireSchemaReplacesShape(schema.Items)
}

// OutputPlan is the immutable exposure-local native output authority.
func (p *Plan) OutputPlan() *output.Plan { return p.compilerInput.Output }
