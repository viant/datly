package tool

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/runtime/output"
	"github.com/viant/mcp-protocol/schema"
)

// Complete envelope branches preserve homogeneous row modes, nullability and
// projection semantics. An item union would incorrectly allow mixed-mode rows.
func (p *Plan) discoveryOutputSchema(effective *exec.EffectiveContract) (*schema.ToolOutputSchema, error) {
	explicit := reflect.TypeFor[invocation.TypedToolResult]()
	if typ := effective.Output; typ != nil && (typ.Implements(explicit) || typ.Kind() != reflect.Pointer && reflect.PointerTo(typ).Implements(explicit)) {
		return nil, fmt.Errorf("%w: explicit MCP results own their structured content", output.ErrSchemaUnavailable)
	}
	branches := make([]any, 0, len(effective.OutputAlternatives))
	for i, metadata := range effective.OutputAlternatives {
		var document map[string]any
		if i < len(effective.OutputAlternativeSchemas) && effective.OutputAlternativeSchemas[i] != nil {
			artifact := effective.OutputAlternativeSchemas[i]
			if err := artifact.Errors[p.caseFormat]; err != nil {
				return nil, err
			}
			document = artifact.Schemas[p.caseFormat]
		}
		if document == nil {
			plan, err := p.compilerInput.Output.WithContract(effective.Output, metadata)
			if err != nil {
				return nil, err
			}
			plan, err = plan.WithMCPVisibility()
			if err != nil {
				return nil, err
			}
			document, err = plan.JSONSchema()
			if err != nil {
				return nil, fmt.Errorf("describe output alternative: %w", err)
			}
		}
		if document["type"] != "object" {
			return nil, fmt.Errorf("output discovery alternative must be an object envelope")
		}
		// Local recursive references were rooted in the original complete schema.
		// Rebase only a cloned publication document into its envelope branch.
		encoded, err := json.Marshal(document)
		if err != nil {
			return nil, err
		}
		var branch map[string]any
		if err := json.Unmarshal(encoded, &branch); err != nil {
			return nil, err
		}
		rebaseOutputReferences(branch, fmt.Sprintf("#/anyOf/%d", i))
		branches = append(branches, branch)
	}
	data, err := json.Marshal(map[string]any{"type": "object", "anyOf": branches})
	if err != nil {
		return nil, err
	}
	result := &schema.ToolOutputSchema{}
	if err := json.Unmarshal(data, result); err != nil {
		return nil, err
	}
	return result, nil
}

func rebaseOutputReferences(value any, root string) {
	switch node := value.(type) {
	case map[string]any:
		if ref, ok := node["$ref"].(string); ok && strings.HasPrefix(ref, "#") {
			node["$ref"] = root + strings.TrimPrefix(ref, "#")
		}
		for _, child := range node {
			rebaseOutputReferences(child, root)
		}
	case []any:
		for _, child := range node {
			rebaseOutputReferences(child, root)
		}
	}
}
