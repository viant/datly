package output

import (
	"reflect"
	"strings"

	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

type mcpFieldKey struct{ path, name string }

// WithMCPVisibility applies MCP exclusions to transport-local metadata. The
// native schema and encoder share this representation; HTTP retains its plan.
func (p *Plan) WithMCPVisibility() (*Plan, error) {
	if p == nil || p.typeOf == nil {
		return p, nil
	}
	walker, err := xshape.CompileRewrite(p.typeOf)
	if err != nil {
		return nil, err
	}
	// Retain caller-selected metadata while the native walker owns storage,
	// container traversal, recursion and custom JSON boundaries.
	edits := map[mcpFieldKey]xshape.FieldMetadata{}
	mcpEffectiveFields(p.representation, "", edits)
	metadata, err := walker.RewriteRepresentation(func(path string, _ reflect.Type, original *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		selected := *original
		if effective, ok := edits[mcpFieldKey{path, original.Name}]; ok {
			selected = effective
		}
		name, _, _ := strings.Cut(selected.Tag.Get("json"), ",")
		if name == "-" || selected.Tag.Get("internal") == "true" {
			selected.SkipChildren = true
		} else if visibility, _, _ := strings.Cut(selected.Tag.Get("mcp"), ","); visibility == "-" {
			field := (&xcontract.Field{Name: selected.Name, Tag: selected.Tag}).Hide()
			selected.Tag, selected.SkipChildren = field.Tag, true
		}
		return &selected, nil
	})
	if err != nil {
		return nil, err
	}
	return p.WithRepresentation(metadata)
}

func mcpEffectiveFields(metadata *xshape.Representation, path string, edits map[mcpFieldKey]xshape.FieldMetadata) {
	if metadata == nil {
		return
	}
	if metadata.Elem() != nil {
		switch metadata.Original().Kind() {
		case reflect.Slice, reflect.Array:
			path += "[]"
		case reflect.Map:
			path += "{}"
		}
		mcpEffectiveFields(metadata.Elem(), path, edits)
		return
	}
	for i := 0; i < metadata.NumField(); i++ {
		original, field := metadata.Original().Field(i), metadata.Field(i)
		if field.Name != original.Name || field.Tag != original.Tag {
			edits[mcpFieldKey{path, original.Name}] = xshape.FieldMetadata{Name: field.Name, Tag: field.Tag}
		}
		child := original.Name
		if path != "" {
			child = path + "." + child
		}
		mcpEffectiveFields(metadata.FieldShape(i), child, edits)
	}
}
