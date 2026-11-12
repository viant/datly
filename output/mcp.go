package output

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/mcp-protocol/schema/fieldname"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

type mcpFieldKey struct{ path, name string }

// WithMCPVisibility applies MCP names and exclusions to transport-local metadata. The
// native schema and encoder share this representation; HTTP retains its plan.
func (p *Plan) WithMCPVisibility() (*Plan, error) {
	if p == nil || p.typeOf == nil {
		return p, nil
	}
	visibilityType := p.typeOf
	declaredWire := false
	owner := reflect.New(p.typeOf)
	if p.typeOf.Kind() == reflect.Pointer {
		owner = reflect.New(p.typeOf.Elem())
	}
	if _, customJSON := owner.Interface().(json.Marshaler); customJSON {
		if _, declared := owner.Interface().(JSONWireType); declared {
			wire, err := p.Wire("json")
			if err != nil {
				return nil, err
			}
			visibilityType, declaredWire = wire.Type, true
		}
	}
	walker, err := xshape.CompileRewrite(visibilityType)
	if err != nil {
		return nil, err
	}
	// Retain caller-selected metadata while the native walker owns storage,
	// container traversal, recursion and custom JSON boundaries.
	edits := map[mcpFieldKey]xshape.FieldMetadata{}
	if !declaredWire {
		mcpEffectiveFields(p.representation, "", edits)
	}
	metadata, err := walker.RewriteRepresentation(func(path string, _ reflect.Type, original *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		selected := *original
		if effective, ok := edits[mcpFieldKey{path, original.Name}]; ok {
			selected = effective
		}
		name, _, _ := strings.Cut(selected.Tag.Get("json"), ",")
		if name == "-" || selected.Tag.Get("internal") == "true" {
			selected.SkipChildren = true
		} else {
			public, err := fieldname.Resolve(reflect.StructField{Name: selected.Name, Tag: selected.Tag}, selected.Name)
			if err != nil {
				return nil, err
			}
			field := &xcontract.Field{Name: selected.Name, Tag: selected.Tag}
			if public.Hidden {
				field = field.Hide()
				selected.Tag, selected.SkipChildren = field.Tag, true
			} else if public.Explicit {
				_, options, _ := strings.Cut(selected.Tag.Get("json"), ",")
				name := public.Public
				if options != "" {
					name += "," + options
				}
				selected.Tag = field.SetTag("json", name).Tag
			}
		}
		return &selected, nil
	})
	if err != nil {
		return nil, err
	}
	if declaredWire {
		// The custom encoder owns the value conversion to its declared wire DTO.
		// Storage/input annotations do not describe that DTO. Preserve the encoder
		// only when its actual wire shape needs no MCP rewrite; never ignore an
		// exclusion or rename on the declared wire type.
		if metadata.Changed() {
			return nil, fmt.Errorf("custom JSON wire type %s requires an MCP codec projection", visibilityType)
		}
		return p, nil
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
