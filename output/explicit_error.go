package output

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
	"github.com/viant/xdatly/response"
)

type errorOutput struct {
	plan         *Plan
	mcpPlan      *Plan
	mcpError     error
	presentation spec.ErrorOutputPresentation
}

func (c Compiler) compileErrorOutputs(component *spec.Component) (map[reflect.Type]errorOutput, error) {
	if len(component.ErrorOutputs) == 0 {
		return nil, nil
	}
	settings := &spec.Settings{}
	if source := component.Settings; source != nil {
		settings.CaseFormat = source.CaseFormat
		settings.DateFormat = source.DateFormat
		if source.Output != nil {
			settings.Output = &spec.OutputSettings{
				Exclude:        source.Output.Exclude,
				OmitEmpty:      source.Output.OmitEmpty,
				NilSlicePolicy: source.Output.NilSlicePolicy,
			}
		}
	}
	errorComponent := &spec.Component{Settings: settings}
	result := make(map[reflect.Type]errorOutput, len(component.ErrorOutputs))
	for _, declared := range component.ErrorOutputs {
		typ := errorOutputType(declared.Type)
		if typ == nil || typ.Kind() != reflect.Struct || typ.Name() == "" {
			return nil, fmt.Errorf("explicit error output requires a named struct, got %v", declared.Type)
		}
		if reflect.PointerTo(typ).Implements(reflect.TypeFor[response.Response]()) {
			return nil, fmt.Errorf("explicit error output %s owns its transport", typ)
		}
		presentation := declared.Presentation
		switch presentation {
		case "":
			presentation = spec.ErrorOutputJSON
		case spec.ErrorOutputJSON, spec.ErrorOutputLegacyText:
		default:
			return nil, fmt.Errorf("unsupported error output presentation %q for %s", presentation, typ)
		}
		if _, exists := result[typ]; exists {
			return nil, fmt.Errorf("duplicate explicit error output %s", typ)
		}
		variantComponent := errorComponent
		if presentation == spec.ErrorOutputLegacyText {
			// The buffered wire contract used standard JSON tags, omission,
			// collection and escaping rules. Exclusions still constrain disclosure.
			legacy := &spec.Settings{}
			if settings.Output != nil {
				legacy.Output = &spec.OutputSettings{Exclude: settings.Output.Exclude}
			}
			variantComponent = &spec.Component{Settings: legacy}
		}
		plan, err := c.compile(CompileInput{Component: variantComponent, Type: typ, explicitError: true}, false)
		if err != nil {
			return nil, fmt.Errorf("compile explicit error output %s: %w", typ, err)
		}
		if presentation == spec.ErrorOutputLegacyText {
			plan, err = plan.withLegacyErrorExclusions()
			if err != nil {
				return nil, fmt.Errorf("compile legacy error output %s: %w", typ, err)
			}
		}
		mcpPlan, mcpErr := plan.WithMCPVisibility()
		result[typ] = errorOutput{plan: plan, mcpPlan: mcpPlan, mcpError: mcpErr, presentation: presentation}
	}
	return result, nil
}

// withLegacyErrorExclusions translates canonical field paths into tags before
// standard JSON serializes public names. Tagged aliases and embedded fields
// therefore cannot make an exclusion ineffective.
func (p *Plan) withLegacyErrorExclusions() (*Plan, error) {
	if len(p.exclude) == 0 {
		return p, nil
	}
	walker, err := xshape.CompileRewrite(p.typeOf)
	if err != nil {
		return nil, err
	}
	edits := map[outputFieldKey]outputFieldEdit{}
	effectiveOutputFields(p.representation, "", edits)
	visited := map[string]bool{}
	containers := strings.NewReplacer("[]", "", "{}", "")
	metadata, err := walker.RewriteRepresentation(func(path string, _ reflect.Type, original *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		selected := *original
		if effective, ok := edits[outputFieldKey{path, original.Name}]; ok {
			selected = effective.value
		}
		canonicalPath := containers.Replace(path)
		if p.exclude.Exclude(canonicalPath, selected.Name) {
			key := strings.TrimPrefix(canonicalPath+"."+selected.Name, ".")
			visited[normalizedExclusion(key)] = true
			selected.Tag = (&xcontract.Field{Name: selected.Name, Tag: selected.Tag}).Hide().Tag
		}
		return &selected, nil
	})
	if err != nil {
		return nil, err
	}
	for _, path := range p.exclude {
		if !visited[normalizedExclusion(path)] {
			return nil, fmt.Errorf("output exclusion %q cannot be applied to legacy JSON", path)
		}
	}
	return p.WithRepresentation(metadata)
}

// projectExplicitError applies a selected representation to an alternate
// envelope when every edit still identifies the same canonical nested field.
// An unmappable edit could carry a caller disclosure constraint.
func projectExplicitError(typ reflect.Type, selected *xshape.Representation) (*xshape.Representation, error) {
	if selected.Original() == typ {
		return selected, nil
	}
	walker, err := xshape.CompileRewrite(typ)
	if err != nil {
		return nil, err
	}
	edits := map[outputFieldKey]outputFieldEdit{}
	effectiveOutputFields(selected, "", edits)
	metadata, err := walker.RewriteRepresentation(func(path string, owner reflect.Type, original *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		key := outputFieldKey{path, original.Name}
		edit, ok := edits[key]
		if !ok || edit.owner != owner {
			return nil, nil
		}
		delete(edits, key)
		return &edit.value, nil
	})
	if err != nil {
		return nil, err
	}
	if len(edits) > 0 {
		return nil, fmt.Errorf("caller output projection cannot be applied to explicit error output %s", typ)
	}
	return metadata, nil
}

func errorOutputType(typ reflect.Type) reflect.Type {
	for typ != nil && typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}

// EncodeExplicitError applies the linked declaration for body, if any.
// On a match the caller must use the returned representation or fail closed.
func (p *Plan) EncodeExplicitError(ctx context.Context, target dexec.ComponentTarget, body any, mcp bool) (Result, spec.ErrorOutputPresentation, bool, error) {
	if p == nil || len(p.errorOutputs) == 0 {
		return Result{}, "", false, nil
	}
	typ := errorOutputType(reflect.TypeOf(body))
	variant, exists := p.errorOutputs[typ]
	if !exists {
		return Result{}, "", false, nil
	}
	plan := variant.plan
	var projected bool
	if effective, ok := dexec.SelectedContract(ctx, target); ok {
		selectedType := effective.Output
		representation := effective.OutputRepresentation
		canonical := p.typeOf
		if p.effectiveSource != nil {
			canonical = p.effectiveSource
		}
		if selectedType != canonical || representation != nil && representation.Changed() {
			if representation == nil || representation.Original() != selectedType || selectedType != canonical && selectedType != typ {
				return Result{}, "", true, fmt.Errorf("caller output projection cannot be applied to explicit error output %s", typ)
			}
			metadata, err := projectExplicitError(typ, representation)
			if err != nil {
				return Result{}, "", true, err
			}
			projected = true
			plan, err = plan.WithContract(typ, metadata)
			if err == nil && variant.presentation == spec.ErrorOutputLegacyText {
				plan, err = plan.withLegacyErrorExclusions()
			}
			if err != nil {
				return Result{}, "", true, err
			}
		}
	}
	if mcp {
		var err error
		if projected {
			plan, err = plan.WithMCPVisibility()
		} else {
			plan, err = variant.mcpPlan, variant.mcpError
		}
		if err != nil {
			return Result{}, "", true, err
		}
	}
	var encoded Result
	var err error
	if variant.presentation == spec.ErrorOutputLegacyText {
		encoded.ContentType = "text/plain; charset=utf-8"
		var options []structjson.Option
		if plan.representation != nil {
			options = append(options, structjson.WithRepresentation(plan.representation))
		}
		if selection := dexec.SelectedOutputFields(ctx, body); selection != nil {
			options = append(options, structjson.WithPathFieldExcluder(selection))
		}
		encoded.Data, err = structjson.MarshalStandard(body, options...)
	} else {
		encoded, err = plan.Encode(ctx, "json", body)
	}
	if err == nil && !json.Valid(encoded.Data) {
		err = fmt.Errorf("explicit error output %s produced invalid JSON", typ)
	}
	return encoded, variant.presentation, true, err
}
