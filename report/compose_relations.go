package report

import (
	"fmt"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/output"
	"github.com/viant/datly/report/cubecompose"
	"github.com/viant/datly/spec"
	"reflect"
)

// CompositionRelation describes a native nested holder appended to composed
// rows. Keys are existing scalar output aliases, never newly grouped fields.
type CompositionRelation struct {
	Name string   `json:"name"`
	Keys []string `json:"keys"`
}

type composeRelation struct {
	name   string
	holder string
	keys   []string
}

func composeRelations(component *spec.Component, rowType reflect.Type) ([]composeRelation, error) {
	view := component.RootView
	// Projection rows have a different envelope. Do not silently drop a
	// source-envelope exclusion/redaction or global presentation contract.
	if settings := component.Settings; settings != nil {
		customOutput := settings.Output != nil && (len(settings.Output.Exclude) > 0 || settings.Output.OmitEmpty || settings.Output.NilSlicePolicy != "")
		if customOutput || settings.JSONMarshalType != "" || settings.DateFormat != "" {
			return nil, fmt.Errorf("projection relations do not support source-envelope output policies; use field-local public output tags")
		}
	}
	encoder, err := (output.Compiler{}).Compile(output.CompileInput{Type: rowType, Component: component})
	if err != nil {
		return nil, err
	}
	public, err := encoder.SelectorFieldNames(rowType, "")
	if err != nil {
		return nil, err
	}
	var result []composeRelation
	for _, relation := range view.Relations {
		if relation == nil || relation.Cardinality != spec.CardinalityOne || len(relation.On) == 0 || relation.View == nil {
			continue
		}
		field, ok := rowType.FieldByName(relation.Holder)
		if !ok || field.Type.Kind() != reflect.Pointer || field.Type.Elem().Kind() != reflect.Struct || field.Tag.Get("internal") == "true" {
			continue
		}
		name := ""
		for _, candidate := range public {
			if candidate.GoName == field.Name {
				name = candidate.PublicName
			}
		}
		if name == "" {
			continue
		}
		candidate := composeRelation{name: name, holder: relation.Holder}
		for _, link := range relation.On {
			if link == nil {
				return nil, fmt.Errorf("invalid source relation %s", name)
			}
			key := link.ParentOutput
			if key == "" {
				key = link.ParentColumn
			}
			if key == "" {
				return nil, fmt.Errorf("source relation %s has no output key", name)
			}
			candidate.keys = append(candidate.keys, key)
		}
		result = append(result, candidate)
	}
	return result, nil
}

func (h *composeHandler) projectionRelations(plan *cubecompose.Plan, names []string) ([]exec.ProjectionRelation, []CompositionRelation, error) {
	if len(names) == 0 {
		return nil, nil, nil
	}
	if h.config == nil || !h.config.ProjectionRelations {
		return nil, nil, fmt.Errorf("compose projection relations are not enabled")
	}
	keys, err := plan.ProjectionKeys()
	if err != nil {
		return nil, nil, err
	}
	var requests []exec.ProjectionRelation
	var metadata []CompositionRelation
	seen := map[string]bool{}
	for _, name := range names {
		if seen[name] {
			return nil, nil, fmt.Errorf("duplicate projection relation %s", name)
		}
		seen[name] = true
		var found *composeRelation
		for i := range h.relations {
			if h.relations[i].name == name {
				if found != nil {
					return nil, nil, fmt.Errorf("ambiguous projection relation %s", name)
				}
				found = &h.relations[i]
			}
		}
		if found == nil {
			return nil, nil, fmt.Errorf("projection relation %s is not an exposed to-one relation", name)
		}
		request := exec.ProjectionRelation{Name: found.holder, Output: found.name}
		entry := CompositionRelation{Name: found.name}
		for _, column := range plan.Columns {
			if column.Name == name {
				return nil, nil, fmt.Errorf("projection relation %s conflicts with scalar output", name)
			}
		}
		for _, source := range found.keys {
			output, ok := keys[source]
			if !ok {
				return nil, nil, fmt.Errorf("projection relation %s requires direct dimension %s in the output", name, source)
			}
			request.Keys = append(request.Keys, exec.ProjectionKey{Source: source, Output: output})
			entry.Keys = append(entry.Keys, output)
		}
		requests = append(requests, request)
		metadata = append(metadata, entry)
	}
	return requests, metadata, nil
}
