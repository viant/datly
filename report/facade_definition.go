package report

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
)

// Definition is the portable cube facade contract. It contains selectors and
// provider mappings, never SQL or a second copy of the source reader plan.
// Generated Go and dynamic DQL compile this same definition against their input.
type Definition struct {
	Target     exec.ComponentTarget  `json:"target"`
	View       string                `json:"view"`
	Dimensions []SelectionDefinition `json:"dimensions"`
	Measures   []SelectionDefinition `json:"measures"`
	Filters    []FilterDefinition    `json:"filters"`
	Relations  []RelationDefinition  `json:"relations,omitempty"`
	OrderBy    string                `json:"orderBy"`
	Limit      string                `json:"limit"`
	Offset     string                `json:"offset"`
	Parameters []*spec.Parameter     `json:"parameters,omitempty"`
}

type SelectionDefinition struct {
	Field string `json:"field"`
	Name  string `json:"name"`
}
type FilterDefinition struct {
	Field    string         `json:"field"`
	Name     string         `json:"name"`
	Location state.Location `json:"location"`
	// SourcePointer preserves nullable raw inputs separately from the facade's
	// outer presence pointer. No codec is executed by the facade.
	SourcePointer bool `json:"sourcePointer,omitempty"`
}
type RelationDefinition struct {
	Holder     string   `json:"holder"`
	Dimensions []string `json:"dimensions"`
}

func (p *Plan) Definition() (Definition, error) {
	if p == nil || p.inputType == nil {
		return Definition{}, fmt.Errorf("cube plan is required")
	}
	result := Definition{Target: p.target, View: p.view}
	appendSelections := func(items []selection, target *[]SelectionDefinition) error {
		for _, item := range items {
			path, err := definitionPath(p.inputType, item.index)
			if err != nil {
				return err
			}
			*target = append(*target, SelectionDefinition{Field: path, Name: item.name})
		}
		return nil
	}
	if err := appendSelections(p.dimensions, &result.Dimensions); err != nil {
		return result, err
	}
	if err := appendSelections(p.measures, &result.Measures); err != nil {
		return result, err
	}
	for _, item := range p.filters {
		path, err := definitionPath(p.inputType, item.index)
		if err != nil {
			return result, err
		}
		result.Filters = append(result.Filters, FilterDefinition{Field: path, Name: item.name, Location: item.location, SourcePointer: item.sourceType.Kind() == reflect.Pointer})
	}
	var err error
	if result.OrderBy, err = definitionPath(p.inputType, p.orderIndex); err != nil {
		return result, err
	}
	if result.Limit, err = definitionPath(p.inputType, p.limitIndex); err != nil {
		return result, err
	}
	if result.Offset, err = definitionPath(p.inputType, p.offsetIndex); err != nil {
		return result, err
	}
	holders := map[string][]string{}
	for dimension, relations := range p.holderByName {
		for _, holder := range relations {
			holders[holder] = append(holders[holder], dimension)
		}
	}
	names := make([]string, 0, len(holders))
	for name := range holders {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		sort.Strings(holders[name])
		result.Relations = append(result.Relations, RelationDefinition{Holder: name, Dimensions: holders[name]})
	}
	return result, nil
}

func (d Definition) Compile(input, output reflect.Type) (*Plan, error) {
	input = normalizeStructType(input)
	if input == nil || output == nil {
		return nil, fmt.Errorf("cube input struct and output type are required")
	}
	if strings.TrimSpace(d.View) == "" {
		return nil, fmt.Errorf("cube source view is required")
	}
	result := &Plan{target: d.Target, view: d.View, inputType: input, outputType: output, holderByName: map[string][]string{}}
	names := map[string]bool{}
	fields := map[string]bool{}
	selections := func(source []SelectionDefinition, target *[]selection) error {
		for _, item := range source {
			if item.Name == "" || names[item.Name] || fields[item.Field] {
				return fmt.Errorf("duplicate or empty cube selection %q", item.Name)
			}
			field, index, err := definitionField(input, item.Field)
			if err != nil {
				return err
			}
			if dereference(field.Type).Kind() != reflect.Bool {
				return fmt.Errorf("cube selection %s must be boolean", item.Field)
			}
			names[item.Name], fields[item.Field] = true, true
			*target = append(*target, selection{name: item.Name, index: index})
		}
		return nil
	}
	if err := selections(d.Dimensions, &result.dimensions); err != nil {
		return nil, err
	}
	if err := selections(d.Measures, &result.measures); err != nil {
		return nil, err
	}
	filterNames := map[string]bool{}
	for _, item := range d.Filters {
		if item.Name == "" || filterNames[item.Name] || fields[item.Field] {
			return nil, fmt.Errorf("duplicate or empty cube filter %q", item.Name)
		}
		field, index, err := definitionField(input, item.Field)
		if err != nil {
			return nil, err
		}
		sourceType := field.Type
		if field.Type.Kind() == reflect.Pointer && !item.SourcePointer {
			sourceType = field.Type.Elem()
		}
		if item.SourcePointer && field.Type.Kind() != reflect.Pointer {
			return nil, fmt.Errorf("cube filter %s lost nullable source type", item.Field)
		}
		if item.Location.Kind == "" {
			return nil, fmt.Errorf("cube filter %s source is required", item.Name)
		}
		filterNames[item.Name], fields[item.Field] = true, true
		presencePath := item.Field[:strings.LastIndex(item.Field, ".")+1] + "Has." + field.Name
		var presenceIndex []int
		if marker, path, err := definitionField(input, presencePath); err == nil && marker.Type.Kind() == reflect.Bool {
			presenceIndex = path
		}
		result.filters = append(result.filters, filter{name: item.Name, index: index, presenceIndex: presenceIndex, location: item.Location, sourceType: sourceType})
	}
	for _, control := range []struct {
		path   string
		typ    reflect.Type
		target *[]int
	}{{d.OrderBy, reflect.TypeFor[[]string](), &result.orderIndex}, {d.Limit, reflect.TypeFor[*int](), &result.limitIndex}, {d.Offset, reflect.TypeFor[*int](), &result.offsetIndex}} {
		field, index, err := definitionField(input, control.path)
		if err != nil {
			return nil, err
		}
		if field.Type != control.typ {
			return nil, fmt.Errorf("cube control %s requires %s", control.path, control.typ)
		}
		*control.target = index
	}
	dimensions := map[string]bool{}
	for _, item := range d.Dimensions {
		dimensions[item.Name] = true
	}
	for _, relation := range d.Relations {
		if relation.Holder == "" || len(relation.Dimensions) == 0 {
			return nil, fmt.Errorf("cube relation requires a holder and complete dimension keys")
		}
		for _, dimension := range relation.Dimensions {
			if !dimensions[dimension] {
				return nil, fmt.Errorf("cube relation %s requires unknown dimension %s", relation.Holder, dimension)
			}
			result.holderByName[dimension] = appendUnique(result.holderByName[dimension], relation.Holder)
		}
	}
	return result, nil
}

func definitionPath(owner reflect.Type, index []int) (string, error) {
	var names []string
	for _, i := range index {
		owner = normalizeStructType(owner)
		if owner == nil || i < 0 || i >= owner.NumField() {
			return "", fmt.Errorf("cube field index is invalid")
		}
		field := owner.Field(i)
		names = append(names, field.Name)
		owner = field.Type
	}
	if len(names) == 0 {
		return "", fmt.Errorf("cube field path is required")
	}
	return strings.Join(names, "."), nil
}
func definitionField(owner reflect.Type, path string) (reflect.StructField, []int, error) {
	var field reflect.StructField
	var index []int
	for _, name := range strings.Split(path, ".") {
		owner = normalizeStructType(owner)
		if owner == nil {
			return field, nil, fmt.Errorf("cube field %s requires a struct", path)
		}
		var ok bool
		field, ok = owner.FieldByName(name)
		if !ok || !field.IsExported() {
			return field, nil, fmt.Errorf("cube field %s is unavailable", path)
		}
		index = append(index, field.Index...)
		owner = field.Type
	}
	return field, index, nil
}
