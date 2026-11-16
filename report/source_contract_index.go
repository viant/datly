package report

import (
	"fmt"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/output"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
	"reflect"
	"strings"
)

// sourceContractIndex is generation-owned provenance, compiled from the exact
// source route's output binding and reader projection. It contains no authority.
// Both linked and dynamically derived report facades use this index.
type sourceContractIndex struct {
	output      *output.Plan
	outputType  reflect.Type
	rowType     reflect.Type
	rowPath     string
	holderIndex []int
	fields      map[string][]int
	relations   map[string][]sourceVisibilityPath
}

func compileSourceContractIndex(component *spec.Component, outputType reflect.Type) (*sourceContractIndex, error) {
	if component == nil || component.RootView == nil {
		return nil, fmt.Errorf("report source reader metadata is required")
	}
	base, err := (output.Compiler{}).Compile(output.CompileInput{Component: component, Type: outputType})
	if err != nil {
		return nil, err
	}
	result := &sourceContractIndex{output: base, outputType: outputType, rowType: dereference(outputType), fields: map[string][]int{}}
	bindings, err := dtag.NewBindingIndex(result.rowType)
	if err != nil {
		return nil, err
	}
	for _, parameter := range component.Parameters {
		if parameter == nil || parameter.Source.Kind != "output" || parameter.Source.Name != "view" {
			continue
		}
		holder, ok, e := bindings.Resolve(parameter)
		if e != nil {
			return nil, e
		}
		if !ok {
			return nil, fmt.Errorf("report source output holder %s is missing", parameter.Name)
		}
		result.holderIndex = append([]int(nil), holder.Index...)
		result.rowType = holder.Type
		names := make([]string, 0, len(holder.Index))
		owner := dereference(outputType)
		for _, position := range holder.Index {
			field := owner.Field(position)
			names = append(names, field.Name)
			owner = dereference(field.Type)
		}
		result.rowPath = strings.Join(names, ".")
		break
	}
	for result.rowType != nil && (result.rowType.Kind() == reflect.Pointer || result.rowType.Kind() == reflect.Slice || result.rowType.Kind() == reflect.Array) {
		result.rowType = result.rowType.Elem()
	}
	if result.rowType == nil || result.rowType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("report source requires typed row output")
	}
	projection, err := (bootstrap.ViewProjection{View: component.RootView}).Columns()
	if err != nil {
		return nil, err
	}
	for _, column := range projection {
		// These names are authored/resolved reader projection identities. Never
		// connect a source filter to an output simply because their names agree.
		field, ok := typecatalog.FieldByName(result.rowType, column.Column.Name)
		if !ok {
			continue
		}
		selector, e := reportOutputIdentifier(column.Selector)
		if e != nil {
			return nil, e
		}
		sqlName, e := reportOutputIdentifier(column.Name)
		if e != nil {
			return nil, e
		}
		for _, name := range []string{selector, sqlName} {
			if previous, found := result.fields[name]; found && !reflect.DeepEqual(previous, field.Index) {
				return nil, fmt.Errorf("ambiguous report source column %s", name)
			}
			result.fields[name] = append([]int(nil), field.Index...)
		}
	}
	baseline, err := result.output.WithMCPVisibility()
	if err != nil {
		return nil, err
	}
	result.relations = map[string][]sourceVisibilityPath{}
	for _, relation := range component.RootView.Relations {
		if relation == nil || relation.Holder == "" {
			continue
		}
		field, ok := result.rowType.FieldByName(relation.Holder)
		if !ok {
			return nil, fmt.Errorf("source relation holder %s is missing", relation.Holder)
		}
		paths := []sourceVisibilityPath{{owner: result.rowType, path: result.rowPath, index: field.Index}}
		childPath, e := definitionPath(result.rowType, field.Index)
		if e != nil {
			return nil, e
		}
		childPath = joinContractPath(result.rowPath, childPath)
		if e = appendSourceVisibilityPaths(baseline, field.Type, childPath, map[reflect.Type]bool{}, &paths); e != nil {
			return nil, e
		}
		result.relations[relation.Holder] = paths
	}
	return result, nil
}

// visibleFields delegates naming, JSON dominance, embedding and MCP visibility
// to the existing output owner. Call this while preparing a metadata variant,
// never once per row or requested expression.
func (s *sourceContractIndex) visibleFields(source *exec.EffectiveContract) (map[string]bool, error) {
	selected, err := s.output.WithContract(source.Output, source.OutputRepresentation)
	if err != nil {
		return nil, err
	}
	selected, err = selected.WithMCPVisibility()
	if err != nil {
		return nil, err
	}
	return s.visibleFieldsIn(selected)
}
func (s *sourceContractIndex) visibleFieldsIn(selected *output.Plan) (map[string]bool, error) {
	holderVisible, err := s.rowHolderVisible(selected)
	if err != nil {
		return nil, err
	}
	if !holderVisible {
		return map[string]bool{}, nil
	}
	fields, err := selected.SelectorFieldNames(s.rowType, s.rowPath)
	if err != nil {
		return nil, err
	}
	indexes := make(map[string]bool, len(fields))
	for _, field := range fields {
		indexes[fmt.Sprint(field.Index)] = true
	}
	result := make(map[string]bool, len(s.fields))
	for name, index := range s.fields {
		result[name] = indexes[fmt.Sprint(index)]
	}
	return result, nil
}

type sourceContractVisibility struct {
	fields        map[string]bool
	relations     map[string]bool
	normalization *xshape.Representation
}

func (s *sourceContractIndex) prepareVisibility(source *exec.EffectiveContract) (*sourceContractVisibility, error) {
	selected, err := s.output.WithContract(source.Output, source.OutputRepresentation)
	if err != nil {
		return nil, err
	}
	selected, err = selected.WithMCPVisibility()
	if err != nil {
		return nil, err
	}
	fields, err := s.visibleFieldsIn(selected)
	if err != nil {
		return nil, err
	}
	result := &sourceContractVisibility{fields: fields, relations: map[string]bool{}}
	for holder, paths := range s.relations {
		visible := len(fields) > 0
		for _, entry := range paths {
			public, e := selected.SelectorFieldNames(entry.owner, entry.path)
			if e != nil {
				return nil, e
			}
			found := false
			for _, field := range public {
				if reflect.DeepEqual(field.Index, entry.index) {
					found = true
					break
				}
			}
			if !found {
				visible = false
				break
			}
		}
		result.relations[holder] = visible
	}
	return result, nil
}

type sourceVisibilityPath struct {
	owner reflect.Type
	path  string
	index []int
}

func appendSourceVisibilityPaths(plan *output.Plan, owner reflect.Type, path string, active map[reflect.Type]bool, result *[]sourceVisibilityPath) error {
	for owner != nil && (owner.Kind() == reflect.Pointer || owner.Kind() == reflect.Slice || owner.Kind() == reflect.Array) {
		owner = owner.Elem()
	}
	if owner == nil || owner.Kind() != reflect.Struct || active[owner] {
		return nil
	}
	active[owner] = true
	defer delete(active, owner)
	fields, err := plan.SelectorFieldNames(owner, path)
	if err != nil {
		return err
	}
	for _, field := range fields {
		*result = append(*result, sourceVisibilityPath{owner: owner, path: path, index: field.Index})
		linked := owner.FieldByIndex(field.Index)
		childPath, e := definitionPath(owner, field.Index)
		if e != nil {
			return e
		}
		if e = appendSourceVisibilityPaths(plan, linked.Type, joinContractPath(path, childPath), active, result); e != nil {
			return e
		}
	}
	return nil
}

func (s *sourceContractIndex) rowHolderVisible(selected *output.Plan) (bool, error) {
	remaining := s.holderIndex
	owner := s.outputType
	path := ""
	for len(remaining) > 0 {
		owner = dereference(owner)
		fields, err := selected.SelectorFieldNames(owner, path)
		if err != nil {
			return false, err
		}
		matched := false
		for _, field := range fields {
			if len(field.Index) > len(remaining) || !reflect.DeepEqual(field.Index, remaining[:len(field.Index)]) {
				continue
			}
			branch, err := definitionPath(owner, field.Index)
			if err != nil {
				return false, err
			}
			path = joinContractPath(path, branch)
			owner = owner.FieldByIndex(field.Index).Type
			remaining = remaining[len(field.Index):]
			matched = true
			break
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func (s *sourceContractVisibility) ContractInputNormalization() *xshape.Representation {
	return s.normalization
}
