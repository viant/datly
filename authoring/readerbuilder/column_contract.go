package readerbuilder

import (
	"fmt"
	"sort"
	"strings"

	"github.com/viant/sqlparser"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/tagly/tags"
)

func inspectColumnContracts(functions []FunctionOccurrence) []ColumnContract {
	items := map[string]*ColumnContract{}
	for _, function := range functions {
		name := strings.ToLower(strings.TrimSpace(function.Name))
		if (name != "cast" && name != "tag") || len(function.Args) != 2 {
			continue
		}
		parts, err := sqlparser.TableIdentifierParts(strings.TrimSpace(function.Args[0]))
		if err != nil || len(parts) != 2 {
			continue
		}
		key := strings.ToLower(parts[0]) + "\x00" + strings.ToLower(parts[1])
		item := items[key]
		if item == nil {
			item = &ColumnContract{View: parts[0], Column: parts[1]}
			items[key] = item
		}
		if name == "cast" {
			item.CastType = strings.Trim(strings.TrimSpace(function.Args[1]), "'\"")
			continue
		}
		parsed, err := tags.Parse(strings.TrimSpace(sqltext.TrimQuote(function.Args[1])))
		if err != nil {
			continue
		}
		if item.Tags == nil {
			item.Tags = map[string]string{}
		}
		for _, tag := range parsed {
			if tag != nil {
				item.Tags[tag.Name] = string(tag.Values)
			}
		}
	}
	result := make([]ColumnContract, 0, len(items))
	for _, item := range items {
		result = append(result, *item)
	}
	sort.Slice(result, func(i, j int) bool {
		if !strings.EqualFold(result[i].View, result[j].View) {
			return strings.ToLower(result[i].View) < strings.ToLower(result[j].View)
		}
		return strings.ToLower(result[i].Column) < strings.ToLower(result[j].Column)
	})
	return result
}

func (s *Service) setColumnContract(source string, mutation *ColumnContractMutation) (string, error) {
	if mutation == nil || !validIdentifier(strings.TrimSpace(mutation.View)) || !validIdentifier(strings.TrimSpace(mutation.Column)) {
		return "", fmt.Errorf("column contract view and column are required identifiers")
	}
	for name := range mutation.Tags {
		if !validIdentifier(strings.TrimSpace(name)) {
			return "", fmt.Errorf("column contract tag name %q is invalid", name)
		}
	}
	for _, name := range mutation.RemoveTags {
		if !validIdentifier(strings.TrimSpace(name)) {
			return "", fmt.Errorf("column contract remove tag name %q is invalid", name)
		}
	}
	target := strings.TrimSpace(mutation.View) + "." + strings.TrimSpace(mutation.Column)
	result := source
	var err error
	if mutation.CastType != nil {
		result, err = s.upsertColumnFunction(result, "cast", target, strings.TrimSpace(*mutation.CastType))
		if err != nil {
			return "", err
		}
	}
	if len(mutation.Tags) == 0 && len(mutation.RemoveTags) == 0 {
		return result, nil
	}
	functions, err := inspectFunctions(result)
	if err != nil {
		return "", err
	}
	var current *FunctionOccurrence
	for index := range functions {
		item := &functions[index]
		if strings.EqualFold(item.Name, "tag") && len(item.Args) == 2 && strings.EqualFold(strings.TrimSpace(item.Args[0]), target) {
			if current != nil {
				return "", fmt.Errorf("column %s has more than one tag function; consolidate it before structured editing", target)
			}
			current = item
		}
	}
	values := tags.Tags{}
	if current != nil {
		values, err = tags.Parse(strings.TrimSpace(sqltext.TrimQuote(current.Args[1])))
		if err != nil {
			return "", fmt.Errorf("column %s tag: %w", target, err)
		}
	}
	remove := map[string]bool{}
	for _, name := range mutation.RemoveTags {
		remove[strings.TrimSpace(name)] = true
	}
	filtered := make(tags.Tags, 0, len(values))
	for _, item := range values {
		if item != nil && !remove[item.Name] {
			filtered = append(filtered, item)
		}
	}
	values = filtered
	names := make([]string, 0, len(mutation.Tags))
	for name := range mutation.Tags {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		values.SetTag(&tags.Tag{Name: name, Values: tags.Values(mutation.Tags[name])})
	}
	literal := strings.TrimSpace(values.Literal())
	if current == nil {
		if literal == "" {
			return result, nil
		}
		return s.editFunction(result, OperationAddFunction, &FunctionMutation{Name: "tag", Args: []string{target, quoteDQLString(literal)}})
	}
	operation := OperationUpdateFunction
	args := []string{target, quoteDQLString(literal)}
	if literal == "" {
		operation = OperationRemoveFunction
		args = current.Args
	}
	return s.editFunction(result, operation, &FunctionMutation{Name: "tag", Args: args, ExpectedArgs: current.Args, Occurrence: current.Occurrence})
}

func (s *Service) upsertColumnFunction(source, name, target, value string) (string, error) {
	functions, err := inspectFunctions(source)
	if err != nil {
		return "", err
	}
	var current *FunctionOccurrence
	for index := range functions {
		item := &functions[index]
		if strings.EqualFold(item.Name, name) && len(item.Args) == 2 && strings.EqualFold(strings.TrimSpace(item.Args[0]), target) {
			if current != nil {
				return "", fmt.Errorf("column %s has more than one %s function", target, name)
			}
			current = item
		}
	}
	if current == nil {
		if value == "" {
			return source, nil
		}
		return s.editFunction(source, OperationAddFunction, &FunctionMutation{Name: name, Args: []string{target, value}})
	}
	if value == "" {
		return s.editFunction(source, OperationRemoveFunction, &FunctionMutation{Name: name, Args: current.Args, ExpectedArgs: current.Args, Occurrence: current.Occurrence})
	}
	return s.editFunction(source, OperationUpdateFunction, &FunctionMutation{Name: name, Args: []string{target, value}, ExpectedArgs: current.Args, Occurrence: current.Occurrence})
}
