package readerbuilder

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/tagly/tags"
)

func (s *Service) setColumnRole(source string, mutation *ColumnRoleMutation) (string, error) {
	if mutation == nil || !validIdentifier(strings.TrimSpace(mutation.View)) || !validIdentifier(strings.TrimSpace(mutation.Column)) {
		return "", fmt.Errorf("column role view and column are required identifiers")
	}
	role := strings.ToLower(strings.TrimSpace(mutation.Role))
	if role != "dimension" && role != "measure" {
		return "", fmt.Errorf("column role must be dimension or measure")
	}
	target := strings.TrimSpace(mutation.View) + "." + strings.TrimSpace(mutation.Column)
	functions, err := inspectFunctions(source)
	if err != nil {
		return "", err
	}
	var matching *FunctionOccurrence
	for index := range functions {
		function := &functions[index]
		if !strings.EqualFold(function.Name, "tag") || len(function.Args) != 2 || !strings.EqualFold(strings.TrimSpace(function.Args[0]), target) {
			continue
		}
		if matching == nil {
			matching = function
		}
		literal := strings.TrimSpace(sqltext.TrimQuote(function.Args[1]))
		parsed, parseErr := tags.Parse(literal)
		if parseErr != nil {
			return "", fmt.Errorf("column %s tag: %w", target, parseErr)
		}
		index := parsed.Index("groupable")
		if index < 0 {
			continue
		}
		if role == "dimension" {
			parsed.Set("groupable", "true")
		} else {
			parsed.Set("groupable", "false")
		}
		args := []string{target, quoteDQLString(parsed.Literal())}
		return s.editFunction(source, OperationUpdateFunction, &FunctionMutation{Name: "tag", Args: args, ExpectedArgs: function.Args, Occurrence: function.Occurrence})
	}
	if matching != nil {
		literal := strings.TrimSpace(sqltext.TrimQuote(matching.Args[1]))
		parsed, parseErr := tags.Parse(literal)
		if parseErr != nil {
			return "", fmt.Errorf("column %s tag: %w", target, parseErr)
		}
		parsed.Set("groupable", map[bool]string{true: "true", false: "false"}[role == "dimension"])
		args := []string{target, quoteDQLString(parsed.Literal())}
		return s.editFunction(source, OperationUpdateFunction, &FunctionMutation{Name: "tag", Args: args, ExpectedArgs: matching.Args, Occurrence: matching.Occurrence})
	}
	value := "true"
	if role == "measure" {
		value = "false"
	}
	return s.editFunction(source, OperationAddFunction, &FunctionMutation{Name: "tag", Args: []string{target, quoteDQLString(`groupable:"` + value + `"`)}})
}

func quoteDQLString(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

func compiledColumnGroupable(column *spec.Column) bool {
	if column == nil {
		return false
	}
	if column.Groupable != nil {
		return *column.Groupable
	}
	tag := tags.NewTags(strings.TrimSpace(column.Tag)).Lookup("groupable")
	return tag != nil && strings.EqualFold(strings.TrimSpace(string(tag.Values)), "true")
}
