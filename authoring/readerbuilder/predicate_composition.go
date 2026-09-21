package readerbuilder

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/transcribe/dql"
	sqltext "github.com/viant/sqlparser/source"
)

func inspectPredicateCompositions(source string, views []ViewOccurrence) []PredicateComposition {
	var result []PredicateComposition
	for _, view := range views {
		fragment := source[view.SourceSpan.Start:view.SourceSpan.End]
		occurrence := 0
		for pos := 0; pos < len(fragment); pos++ {
			if fragment[pos] != '$' || sqltext.ProtectionAt(fragment, pos) != "" {
				continue
			}
			prefix := "$predicate.Builder"
			braced := strings.HasPrefix(fragment[pos:], "${predicate.Builder")
			if braced {
				prefix = "${predicate.Builder"
			}
			if !strings.HasPrefix(fragment[pos:], prefix) {
				continue
			}
			cursor := skipCompositionSpace(fragment, pos+len(prefix))
			if cursor >= len(fragment) || fragment[cursor] != '(' {
				continue
			}
			composition, end := parsePredicateComposition(fragment, pos, cursor, braced)
			composition.View, composition.Occurrence = view.Name, occurrence
			composition.SourceSpan = dql.SourceSpan{Start: view.SourceSpan.Start + pos, End: view.SourceSpan.Start + end}
			result = append(result, composition)
			occurrence++
			pos = end - 1
		}
	}
	return result
}

func parsePredicateComposition(source string, start, open int, braced bool) (PredicateComposition, int) {
	result := PredicateComposition{Editable: true}
	group, cursor, ok := sqltext.ReadGroupString(source, open, '(', ')')
	if !ok {
		result.Editable, result.Reason = false, "Builder call is incomplete"
		return result, open + 1
	}
	if strings.TrimSpace(group[1:len(group)-1]) != "" {
		result.Editable, result.Reason = false, "Builder arguments are unsupported"
	}
	connector := "AND"
	for {
		pos := skipCompositionSpace(source, cursor)
		if pos >= len(source) || source[pos] != '.' {
			result.Editable, result.Reason = false, "Builder chain must end with Build"
			return result, cursor
		}
		nameStart := skipCompositionSpace(source, pos+1)
		nameEnd := nameStart
		for nameEnd < len(source) && (source[nameEnd] >= 'A' && source[nameEnd] <= 'Z' || source[nameEnd] >= 'a' && source[nameEnd] <= 'z') {
			nameEnd++
		}
		name := source[nameStart:nameEnd]
		open = skipCompositionSpace(source, nameEnd)
		group, end, valid := sqltext.ReadGroupString(source, open, '(', ')')
		if !valid {
			result.Editable, result.Reason = false, "Builder chain contains an incomplete call"
			return result, cursor
		}
		args := sqltext.SplitArgs(group[1 : len(group)-1])
		cursor = end
		switch name {
		case "And", "Or":
			if strings.TrimSpace(group[1:len(group)-1]) != "" {
				result.Editable, result.Reason = false, "Builder connectors cannot have arguments"
			}
			connector = strings.ToUpper(name)
		case "Combine", "CombineAnd", "CombineOr":
			term := PredicateCompositionTerm{Operator: "AND", Connector: connector}
			if name == "CombineOr" {
				term.Operator = "OR"
			}
			for _, argument := range args {
				id, _, valid := compositionGroup(argument)
				if !valid {
					result.Editable, result.Reason = false, "Composition contains a nested builder or non-group expression"
					continue
				}
				term.Groups = append(term.Groups, id)
			}
			if len(term.Groups) == 0 {
				result.Editable, result.Reason = false, "Composition contains an empty or unsupported term"
			}
			result.Terms = append(result.Terms, term)
		case "Build":
			if len(args) != 1 || !compositionQuoted(args[0]) {
				result.Editable, result.Reason = false, "Build keyword must be a static string"
			} else {
				result.BuildKeyword = strings.ToUpper(strings.TrimSpace(sqltext.TrimQuote(strings.TrimSpace(args[0]))))
				if result.BuildKeyword != "" && result.BuildKeyword != "WHERE" && result.BuildKeyword != "AND" {
					result.Editable, result.Reason = false, "Build keyword must be WHERE, AND, or empty"
				}
			}
			if braced {
				close := skipCompositionSpace(source, cursor)
				if close >= len(source) || source[close] != '}' {
					result.Editable, result.Reason = false, "Builder expression has no closing brace"
				} else {
					cursor = close + 1
				}
			}
			if len(result.Terms) == 0 {
				result.Editable, result.Reason = false, "Builder has no composition terms"
			}
			return result, cursor
		default:
			result.Editable, result.Reason = false, "Unsupported Builder method "+name
		}
	}
}

func compositionQuoted(value string) bool {
	value = strings.TrimSpace(value)
	return len(value) >= 2 && (value[0] == '\'' || value[0] == '"') && value[len(value)-1] == value[0]
}

func skipCompositionSpace(source string, pos int) int {
	for pos < len(source) && sqltext.IsWhitespace(source[pos]) {
		pos++
	}
	return pos
}

func compositionGroup(expression string) (int, string, bool) {
	expression = strings.TrimSpace(expression)
	for _, method := range []string{"FilterGroup", "ExpandWith", "Expand"} {
		for _, prefix := range []string{"$predicate.", "${predicate."} {
			name := prefix + method
			if !strings.HasPrefix(expression, name) {
				continue
			}
			open := skipCompositionSpace(expression, len(name))
			group, end, ok := sqltext.ReadGroupString(expression, open, '(', ')')
			if !ok {
				return 0, "", false
			}
			tail := strings.TrimSpace(expression[end:])
			if prefix == "${predicate." && tail != "}" || prefix == "$predicate." && tail != "" {
				return 0, "", false
			}
			args := sqltext.SplitArgs(group[1 : len(group)-1])
			id, valid := staticInt(args)
			if !valid || id < 0 || method == "Expand" && len(args) != 1 || method != "Expand" && len(args) != 2 {
				return 0, "", false
			}
			operator := "AND"
			if len(args) == 2 {
				if !compositionQuoted(args[1]) {
					return 0, "", false
				}
				operator = strings.ToUpper(strings.TrimSpace(sqltext.TrimQuote(strings.TrimSpace(args[1]))))
				if operator != "AND" && operator != "OR" {
					return 0, "", false
				}
			}
			return id, operator, true
		}
	}
	return 0, "", false
}

func normalizeCompositionMutation(mutation *PredicateCompositionMutation) ([]PredicateCompositionTerm, string, error) {
	if mutation == nil || strings.TrimSpace(mutation.View) == "" || mutation.Occurrence < 0 || len(mutation.Terms) == 0 {
		return nil, "", fmt.Errorf("predicate composition requires a view, non-negative occurrence, and terms")
	}
	keyword := strings.ToUpper(strings.TrimSpace(mutation.BuildKeyword))
	if keyword != "" && keyword != "WHERE" && keyword != "AND" {
		return nil, "", fmt.Errorf("predicate composition Build keyword must be WHERE, AND, or empty")
	}
	terms := make([]PredicateCompositionTerm, len(mutation.Terms))
	protected := false
	for i, term := range mutation.Terms {
		term.Operator = strings.ToUpper(strings.TrimSpace(term.Operator))
		term.Connector = strings.ToUpper(strings.TrimSpace(term.Connector))
		if (term.Operator != "AND" && term.Operator != "OR") || (term.Connector != "AND" && term.Connector != "OR") || len(term.Groups) == 0 {
			return nil, "", fmt.Errorf("predicate composition term %d requires AND/OR operator, AND/OR connector, and groups", i)
		}
		for _, id := range term.Groups {
			if id < 0 {
				return nil, "", fmt.Errorf("predicate group must be non-negative")
			}
			if id == 99 {
				protected = true
				if term.Operator != "AND" {
					return nil, "", fmt.Errorf("authorization group 99 requires an AND term")
				}
			}
		}
		terms[i] = term
	}
	if protected {
		for _, term := range terms {
			if term.Connector != "AND" {
				return nil, "", fmt.Errorf("authorization group 99 requires AND between every composition term")
			}
		}
	}
	return terms, keyword, nil
}

func updatePredicateComposition(source string, mutation *PredicateCompositionMutation) (string, error) {
	terms, keyword, err := normalizeCompositionMutation(mutation)
	if err != nil {
		return "", err
	}
	views, expansions, err := inspectViewSources(source)
	if err != nil {
		return "", err
	}
	if !containsView(views, strings.TrimSpace(mutation.View)) {
		return "", fmt.Errorf("predicate composition view %q was not found", mutation.View)
	}
	var selected *PredicateComposition
	for _, composition := range inspectPredicateCompositions(source, views) {
		if strings.EqualFold(composition.View, strings.TrimSpace(mutation.View)) && composition.Occurrence == mutation.Occurrence {
			copy := composition
			selected = &copy
			break
		}
	}
	if selected == nil {
		return "", fmt.Errorf("predicate composition %s occurrence %d was not found", mutation.View, mutation.Occurrence)
	}
	if !selected.Editable {
		return "", fmt.Errorf("predicate composition cannot be edited: %s", selected.Reason)
	}
	counts := func(terms []PredicateCompositionTerm) map[int]int {
		result := map[int]int{}
		for _, term := range terms {
			for _, id := range term.Groups {
				result[id]++
			}
		}
		return result
	}
	if !reflect.DeepEqual(counts(selected.Terms), counts(terms)) {
		return "", fmt.Errorf("predicate composition must preserve every existing group occurrence; use predicate operations to add or remove groups")
	}
	groupSource := map[int]string{}
	groupOperator := map[int]string{}
	for _, expansion := range expansions {
		if expansion.SourceSpan.Start < selected.SourceSpan.Start || expansion.SourceSpan.End > selected.SourceSpan.End {
			continue
		}
		expression := source[expansion.SourceSpan.Start:expansion.SourceSpan.End]
		if strings.HasPrefix(expression, "${") {
			expression += "}"
		}
		_, operator, valid := compositionGroup(expression)
		if !valid {
			return "", fmt.Errorf("predicate group %d has unsupported expansion", expansion.Group)
		}
		if previous, exists := groupOperator[expansion.Group]; exists && previous != operator {
			return "", fmt.Errorf("predicate group %d has ambiguous within-group operators", expansion.Group)
		}
		groupSource[expansion.Group], groupOperator[expansion.Group] = expression, operator
	}
	var rendered strings.Builder
	braced := strings.HasPrefix(source[selected.SourceSpan.Start:selected.SourceSpan.End], "${")
	if braced {
		rendered.WriteString("${predicate.Builder()")
	} else {
		rendered.WriteString("$predicate.Builder()")
	}
	for _, term := range terms {
		if term.Connector == "OR" {
			rendered.WriteString(".Or()")
		} else {
			rendered.WriteString(".And()")
		}
		if term.Operator == "OR" {
			rendered.WriteString(".CombineOr(")
		} else {
			rendered.WriteString(".CombineAnd(")
		}
		for i, id := range term.Groups {
			if i > 0 {
				rendered.WriteString(", ")
			}
			expression, exists := groupSource[id]
			if !exists {
				return "", fmt.Errorf("predicate group %d is not expanded in selected composition", id)
			}
			rendered.WriteString(expression)
		}
		rendered.WriteByte(')')
	}
	rendered.WriteString(".Build(" + strconv.Quote(keyword) + ")")
	if braced {
		rendered.WriteByte('}')
	}
	return dql.ApplyPatch(source, selected.SourceSpan, rendered.String())
}

func validatePredicateCompositionResult(mutation *PredicateCompositionMutation, structure *Structure) error {
	terms, keyword, err := normalizeCompositionMutation(mutation)
	if err != nil {
		return err
	}
	if structure != nil {
		for _, composition := range structure.PredicateCompositions {
			if strings.EqualFold(composition.View, strings.TrimSpace(mutation.View)) && composition.Occurrence == mutation.Occurrence && composition.Editable && composition.BuildKeyword == keyword && reflect.DeepEqual(composition.Terms, terms) {
				return nil
			}
		}
	}
	return fmt.Errorf("predicate composition did not compile with the requested terms and Build keyword")
}
