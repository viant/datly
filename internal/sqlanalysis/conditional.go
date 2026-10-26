package sqlanalysis

import (
	"fmt"
	"reflect"
	"strings"

	sqlsource "github.com/viant/sqlparser/source"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

// ConditionalSQL returns analysis-only branch alternatives. Conditions and value
// expressions are never executed. Callers must validate every alternative, not
// infer one table/projection from a chosen runtime branch. Authored SQL is kept.
func ConditionalSQL(source string) ([]string, error) {
	prefix := "__datly_conditional_literal_"
	for strings.Contains(source, prefix) {
		prefix += "_"
	}
	escape := strings.NewReplacer("$", prefix+"dollar", "#", prefix+"hash")
	restore := strings.NewReplacer(prefix+"dollar", "$", prefix+"hash", "#")
	scanner := sqlsource.NewCodeScanner(source, 0)
	var masked strings.Builder
	previous, depth, count := 0, 0, 0
	for pos, ok := scanner.Next(); ok; pos, ok = scanner.Next() {
		if pos < previous || source[pos] != '#' {
			continue
		}
		end := pos + 1
		for end < len(source) && ((source[end] >= 'a' && source[end] <= 'z') || (source[end] >= 'A' && source[end] <= 'Z') || source[end] == '_') {
			end++
		}
		keyword := source[pos+1 : end]
		switch keyword {
		case "if", "elseif":
			if keyword == "elseif" && depth == 0 {
				return nil, fmt.Errorf("unmatched conditional SQL #elseif at byte %d", pos)
			}
			count++
			if keyword == "if" {
				depth++
			}
			if depth > 32 || count > 64 {
				return nil, fmt.Errorf("conditional SQL requires bounded discovery (maximum 32 levels and 64 conditions)")
			}
			start := end
			for start < len(source) && sqlsource.IsWhitespace(source[start]) {
				start++
			}
			if start == len(source) || source[start] != '(' {
				return nil, fmt.Errorf("conditional SQL #%s requires parentheses at byte %d", keyword, pos)
			}
			level, closed := 0, false
			for at, more := scanner.Next(); more; at, more = scanner.Next() {
				if at < start {
					continue
				}
				if source[at] == '(' {
					level++
				}
				if source[at] == ')' {
					level--
					if level == 0 {
						end = at + 1
						closed = true
						break
					}
				}
			}
			if !closed {
				return nil, fmt.Errorf("unclosed conditional SQL #%s at byte %d", keyword, pos)
			}
		case "else":
			if depth == 0 {
				return nil, fmt.Errorf("unmatched conditional SQL #else at byte %d", pos)
			}
		case "end":
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unmatched conditional SQL #end at byte %d", pos)
			}
		default:
			continue
		}
		masked.WriteString(escape.Replace(source[previous:pos]))
		masked.WriteString(source[pos:end])
		previous = end
	}
	if count == 0 {
		return []string{source}, nil
	}
	if depth != 0 {
		return nil, fmt.Errorf("unclosed conditional SQL #if")
	}
	masked.WriteString(escape.Replace(source[previous:]))
	block, err := veltyparser.Parse([]byte(masked.String()))
	if err != nil {
		return nil, fmt.Errorf("parse conditional SQL: %w", err)
	}
	variants, err := conditionalBlock(block, 0)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(variants))
	seen := map[string]bool{}
	for _, text := range variants {
		text = restore.Replace(text)
		if !seen[text] {
			result = append(result, text)
			seen[text] = true
		}
	}
	return result, nil
}

func conditionalBlock(block *stmt.Block, depth int) ([]string, error) {
	if depth > 32 {
		return nil, fmt.Errorf("conditional SQL exceeds 32 levels")
	}
	result := []string{""}
	for _, node := range block.Stmt {
		var alternatives []string
		switch value := node.(type) {
		case *stmt.Append:
			alternatives = []string{value.Append}
		case *stmt.If:
			unconditional := false
			for branch := value; branch != nil; branch = branch.Else {
				truth, known := conditionalBool(branch.Condition)
				if !known || truth {
					bodies, err := conditionalBlock(&branch.Body, depth+1)
					if err != nil {
						return nil, err
					}
					alternatives = append(alternatives, bodies...)
				}
				if known && truth {
					unconditional = true
					break
				}
			}
			if !unconditional {
				alternatives = append(alternatives, "")
			}
		default:
			return nil, fmt.Errorf("unsupported conditional SQL analysis node %T", node)
		}
		next := make([]string, 0)
		seen := map[string]bool{}
		for _, left := range result {
			for _, right := range alternatives {
				text := left + right
				if seen[text] {
					continue
				}
				if len(next) == 64 {
					return nil, fmt.Errorf("conditional SQL exceeds 64 distinct branch alternatives; requires bound discovery")
				}
				seen[text] = true
				next = append(next, text)
			}
		}
		result = next
	}
	return result, nil
}

// Velty represents #else as true == true. Only boolean constants are folded;
// selectors, method calls and other expressions remain opaque alternatives.
func conditionalBool(value ast.Expression) (bool, bool) {
	switch actual := value.(type) {
	case *expr.Literal:
		if actual.RType == reflect.TypeFor[bool]() {
			return actual.Value == "true", true
		}
	case *expr.Binary:
		if actual.Token == "==" {
			left, lk := conditionalBool(actual.X)
			right, rk := conditionalBool(actual.Y)
			if lk && rk {
				return left == right, true
			}
		}
	}
	return false, false
}
