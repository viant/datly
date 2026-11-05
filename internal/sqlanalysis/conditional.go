package sqlanalysis

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/parsly"
	sqlsource "github.com/viant/sqlparser/source"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

// ConditionalSQL returns analysis-only branch alternatives. Conditions and value
// expressions are never executed. Callers must validate every alternative, not
// infer one table/projection from a chosen runtime branch. Authored SQL is kept.
func ConditionalSQL(source string, validators ...InputValidator) ([]string, error) {
	prefix := "__datly_conditional_literal_"
	for strings.Contains(source, prefix) {
		prefix += "_"
	}
	escape := strings.NewReplacer("$", prefix+"dollar", "#", prefix+"hash")
	restore := strings.NewReplacer(prefix+"dollar", "$", prefix+"hash", "#")
	scanner := sqlsource.NewCodeScanner(source, 0)
	var masked strings.Builder
	previous, depth, count := 0, 0, 0
	var stack []string
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
		case "if", "elseif", "foreach", "set":
			if keyword == "elseif" && (depth == 0 || stack[len(stack)-1] != "if") {
				return nil, fmt.Errorf("unmatched conditional SQL #elseif at byte %d", pos)
			}
			count++
			if keyword == "if" || keyword == "foreach" {
				depth++
				stack = append(stack, keyword)
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
			if depth == 0 || stack[len(stack)-1] != "if" {
				return nil, fmt.Errorf("unmatched conditional SQL #else at byte %d", pos)
			}
		case "end":
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unmatched conditional SQL #end at byte %d", pos)
			}
			stack = stack[:len(stack)-1]
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
	ctx := &conditionalContext{restore: restore.Replace, markers: map[string]string{}, prefix: prefix, locals: map[string]bool{}}
	ctx.collectLocals(block)
	if len(validators) > 0 {
		ctx.validate = validators[0]
	}
	variants, err := ctx.block(block, []conditionalVariant{{bindings: map[string]string{}}}, 0)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(variants))
	seen := map[string]bool{}
	for _, text := range variants {
		text := restore.Replace(text.sql)
		if len(ctx.markers) > 0 && strings.TrimSpace(text) != "" {
			_, parsed, err := Parse(text, ctx.validate)
			if err != nil {
				return nil, err
			}
			markers := map[string]string{}
			for marker, keyword := range ctx.markers {
				if sqlsource.Token(marker).Contains(text) {
					markers[marker] = keyword
				}
			}
			if err = validateAnalysisPredicates(parsed, markers, nil); err != nil {
				return nil, err
			}
		}
		if !seen[text] {
			result = append(result, text)
			seen[text] = true
		}
	}
	return result, nil
}

type conditionalVariant struct {
	sql      string
	bindings map[string]string
}
type conditionalContext struct {
	restore  func(string) string
	validate InputValidator
	markers  map[string]string
	prefix   string
	locals   map[string]bool
}

func cloneBindings(src map[string]string) map[string]string {
	dst := map[string]string{}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
func (c *conditionalContext) block(block *stmt.Block, states []conditionalVariant, depth int) ([]conditionalVariant, error) {
	if depth > 32 {
		return nil, fmt.Errorf("conditional SQL exceeds 32 levels")
	}
	for _, node := range block.Stmt {
		var next []conditionalVariant
		seen := map[string]bool{}
		for _, state := range states {
			var alternatives []conditionalVariant
			switch value := node.(type) {
			case *stmt.Append:
				text, err := c.append(c.restore(value.Append), state.bindings)
				if err != nil {
					return nil, err
				}
				alternatives = []conditionalVariant{{sql: text, bindings: state.bindings}}
			case *stmt.Statement:
				target, ok := value.X.(*expr.Select)
				if !ok || target.X != nil || fmt.Sprint(value.Op) != "=" {
					return nil, fmt.Errorf("unsupported conditional SQL assignment")
				}
				source, err := c.selector(value.Y, state.bindings)
				if err != nil {
					return nil, err
				}
				bindings := cloneBindings(state.bindings)
				bindings[target.ID] = source
				alternatives = []conditionalVariant{{bindings: bindings}}
			case *stmt.If:
				unconditional := false
				for branch := value; branch != nil; branch = branch.Else {
					truth, known := conditionalBool(branch.Condition)
					if selector, ok := branch.Condition.(*expr.Select); ok {
						if v, found := state.bindings[selectorPath(selector)]; found && (v == "true" || v == "false") {
							truth, known = v == "true", true
						}
					}
					if !known || truth {
						bodies, err := c.block(&branch.Body, []conditionalVariant{{bindings: cloneBindings(state.bindings)}}, depth+1)
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
					alternatives = append(alternatives, conditionalVariant{bindings: state.bindings})
				}
			case *stmt.ForEach:
				if value.Index != nil {
					return nil, fmt.Errorf("indexed foreach requires bound discovery")
				}
				source, err := c.selector(value.Set, state.bindings)
				if err != nil {
					return nil, err
				}
				if c.validate == nil {
					return nil, fmt.Errorf("foreach predicate analysis requires declared inputs")
				}
				cursor := parsly.NewCursor("", []byte("{"+source+"[0]}"), 0)
				selector, parseErr := veltyparser.MatchSelector(cursor)
				if parseErr != nil {
					return nil, parseErr
				}
				if _, err = c.validate(selector); err != nil {
					return nil, err
				}
				marker := fmt.Sprintf("%sloop_%d", c.prefix, len(c.markers))
				c.markers[marker] = ""
				// One item checks the terminal body; two check both HasNext branches
				// and their join. No runtime collection is read or expanded. Empty
				// collections remain owned by the authored surrounding condition.
				for _, size := range []int{1, 2} {
					bodies := []conditionalVariant{{bindings: cloneBindings(state.bindings)}}
					for i := 0; i < size; i++ {
						for j := range bodies {
							bodies[j].bindings = cloneBindings(bodies[j].bindings)
							bodies[j].bindings[value.Item.ID] = source + "[0]"
							bodies[j].bindings["foreach.HasNext"] = fmt.Sprint(i+1 < size)
						}
						bodies, err = c.block(&value.Body, bodies, depth+1)
						if err != nil {
							return nil, err
						}
					}
					for _, body := range bodies {
						alternatives = append(alternatives, conditionalVariant{sql: "((" + marker + " = 1) AND (" + body.sql + "))", bindings: state.bindings})
					}
				}
			default:
				return nil, fmt.Errorf("unsupported conditional SQL analysis node %T", node)
			}
			for _, alternative := range alternatives {
				alternative.sql = state.sql + alternative.sql
				key := alternative.sql + fmt.Sprint(alternative.bindings)
				if seen[key] {
					continue
				}
				seen[key] = true
				if len(next) == 64 {
					return nil, fmt.Errorf("conditional SQL exceeds 64 distinct branch alternatives; requires bound discovery")
				}
				next = append(next, alternative)
			}
		}
		states = next
	}
	return states, nil
}

// selectorPath names a native control selector without evaluating it.
func selectorPath(s *expr.Select) string {
	p := s.ID
	if child, ok := s.X.(*expr.Select); ok {
		p += "." + selectorPath(child)
	}
	return p
}
func (c *conditionalContext) selector(value ast.Expression, bindings map[string]string) (string, error) {
	s, ok := value.(*expr.Select)
	if !ok {
		return "", fmt.Errorf("conditional SQL bindings require input member selectors")
	}
	path := s.ID
	if bound, ok := bindings[s.ID]; ok {
		path = bound
	}
	for child := s.X; child != nil; {
		member, ok := child.(*expr.Select)
		if !ok {
			return "", fmt.Errorf("unsupported conditional SQL binding expression")
		}
		path += "." + member.ID
		child = member.X
	}
	return path, nil
}
func (c *conditionalContext) append(text string, bindings map[string]string) (string, error) {
	scanner := sqlsource.NewCodeScanner(text, 0)
	previous := 0
	var out strings.Builder
	for pos, ok := scanner.Next(); ok; pos, ok = scanner.Next() {
		if pos < previous || text[pos] != '$' {
			continue
		}
		cursor := parsly.NewCursor("", []byte(text), 0)
		cursor.Pos = pos + 1
		selector, err := veltyparser.MatchSelector(cursor)
		if err != nil {
			continue
		}
		if _, found := bindings[selector.ID]; !found {
			if c.locals[selector.ID] {
				return "", fmt.Errorf("conditional SQL local %s is out of scope or unbound", selector.ID)
			}
			continue
		}
		path, err := c.selector(selector, bindings)
		if err != nil {
			return "", err
		}
		out.WriteString(text[previous:pos])
		out.WriteString("${" + path + "}")
		previous = cursor.Pos
	}
	out.WriteString(text[previous:])
	return out.String(), nil
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

func (c *conditionalContext) collectLocals(block *stmt.Block) {
	for _, node := range block.Stmt {
		switch value := node.(type) {
		case *stmt.ForEach:
			c.locals[value.Item.ID] = true
			c.locals["foreach"] = true
			c.collectLocals(&value.Body)
		case *stmt.Statement:
			if target, ok := value.X.(*expr.Select); ok {
				c.locals[target.ID] = true
			}
		case *stmt.If:
			for branch := value; branch != nil; branch = branch.Else {
				c.collectLocals(&branch.Body)
			}
		}
	}
}
