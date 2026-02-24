package compile

import (
	"strconv"
	"strings"

	"github.com/viant/datly/repository/shape/plan"
)

func appendDeclaredStates(rawDQL string, result *plan.Result) {
	if result == nil || strings.TrimSpace(rawDQL) == "" {
		return
	}
	seen := map[string]bool{}
	for _, block := range extractSetBlocks(rawDQL) {
		holder, kind, location, tail, ok := parseSetDeclarationBody(block.Body)
		if !ok {
			continue
		}
		if kind == "view" || kind == "data_view" {
			continue
		}
		key := declaredStateKey(holder, kind, location)
		if seen[key] {
			continue
		}
		state := &plan.State{
			Path: holder,
			Name: holder,
			Kind: kind,
			In:   location,
		}
		switch strings.ToLower(kind) {
		case "query":
			required := false
			state.Required = &required
		case "header":
			required := true
			state.Required = &required
		}
		applyDeclaredStateOptions(state, tail)
		result.States = append(result.States, state)
		seen[key] = true
	}
}

func declaredStateKey(name, kind, in string) string {
	return strings.ToLower(strings.TrimSpace(name)) + "|" +
		strings.ToLower(strings.TrimSpace(kind)) + "|" +
		strings.ToLower(strings.TrimSpace(in))
}

func applyDeclaredStateOptions(state *plan.State, tail string) {
	if state == nil || strings.TrimSpace(tail) == "" {
		return
	}
	cursor := newOptionCursor(tail)
	for cursor.next() {
		name, args := cursor.option()
		switch {
		case strings.EqualFold(name, "WithURI"):
			if len(args) == 1 {
				state.URI = trimQuote(args[0])
			}
		case strings.EqualFold(name, "Optional"):
			required := false
			state.Required = &required
		case strings.EqualFold(name, "Required"):
			required := true
			state.Required = &required
		case strings.EqualFold(name, "Cacheable"):
			if len(args) == 1 {
				if value, err := strconv.ParseBool(strings.TrimSpace(trimQuote(args[0]))); err == nil {
					state.Cacheable = &value
				}
			}
		case strings.EqualFold(name, "QuerySelector"):
			if len(args) == 1 {
				state.QuerySelector = trimQuote(args[0])
				if state.Cacheable == nil {
					cacheable := false
					state.Cacheable = &cacheable
				}
			}
		case strings.EqualFold(name, "WithPredicate"), strings.EqualFold(name, "Predicate"):
			appendStatePredicate(state, args, false)
		case strings.EqualFold(name, "EnsurePredicate"):
			appendStatePredicate(state, args, true)
		case strings.EqualFold(name, "When"):
			if len(args) == 1 {
				state.When = trimQuote(args[0])
			}
		case strings.EqualFold(name, "Scope"):
			if len(args) == 1 {
				state.Scope = trimQuote(args[0])
			}
		case strings.EqualFold(name, "WithType"):
			if len(args) == 1 {
				state.DataType = trimQuote(args[0])
			}
		case strings.EqualFold(name, "Value"):
			if len(args) == 1 {
				state.Value = trimQuote(args[0])
			}
		case strings.EqualFold(name, "Async"):
			state.Async = true
		}
	}
}

func appendStatePredicate(state *plan.State, args []string, ensure bool) {
	if state == nil || len(args) == 0 {
		return
	}
	group := 0
	nameIdx := 0
	if len(args) >= 2 {
		if parsed, err := strconv.Atoi(strings.TrimSpace(trimQuote(args[0]))); err == nil {
			group = parsed
			nameIdx = 1
		}
	}
	if len(args) <= nameIdx {
		return
	}
	predicate := &plan.StatePredicate{
		Group:     group,
		Name:      trimQuote(args[nameIdx]),
		Ensure:    ensure,
		Arguments: []string{},
	}
	for _, arg := range args[nameIdx+1:] {
		predicate.Arguments = append(predicate.Arguments, trimQuote(arg))
	}
	state.Predicates = append(state.Predicates, predicate)
}

type optionCursor struct {
	raw    string
	cursor int
	name   string
	args   []string
}

func newOptionCursor(raw string) *optionCursor {
	return &optionCursor{raw: raw}
}

func (o *optionCursor) next() bool {
	o.name = ""
	o.args = nil
	for o.cursor < len(o.raw) && (o.raw[o.cursor] == ' ' || o.raw[o.cursor] == '\n' || o.raw[o.cursor] == '\t' || o.raw[o.cursor] == '\r') {
		o.cursor++
	}
	if o.cursor >= len(o.raw) || o.raw[o.cursor] != '.' {
		return false
	}
	o.cursor++
	start := o.cursor
	for o.cursor < len(o.raw) {
		ch := o.raw[o.cursor]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			o.cursor++
			continue
		}
		break
	}
	if o.cursor == start {
		return false
	}
	o.name = strings.TrimSpace(o.raw[start:o.cursor])
	for o.cursor < len(o.raw) && (o.raw[o.cursor] == ' ' || o.raw[o.cursor] == '\n' || o.raw[o.cursor] == '\t' || o.raw[o.cursor] == '\r') {
		o.cursor++
	}
	if o.cursor >= len(o.raw) || o.raw[o.cursor] != '(' {
		return false
	}
	groupStart := o.cursor
	depth := 0
	inSingle := false
	inDouble := false
	escape := false
	for o.cursor < len(o.raw) {
		ch := o.raw[o.cursor]
		if escape {
			escape = false
			o.cursor++
			continue
		}
		switch ch {
		case '\\':
			escape = true
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
		case ')':
			if !inSingle && !inDouble {
				depth--
				if depth == 0 {
					o.cursor++
					content := o.raw[groupStart+1 : o.cursor-1]
					o.args = splitArgs(content)
					return true
				}
			}
		}
		o.cursor++
	}
	return false
}

func (o *optionCursor) option() (string, []string) {
	return o.name, o.args
}
