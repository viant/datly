// Package executionhint renders reviewed planning options on detached final SQL.
package executionhint

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/viant/datly/spec"
)

const assignment = "condition_fanout_filter=off"

var switchAssignment = regexp.MustCompile(`(?i)^\s*([a-z_][a-z_0-9]*)\s*=\s*(on|off|default)\s*$`)

// Render never changes logical/source SQL. Its output is both the execution
// statement and the exact-query lazy-cache key; it is not a warmup identity.
func Render(sql, product string, options *spec.ExecutionOptions) (string, error) {
	if err := options.Validate(); err != nil {
		return "", err
	}
	if !options.Active(product) {
		return sql, nil
	}
	tokens, err := lex(sql)
	if err != nil {
		return "", fmt.Errorf("render MySQL execution hint: %w", err)
	}
	depth, selectIndex := 0, -1
	for _, token := range tokens {
		if token.kind == 'c' {
			continue
		}
		if token.kind != 'w' || !strings.EqualFold(token.text, "SELECT") && !strings.EqualFold(token.text, "WITH") {
			return "", fmt.Errorf("MySQL execution hints require a SELECT statement")
		}
		break
	}
	for i, token := range tokens {
		if token.kind != 'w' && token.kind != 'p' {
			continue
		}
		if token.text == "(" {
			depth++
		} else if token.text == ")" {
			depth--
		} else if depth == 0 && strings.EqualFold(token.text, "SELECT") {
			selectIndex = i
			break
		}
	}
	if selectIndex == -1 {
		return "", fmt.Errorf("MySQL execution hints require a final outer SELECT")
	}
	selectToken := tokens[selectIndex]
	var hint *sqlToken
	for i := selectIndex + 1; i < len(tokens) && tokens[i].kind == 'c'; i++ {
		if strings.HasPrefix(tokens[i].text, "/*+") {
			if hint != nil {
				return "", fmt.Errorf("multiple outer optimizer hint comments cannot be merged safely")
			}
			hint = &tokens[i]
		}
	}
	if hint == nil {
		return sql[:selectToken.end] + " /*+ SET_VAR(optimizer_switch='" + assignment + "') */" + sql[selectToken.end:], nil
	}
	content := hint.text[3 : len(hint.text)-2]
	merged, err := merge(content)
	if err != nil {
		return "", err
	}
	return sql[:hint.start] + "/*+" + merged + "*/" + sql[hint.end:], nil
}

func merge(content string) (string, error) {
	tokens, err := lex(content)
	if err != nil {
		return "", fmt.Errorf("invalid existing optimizer hint: %w", err)
	}
	var value *sqlToken
	for _, token := range tokens {
		if token.kind == 'p' && token.text == ";" {
			return "", fmt.Errorf("invalid optimizer hint terminator")
		}
	}
	for i := 0; i < len(tokens); i++ {
		if !strings.EqualFold(tokens[i].text, "SET_VAR") || tokens[i].kind != 'w' {
			continue
		}
		if i+1 >= len(tokens) || tokens[i+1].text != "(" {
			return "", fmt.Errorf("unsupported SET_VAR hint syntax")
		}
		end, depth := i+2, 1
		for ; end < len(tokens); end++ {
			if tokens[end].kind == 'p' {
				if tokens[end].text == "(" {
					depth++
				}
				if tokens[end].text == ")" {
					depth--
				}
			}
			if depth == 0 {
				break
			}
		}
		if end >= len(tokens) {
			return "", fmt.Errorf("unterminated SET_VAR hint")
		}
		args := tokens[i+2 : end]
		for _, arg := range args {
			if (arg.kind == 'q' || arg.kind == 's') && strings.EqualFold(strings.Trim(arg.text, "`\"'"), "optimizer_switch") {
				return "", fmt.Errorf("quoted optimizer_switch cannot be merged safely")
			}
			if strings.EqualFold(arg.text, "optimizer_switch") && (len(args) == 0 || !strings.EqualFold(args[0].text, "optimizer_switch")) {
				return "", fmt.Errorf("qualified optimizer_switch cannot be merged safely")
			}
		}
		if len(args) > 0 && strings.EqualFold(args[0].text, "optimizer_switch") {
			if value != nil || len(args) != 3 || args[1].text != "=" || args[2].kind != 's' || args[2].text[0] != '\'' {
				return "", fmt.Errorf("optimizer_switch requires one unambiguous single-quoted SET_VAR assignment")
			}
			value = &tokens[i+4]
		}
		i = end
	}
	if value == nil {
		return content + " SET_VAR(optimizer_switch='" + assignment + "') ", nil
	}
	// Only a plain switch list is mergeable. Escapes, expressions, session
	// qualifiers and duplicate switches are rejected instead of reinterpreted.
	inner := value.text[1 : len(value.text)-1]
	seen := map[string]bool{}
	found := false
	for _, item := range strings.Split(inner, ",") {
		parts := switchAssignment.FindStringSubmatch(item)
		if parts == nil {
			return "", fmt.Errorf("optimizer_switch cannot be merged safely: %q", item)
		}
		name, state := strings.ToLower(parts[1]), strings.ToLower(parts[2])
		if seen[name] {
			return "", fmt.Errorf("duplicate optimizer_switch option %q", name)
		}
		seen[name] = true
		if name == "condition_fanout_filter" {
			if state != "off" {
				return "", fmt.Errorf("authored condition_fanout_filter=%s conflicts with false execution option", state)
			}
			found = true
		}
	}
	if found {
		return content, nil
	}
	position := value.end - 1
	return content[:position] + "," + assignment + content[position:], nil
}

type sqlToken struct {
	text       string
	start, end int
	kind       byte
}

// This scanner only locates the outer SELECT and hint assignments. It does
// not parse/reprint SQL or assign authority to identifiers or template values.
func lex(sql string) ([]sqlToken, error) {
	var tokens []sqlToken
	depth := 0
	terminated := false
	for i := 0; i < len(sql); {
		start, kind := i, byte('p')
		c := sql[i]
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			i++
			continue
		}
		switch {
		case c == '\'' || c == '"' || c == '`':
			kind = 's'
			if c == '`' {
				kind = 'q'
			}
			i++
			closed := false
			for i < len(sql) {
				if sql[i] == '\\' {
					i += 2
					continue
				}
				if sql[i] == c {
					i++
					if i < len(sql) && sql[i] == c {
						i++
						continue
					}
					closed = true
					break
				}
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated quoted token")
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			kind = 'c'
			end := strings.Index(sql[i+2:], "*/")
			if end == -1 {
				return nil, fmt.Errorf("unterminated SQL comment")
			}
			i += end + 4
		case c == '#' || c == '-' && i+2 < len(sql) && sql[i+1] == '-' && sql[i+2] <= ' ':
			kind = 'c'
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case isWord(c):
			kind = 'w'
			for i < len(sql) && isWord(sql[i]) {
				i++
			}
		default:
			i++
			if c == '(' {
				depth++
			}
			if c == ')' {
				depth--
			}
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced SQL parentheses")
			}
		}
		if kind != 'c' {
			if terminated {
				return nil, fmt.Errorf("multiple SQL statements are unsupported")
			}
			terminated = kind == 'p' && sql[start:i] == ";"
		}
		tokens = append(tokens, sqlToken{text: sql[start:i], start: start, end: i, kind: kind})
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced SQL parentheses")
	}
	return tokens, nil
}

func isWord(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}
