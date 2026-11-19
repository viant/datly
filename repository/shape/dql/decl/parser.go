package decl

import (
	"fmt"
	"strings"

	"github.com/viant/parsly"
)

// Parse extracts declarations from original DQL text.
func Parse(dql string) ([]*Declaration, error) {
	cursor := parsly.NewCursor("", []byte(dql), 0)
	var result []*Declaration
	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAfterOptional(whitespaceMatcher,
			commentBlockMatcher,
			singleQuotedMatcher,
			doubleQuotedMatcher,
			identifierMatcher,
			anyMatcher,
		)
		switch matched.Code {
		case identifierToken:
			name := strings.ToLower(matched.Text(cursor))
			callOffset := matched.Offset
			block := cursor.MatchAfterOptional(whitespaceMatcher, parenthesesBlockMatcher)
			if block.Code != parenthesesBlockToken {
				continue
			}
			rawCall := name + block.Text(cursor)
			argsText := block.Text(cursor)
			if len(argsText) < 2 {
				continue
			}
			args := splitArgs(argsText[1 : len(argsText)-1])
			if rewrittenName, rewrittenArgs, ok := unwrapSetSpecial(name, args); ok {
				name = rewrittenName
				args = rewrittenArgs
				rawCall = name + "(" + strings.Join(args, ", ") + ")"
			}
			decl := &Declaration{
				Kind:   parseKind(name),
				Raw:    rawCall,
				Offset: callOffset,
				Args:   args,
			}
			normalizeDeclaration(decl)
			result = append(result, decl)
		case parsly.Invalid:
			return nil, cursor.NewError(identifierMatcher)
		}
	}
	return result, nil
}

func parseKind(name string) Kind {
	switch strings.ToLower(name) {
	case "cast":
		return KindCast
	case "tag":
		return KindTag
	case "set_limit":
		return KindSetLimit
	case "allow_nulls":
		return KindAllowNulls
	case "set_partitioner":
		return KindSetPartitioner
	case "use_connector":
		return KindUseConnector
	case "match_strategy":
		return KindMatchStrategy
	case "compress_above_size":
		return KindCompressAboveSize
	case "batch_size":
		return KindBatchSize
	case "relational_concurrency":
		return KindRelationalConcurrency
	case "publish_parent":
		return KindPublishParent
	case "cardinality":
		return KindCardinality
	case "package":
		return KindPackage
	case "import":
		return KindImport
	default:
		return Kind(name)
	}
}

func normalizeDeclaration(decl *Declaration) {
	if decl == nil || len(decl.Args) == 0 {
		return
	}
	decl.Target = strings.TrimSpace(decl.Args[0])
	switch decl.Kind {
	case KindCast:
		if len(decl.Args) >= 2 {
			decl.DataType = normalizeCastType(decl.Args[1])
		} else if len(decl.Args) == 1 {
			target, dataType := splitCastExpression(decl.Args[0])
			if target != "" {
				decl.Target = target
			}
			if dataType != "" {
				decl.DataType = dataType
			}
		}
	case KindTag:
		if len(decl.Args) >= 2 {
			decl.Tag = trimQuotes(strings.TrimSpace(decl.Args[1]))
		}
	case KindSetLimit:
		if len(decl.Args) >= 2 {
			decl.Limit = strings.TrimSpace(decl.Args[1])
		}
	case KindUseConnector:
		if len(decl.Args) >= 2 {
			decl.Connector = trimQuotes(strings.TrimSpace(decl.Args[1]))
		}
	case KindMatchStrategy:
		if len(decl.Args) >= 2 {
			decl.Strategy = trimQuotes(strings.TrimSpace(decl.Args[1]))
		}
	case KindSetPartitioner:
		if len(decl.Args) >= 2 {
			decl.Partition = trimQuotes(strings.TrimSpace(decl.Args[1]))
		}
		if len(decl.Args) >= 3 {
			decl.Value = strings.TrimSpace(decl.Args[2])
		}
	case KindCompressAboveSize:
		if len(decl.Args) >= 1 {
			decl.Size = strings.TrimSpace(decl.Args[0])
		}
	case KindBatchSize, KindRelationalConcurrency, KindCardinality:
		if len(decl.Args) >= 2 {
			decl.Value = trimQuotes(strings.TrimSpace(decl.Args[1]))
		}
	case KindPackage:
		decl.Package = trimQuotes(strings.TrimSpace(decl.Args[0]))
	case KindImport:
		switch len(decl.Args) {
		case 1:
			decl.Package = trimQuotes(strings.TrimSpace(decl.Args[0]))
		default:
			decl.Alias = trimQuotes(strings.TrimSpace(decl.Args[0]))
			decl.Package = trimQuotes(strings.TrimSpace(decl.Args[1]))
		}
	}
}

func splitCastExpression(expr string) (string, string) {
	text := strings.TrimSpace(expr)
	if text == "" {
		return "", ""
	}
	lowered := strings.ToLower(text)
	quote := rune(0)
	escape := false
	depth := 0
	for i := 0; i < len(text); i++ {
		r := rune(text[i])
		if quote != 0 {
			if escape {
				escape = false
				continue
			}
			if r == '\\' {
				escape = true
				continue
			}
			if r == quote {
				quote = 0
			}
			continue
		}
		switch r {
		case '\'', '"', '`':
			quote = r
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && i+4 <= len(text) {
			chunk := lowered[i : i+4]
			if chunk == " as " {
				left := strings.TrimSpace(text[:i])
				right := strings.TrimSpace(text[i+4:])
				return left, trimQuotes(right)
			}
		}
	}
	return text, ""
}

func normalizeCastType(arg string) string {
	text := strings.TrimSpace(arg)
	lower := strings.ToLower(text)
	if strings.HasPrefix(lower, "as ") {
		text = strings.TrimSpace(text[3:])
	}
	return trimQuotes(text)
}

func unwrapSetSpecial(name string, args []string) (string, []string, bool) {
	if strings.ToLower(strings.TrimSpace(name)) != "set" || len(args) != 1 {
		return "", nil, false
	}
	expr := args[0]
	if expr == "" {
		return "", nil, false
	}
	for _, functionName := range []string{"package", "import"} {
		token := "$" + functionName + "("
		lowerExpr := strings.ToLower(expr)
		idx := strings.Index(lowerExpr, token)
		if idx == -1 {
			continue
		}
		openPos := idx + len(token) - 1
		closePos := findClosingParen(expr, openPos)
		if closePos <= openPos {
			continue
		}
		inner := strings.TrimSpace(expr[openPos+1 : closePos])
		return functionName, splitArgs(inner), true
	}
	return "", nil, false
}

func findClosingParen(text string, openPos int) int {
	if openPos < 0 || openPos >= len(text) || text[openPos] != '(' {
		return -1
	}
	depth := 0
	var quote rune
	escape := false
	runes := []rune(text)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if quote != 0 {
			if escape {
				escape = false
				continue
			}
			if r == '\\' {
				escape = true
				continue
			}
			if r == quote {
				quote = 0
			}
			continue
		}
		switch r {
		case '\'', '"', '`':
			quote = r
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func splitArgs(text string) []string {
	var result []string
	start := 0
	depth := 0
	var quote rune
	escape := false
	runes := []rune(text)
	for i, r := range runes {
		if quote != 0 {
			if escape {
				escape = false
				continue
			}
			if r == '\\' {
				escape = true
				continue
			}
			if r == quote {
				quote = 0
			}
			continue
		}
		switch r {
		case '\'', '"', '`':
			quote = r
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				result = append(result, strings.TrimSpace(string(runes[start:i])))
				start = i + 1
			}
		}
	}
	last := strings.TrimSpace(string(runes[start:]))
	if last != "" || strings.TrimSpace(text) != "" {
		result = append(result, last)
	}
	return result
}

func trimQuotes(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < 2 {
		return value
	}
	first := value[0]
	last := value[len(value)-1]
	if (first == '\'' && last == '\'') || (first == '"' && last == '"') || (first == '`' && last == '`') {
		return value[1 : len(value)-1]
	}
	return value
}

func (d *Declaration) String() string {
	if d == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s(%s)", d.Kind, strings.Join(d.Args, ", "))
}
