package sanitize

import (
	"fmt"
	"strings"

	"github.com/viant/velty"
	"github.com/viant/velty/ast"
	aexpr "github.com/viant/velty/ast/expr"
)

type Options struct {
	Declared map[string]bool
	Consts   map[string]bool
}

type RewriteResult struct {
	SQL        string
	Patches    []velty.Patch
	TrimPrefix int
}

func Declared(input string) map[string]bool {
	ret := map[string]bool{}
	listener := &declaredListener{declared: ret}
	_, _, _ = velty.New(velty.Listener(listener)).Compile([]byte(input))
	for _, name := range scanSetDeclaredHolders(input) {
		if name != "" {
			ret[name] = true
		}
	}
	return ret
}

func scanSetDeclaredHolders(input string) []string {
	result := make([]string, 0)
	lower := strings.ToLower(input)
	for i := 0; i < len(input); i++ {
		if input[i] != '#' {
			continue
		}
		if !strings.HasPrefix(lower[i:], "#set") {
			continue
		}
		j := i + len("#set")
		for j < len(input) && (input[j] == ' ' || input[j] == '\t' || input[j] == '\r' || input[j] == '\n') {
			j++
		}
		if j >= len(input) || input[j] != '(' {
			continue
		}
		body, end, ok := readSetDirectiveBody(input, j)
		if !ok {
			continue
		}
		if name, ok := parseSetDeclaredHolder(body); ok {
			result = append(result, name)
		}
		i = end
	}
	return result
}

func parseSetDeclaredHolder(body string) (string, bool) {
	text := strings.TrimSpace(body)
	if text == "" {
		return "", false
	}
	if !strings.HasPrefix(text, "$_") {
		return "", false
	}
	text = strings.TrimSpace(text[len("$_"):])
	if !strings.HasPrefix(text, "=") {
		return "", false
	}
	text = strings.TrimSpace(text[1:])
	if !strings.HasPrefix(text, "$") || len(text) < 2 {
		return "", false
	}
	name := text[1:]
	if !isSanitizeIdentifierStart(name[0]) {
		return "", false
	}
	end := 1
	for end < len(name) && isSanitizeIdentifierPart(name[end]) {
		end++
	}
	return strings.TrimSpace(name[:end]), true
}

func readSetDirectiveBody(input string, openParen int) (string, int, bool) {
	depth := 0
	quote := byte(0)
	for i := openParen; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(input) {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '"' || ch == '\'' {
			quote = ch
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			depth--
			if depth == 0 {
				return input[openParen+1 : i], i, true
			}
		}
	}
	return "", -1, false
}

func isSanitizeIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isSanitizeIdentifierPart(ch byte) bool {
	return isSanitizeIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}

func SQL(input string, opts Options) string {
	return Rewrite(input, opts).SQL
}

func Rewrite(input string, opts Options) RewriteResult {
	if strings.TrimSpace(input) == "" {
		return RewriteResult{SQL: strings.TrimSpace(input)}
	}
	adjuster := &bindingAdjuster{
		source:   []byte(input),
		declared: opts.Declared,
		consts:   opts.Consts,
		policy:   newRewritePolicy(opts.Declared, opts.Consts),
	}
	out, err := velty.TransformTemplate([]byte(input), adjuster)
	if err != nil {
		return RewriteResult{SQL: strings.TrimSpace(input)}
	}
	trimPrefix := leadingTrimWidth(out)
	return RewriteResult{
		SQL:        strings.TrimSpace(string(out)),
		Patches:    append([]velty.Patch{}, adjuster.patches...),
		TrimPrefix: trimPrefix,
	}
}

type bindingAdjuster struct {
	source   []byte
	declared map[string]bool
	consts   map[string]bool
	policy   *rewritePolicy
	patches  []velty.Patch
}

func (b *bindingAdjuster) Adjust(node ast.Node, ctx *velty.ParserContext) (velty.Action, error) {
	sel, ok := node.(*aexpr.Select)
	if !ok {
		return velty.Keep(), nil
	}
	if ctx.CurrentExprContext().Kind == velty.CtxSetLHS {
		return velty.Keep(), nil
	}
	span, ok := ctx.GetSpan(sel)
	if !ok {
		return velty.Keep(), nil
	}
	if b.inSetDirective(span.Start) {
		return velty.Keep(), nil
	}
	raw := string(b.source[span.Start : span.End+1])
	replacement := b.rewrite(raw)
	if replacement == raw {
		return velty.Keep(), nil
	}
	b.patches = append(b.patches, velty.Patch{
		Span:        span,
		Replacement: []byte(replacement),
	})
	return velty.PatchSpan(span, []byte(replacement)), nil
}

func (b *bindingAdjuster) inSetDirective(pos int) bool {
	if pos <= 0 || pos > len(b.source) {
		return false
	}
	prefix := string(b.source[:pos])
	setPos := strings.LastIndex(prefix, "#set(")
	if setPos == -1 {
		return false
	}
	if nl := strings.LastIndex(prefix, "\n"); nl > setPos {
		return false
	}
	segment := prefix[setPos:pos]
	return strings.Count(segment, "(") > strings.Count(segment, ")")
}

func (b *bindingAdjuster) rewrite(raw string) string {
	if b.policy == nil {
		b.policy = newRewritePolicy(b.declared, b.consts)
	}
	return b.policy.rewrite(raw)
}

func holderName(raw string) string {
	name := strings.TrimSpace(raw)
	if name == "" {
		return ""
	}
	if strings.HasPrefix(name, "${") && strings.HasSuffix(name, "}") {
		name = "$" + name[2:len(name)-1]
	}
	if !strings.HasPrefix(name, "$") {
		return ""
	}
	name = strings.TrimPrefix(name, "$")
	if idx := strings.Index(name, "("); idx != -1 {
		return ""
	}
	if idx := strings.Index(name, "."); idx != -1 {
		head := name[:idx]
		if head == "Unsafe" || head == "Has" {
			name = name[idx+1:]
			if j := strings.Index(name, "."); j != -1 {
				return name[:j]
			}
			return name
		}
		return head
	}
	return name
}

func addUnsafePrefix(raw string) string {
	if strings.HasPrefix(raw, "${") {
		return strings.Replace(raw, "${", "${Unsafe.", 1)
	}
	return strings.Replace(raw, "$", "$Unsafe.", 1)
}

func asPlaceholder(raw string) string {
	if strings.HasPrefix(raw, "${") && strings.HasSuffix(raw, "}") {
		inner := "$" + raw[2:len(raw)-1]
		return fmt.Sprintf("${criteria.AppendBinding(%s)}", inner)
	}
	return fmt.Sprintf("$criteria.AppendBinding(%s)", raw)
}

func leadingTrimWidth(data []byte) int {
	i := 0
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\r', '\n':
			i++
		default:
			return i
		}
	}
	return i
}

type declaredListener struct {
	declared map[string]bool
}

func (d *declaredListener) OnEvent(e velty.Event) {
	if e.Type != velty.EventEnterNode {
		return
	}
	if e.ExprContext.Kind != velty.CtxSetLHS {
		return
	}
	sel, ok := e.Node.(*aexpr.Select)
	if !ok {
		return
	}
	name := holderName(sel.FullName)
	if name == "" {
		name = holderName("$" + sel.ID)
	}
	if name != "" {
		d.declared[name] = true
	}
}
