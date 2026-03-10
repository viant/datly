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
	Foreach  map[string]bool
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
	for _, name := range scanForeachDeclaredHolders(input) {
		if name != "" {
			ret[name] = true
		}
	}
	return ret
}

func ForeachDeclared(input string) map[string]bool {
	ret := map[string]bool{}
	for _, name := range scanForeachDeclaredHolders(input) {
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

func scanForeachDeclaredHolders(input string) []string {
	result := make([]string, 0)
	lower := strings.ToLower(input)
	for i := 0; i < len(input); i++ {
		if input[i] != '#' {
			continue
		}
		if !strings.HasPrefix(lower[i:], "#foreach") {
			continue
		}
		j := i + len("#foreach")
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
		if name, ok := parseForeachHolder(body); ok {
			result = append(result, name)
		}
		i = end
	}
	return result
}

func parseForeachHolder(body string) (string, bool) {
	text := strings.TrimSpace(body)
	if text == "" || text[0] != '$' || len(text) < 2 {
		return "", false
	}
	text = text[1:]
	end := 0
	for end < len(text) && isSanitizeIdentifierPart(text[end]) {
		end++
	}
	if end == 0 {
		return "", false
	}
	name := text[:end]
	rest := strings.TrimSpace(text[end:])
	if !strings.HasPrefix(strings.ToLower(rest), "in ") {
		return "", false
	}
	return name, true
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
		foreach:  opts.Foreach,
		consts:   opts.Consts,
		policy:   newRewritePolicy(opts.Declared, opts.Foreach, opts.Consts),
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
	foreach  map[string]bool
	consts   map[string]bool
	policy   *rewritePolicy
	patches  []velty.Patch
}

func (b *bindingAdjuster) Adjust(node ast.Node, ctx *velty.ParserContext) (velty.Action, error) {
	if call, ok := node.(*aexpr.Call); ok {
		b.rewriteCallNodeArgs(call, ctx)
		return velty.Keep(), nil
	}

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
	replacement := b.rewrite(raw, ctx.CurrentExprContext().Kind)
	if replacement == raw {
		return velty.Keep(), nil
	}
	b.patches = append(b.patches, velty.Patch{
		Span:        span,
		Replacement: []byte(replacement),
	})
	return velty.PatchSpan(span, []byte(replacement)), nil
}

func (b *bindingAdjuster) rewriteCallNodeArgs(call *aexpr.Call, ctx *velty.ParserContext) {
	selectors := make([]*aexpr.Select, 0, 4)
	for _, arg := range call.Args {
		b.collectSelectors(arg, &selectors)
	}
	for _, sel := range selectors {
		span, ok := ctx.GetSpan(sel)
		if !ok {
			continue
		}
		if b.inSetDirective(span.Start) {
			continue
		}
		raw := string(b.source[span.Start : span.End+1])
		replacement := b.rewrite(raw, velty.CtxFuncArg)
		if replacement == raw {
			continue
		}
		b.patches = append(b.patches, velty.Patch{
			Span:        span,
			Replacement: []byte(replacement),
		})
	}
}

func (b *bindingAdjuster) collectSelectors(expr ast.Expression, selectors *[]*aexpr.Select) {
	switch actual := expr.(type) {
	case *aexpr.Select:
		*selectors = append(*selectors, actual)
	case *aexpr.Call:
		b.collectSelectors(actual.X, selectors)
		for _, arg := range actual.Args {
			b.collectSelectors(arg, selectors)
		}
	case *aexpr.Binary:
		b.collectSelectors(actual.X, selectors)
		b.collectSelectors(actual.Y, selectors)
	case *aexpr.Unary:
		b.collectSelectors(actual.X, selectors)
	case *aexpr.Parentheses:
		b.collectSelectors(actual.P, selectors)
	}
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

func (b *bindingAdjuster) rewrite(raw string, kind velty.ExprContextKind) string {
	if b.policy == nil {
		b.policy = newRewritePolicy(b.declared, b.foreach, b.consts)
	}
	if rewritten, ok := b.rewriteCallArgsInSelector(raw); ok {
		return rewritten
	}
	return b.policy.rewrite(raw, kind)
}

func (b *bindingAdjuster) rewriteCallArgsInSelector(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !strings.HasPrefix(trimmed, "$") || !strings.Contains(trimmed, "(") {
		return "", false
	}

	hasBraces := strings.HasPrefix(trimmed, "${") && strings.HasSuffix(trimmed, "}")
	expr := trimmed
	if hasBraces {
		expr = "$" + trimmed[2:len(trimmed)-1]
	}

	open := strings.Index(expr, "(")
	if open <= 0 || !strings.HasPrefix(expr, "$") {
		return "", false
	}
	closeIdx, ok := matchingParen(expr, open)
	if !ok || closeIdx != len(expr)-1 {
		return "", false
	}

	args := expr[open+1 : closeIdx]
	rewrittenArgs, changed := b.rewriteCallArgs(args)
	if !changed {
		return "", false
	}
	rewritten := expr[:open+1] + rewrittenArgs + expr[closeIdx:]
	if hasBraces {
		rewritten = "${" + rewritten[1:] + "}"
	}
	return rewritten, true
}

func (b *bindingAdjuster) rewriteCallArgs(args string) (string, bool) {
	parts := splitArgs(args)
	if len(parts) == 0 {
		return args, false
	}
	changed := false
	for i := range parts {
		part := parts[i]
		lead, core, tail := trimArgWhitespace(part)
		if core == "" {
			continue
		}
		rewrittenCore := core
		if strings.HasPrefix(core, "$") {
			if nested, ok := b.rewriteCallArgsInSelector(core); ok {
				rewrittenCore = nested
			} else {
				rewrittenCore = b.policy.rewrite(core, velty.CtxFuncArg)
			}
		}
		if rewrittenCore != core {
			changed = true
			parts[i] = lead + rewrittenCore + tail
		}
	}
	if !changed {
		return args, false
	}
	return strings.Join(parts, ","), true
}

func splitArgs(input string) []string {
	if input == "" {
		return nil
	}
	result := make([]string, 0, 4)
	start := 0
	depth := 0
	quote := byte(0)
	for i := 0; i < len(input); i++ {
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
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			if depth > 0 {
				depth--
			}
			continue
		}
		if ch == ',' && depth == 0 {
			result = append(result, input[start:i])
			start = i + 1
		}
	}
	result = append(result, input[start:])
	return result
}

func trimArgWhitespace(input string) (string, string, string) {
	start := 0
	for start < len(input) && (input[start] == ' ' || input[start] == '\t' || input[start] == '\n' || input[start] == '\r') {
		start++
	}
	end := len(input)
	for end > start && (input[end-1] == ' ' || input[end-1] == '\t' || input[end-1] == '\n' || input[end-1] == '\r') {
		end--
	}
	return input[:start], input[start:end], input[end:]
}

func matchingParen(input string, open int) (int, bool) {
	depth := 0
	quote := byte(0)
	for i := open; i < len(input); i++ {
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
		if ch == '\'' || ch == '"' {
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
				return i, true
			}
		}
	}
	return -1, false
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
