package sanitize

import (
	"fmt"
	"regexp"
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

var declarationHolderExpr = regexp.MustCompile(`(?i)#set\s*\(\s*\$_\s*=\s*\$([a-zA-Z_][a-zA-Z0-9_]*)`)

func Declared(input string) map[string]bool {
	ret := map[string]bool{}
	listener := &declaredListener{declared: ret}
	_, _, _ = velty.New(velty.Listener(listener)).Compile([]byte(input))
	for _, match := range declarationHolderExpr.FindAllStringSubmatch(input, -1) {
		if len(match) < 2 {
			continue
		}
		name := strings.TrimSpace(match[1])
		if name != "" {
			ret[name] = true
		}
	}
	return ret
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
