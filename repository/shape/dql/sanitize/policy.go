package sanitize

import (
	"strings"

	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty"
)

type rewritePolicy struct {
	declared map[string]bool
	foreach  map[string]bool
	consts   map[string]bool
}

func newRewritePolicy(declared, foreach, consts map[string]bool) *rewritePolicy {
	return &rewritePolicy{
		declared: declared,
		foreach:  foreach,
		consts:   consts,
	}
}

func (p *rewritePolicy) rewrite(raw string, kind velty.ExprContextKind) string {
	holder := holderName(raw)
	if holder == "" {
		return raw
	}
	if keywords.Has(holder) {
		return raw
	}
	if strings.HasPrefix(raw, "$Unsafe.") || strings.HasPrefix(raw, "${Unsafe.") || strings.HasPrefix(raw, "$Has.") || strings.HasPrefix(raw, "${Has.") {
		return raw
	}
	if p.consts != nil && p.consts[holder] {
		return addUnsafePrefix(raw)
	}
	if isControlOrFuncContext(kind) {
		if p.declared != nil && p.declared[holder] {
			return raw
		}
		if hasExplicitPrefix(raw) {
			return raw
		}
		return addUnsafePrefix(raw)
	}
	if p.declared != nil && p.declared[holder] {
		if hasExplicitPrefix(raw) {
			if p.foreach != nil && p.foreach[holder] {
				return asPlaceholder(raw)
			}
			return asPlaceholder(addUnsafePrefix(raw))
		}
		return asPlaceholder(raw)
	}
	if hasExplicitPrefix(raw) {
		if p.foreach != nil && p.foreach[holder] {
			return asPlaceholder(raw)
		}
		return asPlaceholder(addUnsafePrefix(raw))
	}
	return asPlaceholder(addUnsafePrefix(raw))
}

func isControlOrFuncContext(kind velty.ExprContextKind) bool {
	switch kind {
	case velty.CtxFuncArg,
		velty.CtxForEachCond,
		velty.CtxIfCond, velty.CtxElseIfCond,
		velty.CtxSetRHS,
		velty.CtxForLoopInit, velty.CtxForLoopCond, velty.CtxForLoopPost:
		return true
	default:
		return false
	}
}

func hasExplicitPrefix(raw string) bool {
	name := strings.TrimSpace(raw)
	if strings.HasPrefix(name, "${") && strings.HasSuffix(name, "}") {
		name = "$" + name[2:len(name)-1]
	}
	if !strings.HasPrefix(name, "$") {
		return false
	}
	name = strings.TrimPrefix(name, "$")
	if idx := strings.Index(name, "("); idx != -1 {
		return false
	}
	return strings.Index(name, ".") != -1
}
