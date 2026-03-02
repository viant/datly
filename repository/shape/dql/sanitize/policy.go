package sanitize

import "strings"

type rewritePolicy struct {
	declared map[string]bool
	consts   map[string]bool
}

func newRewritePolicy(declared, consts map[string]bool) *rewritePolicy {
	return &rewritePolicy{
		declared: declared,
		consts:   consts,
	}
}

func (p *rewritePolicy) rewrite(raw string) string {
	holder := holderName(raw)
	if holder == "" {
		return raw
	}
	if strings.HasPrefix(raw, "$Unsafe.") || strings.HasPrefix(raw, "${Unsafe.") || strings.HasPrefix(raw, "$Has.") || strings.HasPrefix(raw, "${Has.") {
		return raw
	}
	if p.consts != nil && p.consts[holder] {
		return addUnsafePrefix(raw)
	}
	if p.declared != nil && p.declared[holder] {
		return asPlaceholder(raw)
	}
	return asPlaceholder(addUnsafePrefix(raw))
}
