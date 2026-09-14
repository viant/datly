package velty

import (
	"strings"
)

type Builder struct {
	lastOperator string
	output       strings.Builder
}

func (b *Builder) Combine(fragments ...string) *Builder {
	return b.combine("AND", fragments)
}

func (b *Builder) CombineAnd(fragments ...string) *Builder {
	return b.combine("AND", fragments)
}

func (b *Builder) CombineOr(fragments ...string) *Builder {
	return b.combine("OR", fragments)
}

func (b *Builder) And() *Builder {
	if b == nil {
		b = &Builder{}
	}
	b.lastOperator = "AND"
	return b
}

func (b *Builder) Or() *Builder {
	if b == nil {
		b = &Builder{}
	}
	b.lastOperator = "OR"
	return b
}

func (b *Builder) Build(keyword string) string {
	if b == nil || b.output.Len() == 0 {
		return ""
	}
	if keyword = strings.TrimSpace(keyword); keyword != "" {
		return " " + keyword + " " + b.output.String()
	}
	return b.output.String()
}

func (b *Builder) combine(operator string, fragments []string) *Builder {
	if b == nil {
		b = &Builder{}
	}
	group := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		if fragment = strings.TrimSpace(fragment); fragment != "" {
			group = append(group, "( "+fragment+" )")
		}
	}
	if len(group) > 0 {
		if b.output.Len() > 0 {
			b.output.WriteByte(' ')
			b.output.WriteString(defaultOperator(b.lastOperator))
			b.output.WriteByte(' ')
		}
		b.output.WriteString("( ")
		b.output.WriteString(strings.Join(group, " "+defaultOperator(operator)+" "))
		b.output.WriteString(" )")
	}
	return b
}

func defaultOperator(operator string) string {
	if operator = strings.TrimSpace(operator); operator == "" {
		return "AND"
	}
	return operator
}
