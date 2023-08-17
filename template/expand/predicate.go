package expand

import (
	"github.com/viant/xdatly/handler/parameter"
	"github.com/viant/xunsafe"
	"strings"
	"unsafe"
)

type (
	Predicate struct {
		config   []*PredicateConfig
		state    interface{}
		has      interface{}
		statePtr unsafe.Pointer
		hasPtr   unsafe.Pointer
		ctx      *Context
	}

	PredicateConfig struct {
		Context       int
		StateAccessor func(state interface{}, statePtr unsafe.Pointer) (interface{}, error)
		HasAccessor   func(has interface{}, hasPtr unsafe.Pointer) (bool, error)
		Expander      func(ctx *Context, state, has, param interface{}) (*parameter.Criteria, error)
		Ensure        bool
	}

	PredicateBuilder struct {
		lastKeyword string
		output      *strings.Builder
	}
)

func NewPredicate(ctx *Context, state, has interface{}, config []*PredicateConfig) *Predicate {
	return &Predicate{
		ctx:      ctx,
		config:   config,
		state:    state,
		statePtr: xunsafe.AsPointer(state),
		has:      has,
		hasPtr:   xunsafe.AsPointer(has),
	}
}

func (p *Predicate) ExpandWith(ctx int, operator string) (string, error) {
	return p.expand(ctx, operator)
}
func (p *Predicate) Expand(ctx int) (string, error) {
	return p.expand(ctx, "AND")
}

func (p *Predicate) Builder() *PredicateBuilder {
	return &PredicateBuilder{
		output: &strings.Builder{},
	}
}

func (p *Predicate) Ctx(ctx int, keyword string) (string, error) {
	return p.expand(ctx, keyword)
}

func (b *PredicateBuilder) Combine(fragments ...string) *PredicateBuilder {
	return b.combine("AND", fragments)
}

func (b *PredicateBuilder) CombineOr(fragments ...string) *PredicateBuilder {
	return b.combine("OR", fragments)
}

func (b *PredicateBuilder) CombineAnd(fragments ...string) *PredicateBuilder {
	return b.combine("AND", fragments)
}

func (b *PredicateBuilder) combine(keyword string, fragments []string) *PredicateBuilder {
	builder := &strings.Builder{}
	for _, fragment := range fragments {
		if strings.TrimSpace(fragment) == "" {
			continue
		}

		if builder.Len() > 0 {
			builder.WriteString(" ")
			builder.WriteString(keyword)
			builder.WriteString(" ")
		}

		builder.WriteString(" ( ")
		builder.WriteString(fragment)
		builder.WriteString(" ) ")
	}

	if builder.Len() > 0 {
		if b.output.Len() != 0 {
			lastK := b.lastKeyword
			if lastK == "" {
				lastK = "AND"
			}
			b.output.WriteString(" ")
			b.output.WriteString(lastK)
			b.output.WriteString(" ")
		}

		b.output.WriteString(" ( ")
		b.output.WriteString(builder.String())
		b.output.WriteString(" ) ")
	}

	return b
}

func (b *PredicateBuilder) Build(keyword string) string {
	if b.output.Len() == 0 {
		return ""
	}

	return " " + keyword + " " + b.output.String()
}

func (p *Predicate) expand(ctx int, operator string) (string, error) {
	result := &strings.Builder{}
	var accArgs []interface{}
	for _, predicateConfig := range p.config {
		if predicateConfig.Context != ctx {
			continue
		}

		if p.hasPtr != nil && !predicateConfig.Ensure {
			value, err := predicateConfig.HasAccessor(p.has, p.hasPtr)
			if err != nil {
				return "", err
			}

			if !value {
				continue
			}
		}

		value, err := predicateConfig.StateAccessor(p.state, p.statePtr)
		if err != nil {
			return "", err
		}

		criteria, err := predicateConfig.Expander(p.ctx, p.state, p.has, value)
		if err != nil {
			return "", err
		}

		if strings.TrimSpace(criteria.Query) == "" {
			continue
		}

		if result.Len() != 0 {
			result.WriteString(" ")
			result.WriteString(operator)
			result.WriteString(" ")
		}

		result.WriteByte('(')
		result.WriteString(criteria.Query)
		result.WriteByte(')')
		accArgs = append(accArgs, criteria.Args...)
	}

	return result.String(), nil
}

func (b *PredicateBuilder) And() *PredicateBuilder {
	b.lastKeyword = "AND"
	return b
}

func (b *PredicateBuilder) Or() *PredicateBuilder {
	b.lastKeyword = "OR"
	return b
}
