package expand

import (
	"context"
	"github.com/viant/datly/view/state/predicate"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"strings"
)

var PredicateState predicateState = "state"
var PredicateCtx predicateCtx = "ctx"

type predicateCtx string
type predicateHas string
type predicateState string

type (
	Predicate struct {
		config []*PredicateConfig
		state  *structology.State
		ctx    *Context
	}

	PredicateConfig struct {
		Group    int
		Selector *structology.Selector `velty:"-"`
		Expander codec.PredicateHandler
		Ensure   bool
	}

	PredicateBuilder struct {
		lastKeyword string
		output      *strings.Builder
	}
)

func NewPredicate(ctx *Context, state *structology.State, config []*PredicateConfig) *Predicate {
	return &Predicate{
		ctx:    ctx,
		config: config,
		state:  state,
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

func (p *Predicate) FilterGroup(group int, keyword string) (string, error) {
	return p.expand(group, keyword)
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

func (p *Predicate) expand(group int, operator string) (string, error) {
	result := &strings.Builder{}

	ctx := context.Background()
	ctx = context.WithValue(ctx, PredicateCtx, p.ctx)
	ctx = context.WithValue(ctx, PredicateState, p.state)
	for _, predicateConfig := range p.config {
		if predicateConfig.Group != group {
			continue
		}

		if p.state != nil && !predicateConfig.Ensure {
			if p.state.HasMarker() && !predicateConfig.Selector.Has(p.state.Pointer()) {
				continue
			}
		}

		selector := predicateConfig.Selector

		value := predicateConfig.Selector.Value(p.state.Pointer())

		criteria, err := predicateConfig.Expander.Compute(ctx, value)
		if err != nil {
			return "", err
		}

		if criteria == nil || strings.TrimSpace(criteria.Predicate) == "" {
			continue
		}

		p.appendFilter(selector, value)
		if result.Len() != 0 {
			result.WriteString(" ")
			result.WriteString(operator)
			result.WriteString(" ")
		}

		result.WriteByte('(')
		result.WriteString(criteria.Predicate)
		result.WriteByte(')')
		if len(criteria.Args) > 0 {
			p.ctx.DataUnit.addAll(criteria.Args...)
		}
	}

	return result.String(), nil
}

func (p *Predicate) appendFilter(selector *structology.Selector, value interface{}) {
	tagText, _ := selector.Tag().Lookup(predicate.TagName)
	predicateTag := predicate.ParseTag(tagText, selector.Name())
	filter := p.ctx.Filters.LookupOrAdd(predicateTag.Name)
	if predicateTag.Exclusion {
		filter.Exclude = value
	} else {
		filter.Include = value
	}
}

func (b *PredicateBuilder) And() *PredicateBuilder {
	b.lastKeyword = "AND"
	return b
}

func (b *PredicateBuilder) Or() *PredicateBuilder {
	b.lastKeyword = "OR"
	return b
}
