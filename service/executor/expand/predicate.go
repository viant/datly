package expand

import (
	"context"
	"fmt"
	"strings"

	vcontext "github.com/viant/datly/view/context"
	"github.com/viant/datly/view/tags"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/handler/logger"
)

var PredicateState predicateState = "state"
var PredicateCtx predicateCtx = "ctx"

type predicateCtx string
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

	// PredicateGroupSQL is the composable, parameterized result of rendering one
	// predicate group. Rendering does not mutate the parent DataUnit or filters.
	PredicateGroupSQL struct {
		Group      int
		Expression string
		Args       []interface{}
	}

	renderedPredicate struct {
		selector *structology.Selector
		args     []interface{}
	}
)

func NewPredicate(ctx *Context, state *structology.State, config []*PredicateConfig, stateType *structology.StateType) *Predicate {
	// Initialize state if not provided, but never override an existing state
	if state == nil && stateType != nil {
		state = stateType.NewState()
	}
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

// RenderGroup renders a predicate group without applying its placeholders or
// filter bookkeeping to the active expansion state.
func (p *Predicate) RenderGroup(group int, operator string) (*PredicateGroupSQL, error) {
	result, _, err := p.renderGroup(group, operator)
	return result, err
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
	if b == nil {
		b = &PredicateBuilder{}
	}
	if b.output == nil {
		b.output = &strings.Builder{}
	}
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
	if b == nil || b.output == nil || b.output.Len() == 0 {
		return ""
	}
	return " " + keyword + " " + b.output.String()
}

func (p *Predicate) expand(group int, operator string) (string, error) {
	result, rendered, err := p.renderGroup(group, operator)
	if err != nil {
		return "", err
	}
	for _, item := range rendered {
		if err := p.appendFilter(item.selector, item.args); err != nil {
			return "", fmt.Errorf("failed to append filter predicate parameter: %w", err)
		}
	}
	if len(result.Args) > 0 {
		p.ctx.DataUnit.addAll(result.Args...)
	}
	return result.Expression, nil
}

func (p *Predicate) renderGroup(group int, operator string) (*PredicateGroupSQL, []renderedPredicate, error) {
	result := &strings.Builder{}
	var args []interface{}
	var rendered []renderedPredicate

	ctx := p.ctx.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = vcontext.WithValue(ctx, PredicateCtx, p.ctx)
	ctx = vcontext.WithValue(ctx, PredicateState, p.state)

	p.ctx.DataUnit.EvalLock.Lock()
	defer p.ctx.DataUnit.EvalLock.Unlock()

	if p.ctx.Session != nil {
		aLogger := p.ctx.Session.Logger()
		ctx = vcontext.WithValue(ctx, logger.ContextKey, aLogger)
	}
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
			return nil, nil, err
		}

		if criteria == nil || strings.TrimSpace(criteria.Expression) == "" {
			continue
		}

		rendered = append(rendered, renderedPredicate{selector: selector, args: append([]interface{}{}, criteria.Placeholders...)})
		if result.Len() != 0 {
			result.WriteString(" ")
			result.WriteString(operator)
			result.WriteString(" ")
		}

		result.WriteByte('(')
		result.WriteString(criteria.Expression)
		result.WriteByte(')')
		args = append(args, criteria.Placeholders...)
	}

	return &PredicateGroupSQL{Group: group, Expression: result.String(), Args: args}, rendered, nil
}

func (p *Predicate) appendFilter(selector *structology.Selector, value []interface{}) error {
	aTag, err := tags.ParseStateTags(selector.Tag(), nil)
	if err != nil {
		return err
	}
	if len(aTag.Predicates) == 0 {
		aTag.EnsurePredicate()
	}
	pTag := aTag.Predicates[0]
	pTag.Init(selector.Name())
	filter := p.ctx.Filters.LookupOrAdd(pTag.Filter)
	if pTag.Exclusion {
		filter.Exclude = value
	} else {
		filter.Include = value
	}
	return nil
}

func (b *PredicateBuilder) And() *PredicateBuilder {
	if b == nil {
		b = &PredicateBuilder{}
	}
	b.lastKeyword = "AND"
	return b
}

func (b *PredicateBuilder) Or() *PredicateBuilder {
	if b == nil {
		b = &PredicateBuilder{}
	}
	b.lastKeyword = "OR"
	return b
}
