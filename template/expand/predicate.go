package expand

import (
	"github.com/viant/datly/utils/types"
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
		StateAccessor func() *types.Accessor
		HasAccessor   func() *types.Accessor
		Expander      func(*Context, interface{}) (*parameter.Criteria, error)
	}

	PredicateBuilder struct {
		output *strings.Builder
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

func (b *PredicateBuilder) Or(fragments ...string) *PredicateBuilder {
	builder := &strings.Builder{}
	for _, fragment := range fragments {
		if strings.TrimSpace(fragment) == "" {
			continue
		}

		if builder.Len() > 0 {
			builder.WriteString(" OR ")
		}

		builder.WriteString(fragment)
	}

	if builder.Len() > 0 {
		if b.output.Len() != 0 {
			b.output.WriteString(" AND ")
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

		if p.hasPtr != nil {
			value, err := predicateConfig.HasAccessor().Value(p.hasPtr)
			if err != nil {
				return "", err
			}

			asBool, ok := value.(bool)
			if !asBool && ok {
				continue
			}
		}

		value, err := predicateConfig.StateAccessor().Value(p.hasPtr)
		if err != nil {
			return "", err
		}

		criteria, err := predicateConfig.Expander(p.ctx, value)
		if err != nil {
			return "", err
		}

		if result.Len() != 0 {
			result.WriteString(" ")
			result.WriteString(operator)
			result.WriteString(" ")
		}

		result.WriteString(criteria.Query)
		accArgs = append(accArgs, criteria.Args...)
	}

	return result.String(), nil
}
