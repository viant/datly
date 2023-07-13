package predicate

import (
	"context"
	"github.com/viant/xdatly/handler/parameter"
	"github.com/viant/xdatly/predicate"
)

type (
	Expander func(ctx context.Context, state interface{}) (*parameter.Criteria, error)

	Evaluator struct {
		expander Expander
		template *predicate.Template
		args     []string
	}
)

func (p *Evaluator) Expand(value interface{}) (*parameter.Criteria, error) {

	return &parameter.Criteria{}, nil
}

func NewEvaluator(template *predicate.Template, expander Expander, args ...string) *Evaluator {
	return &Evaluator{
		expander: expander,
		template: template,
		args:     args,
	}
}
