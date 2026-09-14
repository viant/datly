package exec

import (
	"context"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

// ReadPlan contains the bound root query with its effective pagination.
// It grants no execution capability and does not promise whole-invocation purity.
type ReadPlan struct {
	SQL  string
	Args []any
}

type ReadPlanner interface {
	PlanRead(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (*ReadPlan, error)
}
