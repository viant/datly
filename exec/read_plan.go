package exec

import (
	"context"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	xreader "github.com/viant/xdatly/reader"
)

// ReadPlan is retained as a source-level name for the shared query contract.
// Deprecated: use reader.ParameterizedQuery from Xdatly.
type ReadPlan = xreader.ParameterizedQuery

type ReadPlanner interface {
	PlanRead(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (*xreader.ParameterizedQuery, error)
}
