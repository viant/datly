package exec

import (
	"context"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
)

// QueryPreparer is an optional capability of a compiled SQL reader.
type QueryPreparer interface {
	PrepareQuery(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (*PreparedQuery, error)
}

// PreparedQuery contains bound source SQL and a projection executor owned by
// that source connection. No database or transaction is exposed to handlers.
type PreparedQuery struct {
	SQL        string
	Args       []any
	Projection ProjectionReader
}

type ProjectionRequest struct {
	SQL     string
	Args    []any
	RowType reflect.Type
}

type ProjectionReader interface {
	ReadProjection(context.Context, ProjectionRequest) (any, error)
}

// ProjectionKey maps an existing direct source dimension to its projected alias.
// It never requests an additional source field or grouping dimension.
type ProjectionKey struct {
	Source string
	Output string
}

// ProjectionRelation selects one declared source relation for a projected graph.
type ProjectionRelation struct {
	Name   string
	Output string
	Keys   []ProjectionKey
}

type ProjectionGraphRequest struct {
	RowType   reflect.Type
	Relations []ProjectionRelation
}

// ProjectionGraphReader is optional; ordinary scalar projection is unchanged.
type ProjectionGraphReader interface {
	PrepareProjectionGraph(context.Context, ProjectionGraphRequest) (ProjectionGraph, error)
}

// ProjectionGraph owns a request-local native graph over an already paged slice.
// Load must receive only the rows retained after composition lookahead trimming.
type ProjectionGraph interface {
	RowType() reflect.Type
	Load(context.Context, any) (any, error)
}
