package reader

import (
	"context"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	xreader "github.com/viant/xdatly/reader"
)

type queryGuardKey struct{}
type guardedExecution struct {
	*Execution
	guard xreader.QueryGuard
}

// WithQueryGuard opts a detached execution into source admission. The ordinary
// execution/session layout and allocation path are unchanged. The guard must be
// concurrency-safe and must not mutate or execute its parameterized query.
func (e *Execution) WithQueryGuard(guard xreader.QueryGuard) exec.Reader {
	if e == nil || guard == nil {
		return e
	}
	return &guardedExecution{Execution: e, guard: guard}
}
func (e *guardedExecution) context(ctx context.Context) context.Context {
	return context.WithValue(ctx, queryGuardKey{}, e)
}
func queryGuardFor(ctx context.Context, component *spec.Component) xreader.QueryGuard {
	guard, _ := ctx.Value(queryGuardKey{}).(*guardedExecution)
	if guard == nil || guard.config.Component != component {
		return nil
	}
	return guard.guard
}
func (e *guardedExecution) Read(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (any, error) {
	return e.Execution.Read(e.context(ctx), input, binder, resolver)
}
func (e *guardedExecution) ReadResult(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (*ReadResult, error) {
	return e.Execution.ReadResult(e.context(ctx), input, binder, resolver)
}
func (e *guardedExecution) ReadRows(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver, consumer *xhandler.RowConsumer) (any, error) {
	return e.Execution.ReadRows(e.context(ctx), input, binder, resolver, consumer)
}
func (e *guardedExecution) Warmup(ctx context.Context, in exec.ReaderWarmupInvocation) (int, error) {
	return e.Execution.Warmup(e.context(ctx), in)
}
func (e *guardedExecution) WithQueryGuard(guard xreader.QueryGuard) exec.Reader {
	return e.Execution.WithQueryGuard(guard)
}
func (e *guardedExecution) WithRecorder(recorder *observability.Recorder) exec.Reader {
	return e.Execution.WithRecorder(recorder).(*Execution).WithQueryGuard(e.guard)
}
func (e *guardedExecution) PrepareQuery(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (*exec.PreparedQuery, error) {
	prepared, err := e.Execution.PrepareQuery(ctx, input, binder, resolver)
	if err != nil || prepared == nil || prepared.Projection == nil {
		return prepared, err
	}
	copy := *prepared
	copy.Projection = guardedProjection{reader: prepared.Projection, owner: e}
	return &copy, nil
}

type guardedProjection struct {
	reader exec.ProjectionReader
	owner  *guardedExecution
}

func (p guardedProjection) ReadProjection(ctx context.Context, request exec.ProjectionRequest) (any, error) {
	return p.reader.ReadProjection(p.owner.context(ctx), request)
}
