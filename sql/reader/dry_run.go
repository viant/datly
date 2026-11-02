package reader

import (
	"context"
	"fmt"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	xreader "github.com/viant/xdatly/reader"
)

func (e *Execution) PlanRead(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (*xreader.ParameterizedQuery, error) {
	if e == nil || e.service == nil {
		return nil, fmt.Errorf("reader execution is not initialized")
	}
	session := e.session()
	session.Parameters, session.DryRun = resolver, true
	value, err := e.service.Read(ctx, session, input, binder)
	if err != nil {
		return nil, err
	}
	plan, ok := value.(*xreader.ParameterizedQuery)
	if !ok {
		return nil, fmt.Errorf("reader returned invalid dry-run plan %T", value)
	}
	return plan, nil
}
