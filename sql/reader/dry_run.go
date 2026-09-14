package reader

import (
	"context"
	"fmt"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

func (e *Execution) PlanRead(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (*dexec.ReadPlan, error) {
	if e == nil || e.service == nil {
		return nil, fmt.Errorf("reader execution is not initialized")
	}
	session := e.session()
	session.Parameters, session.DryRun = resolver, true
	value, err := e.service.Read(ctx, session, input, binder)
	if err != nil {
		return nil, err
	}
	plan, ok := value.(*dexec.ReadPlan)
	if !ok {
		return nil, fmt.Errorf("reader returned invalid dry-run plan %T", value)
	}
	return plan, nil
}
