package engine

import (
	"context"
	"fmt"
	"github.com/viant/sqlx/io/sequence"
)

func (c sequencerCapability) ReserveScoped(ctx context.Context, table, column string, scope []sequence.Scope, count int, supplied []int64) ([]int64, error) {
	service, ok := c.service.(interface {
		ReserveScoped(context.Context, string, string, []sequence.Scope, int, []int64) ([]int64, error)
	})
	if !ok {
		return nil, fmt.Errorf("native scoped sequence capability is unavailable")
	}
	return service.ReserveScoped(ctx, table, column, scope, count, supplied)
}
func (c sequencerCapability) ScopedSequenceCollision(ctx context.Context, table, column string, scope []sequence.Scope, value int64, identity []sequence.Scope) (bool, error) {
	service, ok := c.service.(interface {
		ScopedSequenceCollision(context.Context, string, string, []sequence.Scope, int64, []sequence.Scope) (bool, error)
	})
	if !ok {
		return false, fmt.Errorf("native scoped collision proof is unavailable")
	}
	return service.ScopedSequenceCollision(ctx, table, column, scope, value, identity)
}
