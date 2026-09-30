package sequencer

import (
	"context"
	"fmt"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/sequence"
)

// ReserveScoped allocates a non-identity integer column within typed partitions.
// SQLX owns SQL/locking; the invocation owns the supplied transaction.
func (s *Service) ReserveScoped(ctx context.Context, table, column string, scope []sequence.Scope, count int, supplied []int64) ([]int64, error) {
	if s.tx == nil {
		return nil, fmt.Errorf("scoped sequence requires an invocation transaction")
	}
	dialect, err := config.Dialect(ctx, s.db, s.tx)
	if err != nil {
		return nil, err
	}
	return sequence.Reserve(ctx, s.tx, sequence.Request{Dialect: dialect.Name, Table: table, Column: column, Scope: scope, Count: count, Supplied: supplied})
}
