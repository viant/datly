package dml

import (
	"context"
	"github.com/viant/datly/sql/sequencer"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/sequence"
)

func (d *Data) ReserveScoped(ctx context.Context, table, column string, scope []sequence.Scope, count int, supplied []int64) (values []int64, err error) {
	err = d.sequence(ctx, func(s *sequencer.Service) error {
		var e error
		values, e = s.ReserveScoped(ctx, table, column, scope, count, supplied)
		return e
	})
	return values, err
}

func (d *Data) ScopedSequenceCollision(ctx context.Context, table, column string, scope []sequence.Scope, value int64, identity []sequence.Scope) (bool, error) {
	owner := d.owner()
	dialect, err := config.Dialect(ctx, owner.db)
	if err != nil {
		return false, err
	}
	return sequence.Collision(ctx, owner.db, dialect.Name, table, column, scope, value, identity)
}
