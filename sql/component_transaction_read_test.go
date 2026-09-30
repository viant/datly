package sql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/txread"
)

func TestTransactionDialectDiscoveryWaitsForCursor(t *testing.T) {
	for _, named := range []bool{false, true} {
		name := "default"
		if named {
			name = "named"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			h := testharness.NewSQLiteHarness(t)
			tx, err := h.DB.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			release, err := txread.Acquire(ctx, tx)
			require.NoError(t, err)
			defer release()
			component := &SQLComponent{DB: h.DB, Tx: tx}
			connector := ""
			if named {
				connector = "main"
				require.NoError(t, component.RegisterConnector(connector, h.DB))
			}
			waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer cancel()
			_, err = component.Resolve(waitCtx, connector)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			release()
			connection, err := component.Resolve(ctx, connector)
			require.NoError(t, err)
			require.Same(t, tx, connection.Tx)
			require.NotNil(t, connection.Dialect)
		})
	}
}
