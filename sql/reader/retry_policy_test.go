package reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	dsql "github.com/viant/datly/sql"
)

func TestReadRetryResolvesCanonicalSource(t *testing.T) {
	for _, name := range []string{"", "selected"} {
		t.Run(name, func(t *testing.T) {
			first, second := sqlite.New(t), sqlite.New(t)
			source := &dsql.SQLComponent{DB: first.DB}
			if name != "" {
				require.NoError(t, source.RegisterConnector(name, first.DB))
			}
			policy := (readRetry{source: source, connector: name}).policy()
			// Swap only in this serial test to distinguish a fresh canonical
			// lookup from a captured database pointer or default-source fallback.
			if name == "" {
				source.DB = second.DB
			} else {
				require.NoError(t, source.RegisterConnector(name, second.DB))
			}
			db, err := policy.Reconnect(context.Background())
			require.NoError(t, err)
			require.Same(t, second.DB, db)
			require.Equal(t, 3, policy.Attempts)
			if name != "" {
				_, err = (readRetry{source: source, connector: "unknown"}).reconnect(context.Background())
				require.ErrorContains(t, err, "not registered")
			}
		})
	}
}
