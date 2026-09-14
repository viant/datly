package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/read/cache"
)

func TestCacheProjectionUsesRenderedGrouping(t *testing.T) {
	stored, err := (CacheProjection{SQL: "SELECT SUM(v) AS total,region,COUNT(*) AS count FROM sales GROUP BY region"}).Fields()
	require.NoError(t, err)
	for _, test := range []struct {
		sql string
		hit bool
	}{
		{"SELECT region,COUNT(*) AS count FROM sales GROUP BY 1", true},
		{"SELECT SUM(v) AS total FROM sales", false},
		{"SELECT region,MAX(v) AS total FROM sales GROUP BY region", false},
	} {
		requested, err := (CacheProjection{SQL: test.sql}).Fields()
		require.NoError(t, err)
		_, hit, _, err := (cache.Projection{Stored: stored}).Indexes(requested)
		require.NoError(t, err)
		require.Equal(t, test.hit, hit, test.sql)
	}
}
