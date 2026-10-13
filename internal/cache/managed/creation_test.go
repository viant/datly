package managed

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/read"
)

func TestCreationObserverCountsOnlySuccessfulPublications(t *testing.T) {
	ctx := context.Background()
	db := database(t)
	c := service(t, t.TempDir(), "orders")
	var created atomic.Int64
	c.created = func(_ string, entries int) { created.Add(int64(entries)) }
	query(t, db, c, nil, false, nil)
	require.Equal(t, int64(1), created.Load())
	query(t, db, c, nil, false, nil)
	require.Equal(t, int64(1), created.Load(), "a hit is not a creation")
	_, err := c.Invalidate(ctx, All)
	require.NoError(t, err)
	reader, err := read.New(ctx, db, "SELECT id,name FROM items WHERE id = ?", func() any { return &record{} }, read.WithCache(c))
	require.NoError(t, err)
	require.Error(t, reader.QueryAll(ctx, func(any) error { return errors.New("consumer failed") }, 1))
	if stmt := reader.Stmt(); stmt != nil {
		require.NoError(t, stmt.Close())
	}
	require.Equal(t, int64(1), created.Load(), "a rollback is not a creation")
	reader, err = read.New(ctx, db, "SELECT missing_column FROM items", func() any { return &record{} }, read.WithCache(c))
	require.NoError(t, err)
	require.Error(t, reader.QueryAll(ctx, func(any) error { return nil }))
	if stmt := reader.Stmt(); stmt != nil {
		require.NoError(t, stmt.Close())
	}
	require.Equal(t, int64(1), created.Load(), "a SQL failure is not a creation")
	_, err = db.Exec("DELETE FROM items")
	require.NoError(t, err)
	query(t, db, c, nil, false, nil)
	require.Equal(t, int64(2), created.Load(), "a published empty result is a creation")
	pending := 0
	c.pending.Range(func(any, any) bool { pending++; return true })
	require.Zero(t, pending, "completed and failed reads must release tracking")
}
