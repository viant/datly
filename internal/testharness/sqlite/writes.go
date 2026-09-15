package sqlite

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/mattn/go-sqlite3"
)

// ObserveWrites counts actual changes to one business table, including changes
// subsequently rolled back. Use a one-connection pool; allocator metadata and
// zero-row INSERT statements do not produce table update events.
func (h *Harness) ObserveWrites(t testing.TB, ctx context.Context, table string) *atomic.Int64 {
	t.Helper()
	count := &atomic.Int64{}
	conn, err := h.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Raw(func(raw any) error {
		raw.(*sqlite3.SQLiteConn).RegisterUpdateHook(func(_ int, _ string, name string, _ int64) {
			if name == table {
				count.Add(1)
			}
		})
		return nil
	})
	closeErr := conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	return count
}
