package managed

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/afs"
)

type record struct {
	ID   int
	Name string
}

func database(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "data.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec("CREATE TABLE items(id INTEGER, name TEXT); INSERT INTO items VALUES(1,'before')")
	require.NoError(t, err)
	return db
}
func service(t *testing.T, root, owner string) *Cache {
	t.Helper()
	native, err := afs.NewCache(root, time.Minute, owner, nil)
	require.NoError(t, err)
	return New(native, NewFileStore(root+"/generations/"+owner), owner)
}
func query(t *testing.T, db *sql.DB, service cache.Cache, matcher *cache.ParmetrizedQuery, refresh bool, emit func()) (string, *cache.Stats) {
	t.Helper()
	stats := &cache.Stats{}
	reader, err := read.New(context.Background(), db, "SELECT id,name FROM items WHERE id = ?", func() any { return &record{} }, read.WithCache(service), read.WithInMatcher(matcher), read.WithCacheStats(stats), read.WithCacheRefresh(cache.Refresh(refresh)))
	require.NoError(t, err)
	defer func() {
		if stmt := reader.Stmt(); stmt != nil {
			_ = stmt.Close()
		}
	}()
	name := ""
	err = reader.QueryAll(context.Background(), func(value any) error {
		name = value.(*record).Name
		if emit != nil {
			emit()
		}
		return nil
	}, 1)
	require.NoError(t, err)
	return name, stats
}
func matcher(grouped bool) *cache.ParmetrizedQuery {
	result := &cache.ParmetrizedQuery{SQL: "SELECT id,name FROM items WHERE id = ?", Args: []any{1}, IdentitySQL: "SELECT id,name FROM items", IdentityArgs: []any{}, Limit: 1}
	if grouped {
		result.By = "id"
		result.In = []any{1}
	}
	return result
}
func warm(t *testing.T, db *sql.DB, c *Cache, grouped bool) {
	t.Helper()
	column := ""
	if grouped {
		column = "id"
	}
	_, err := c.IndexBy(context.Background(), db, column, "SELECT id,name FROM items", nil)
	require.NoError(t, err)
}
func change(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("UPDATE items SET name='after'")
	require.NoError(t, err)
}
func TestInvalidateScopesAcrossInstances(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "indexed"}[grouped], func(t *testing.T) {
			ctx := context.Background()
			db := database(t)
			root := t.TempDir()
			first, second := service(t, root, "orders"), service(t, root, "orders")
			name, _ := query(t, db, first, nil, false, nil)
			require.Equal(t, "before", name)
			warm(t, db, first, grouped)
			change(t, db)
			name, stats := query(t, db, second, matcher(grouped), false, nil)
			require.Equal(t, "before", name)
			require.True(t, stats.FoundWarmup)
			_, err := second.Invalidate(ctx, Warmup)
			require.NoError(t, err)
			name, stats = query(t, db, first, matcher(grouped), false, nil)
			require.Equal(t, "before", name)
			require.True(t, stats.FoundLazy)
			_, err = second.Invalidate(ctx, Lazy)
			require.NoError(t, err)
			name, stats = query(t, db, first, matcher(grouped), false, nil)
			require.Equal(t, "after", name)
			require.False(t, stats.FoundAny())
			// A process recreated after invalidation observes the persisted generation.
			name, stats = query(t, db, service(t, root, "orders"), nil, false, nil)
			require.Equal(t, "after", name)
			require.True(t, stats.FoundLazy)
		})
	}
}
func TestInvalidateLazyPreservesWarmupAndOwnerIsolation(t *testing.T) {
	ctx := context.Background()
	db := database(t)
	root := t.TempDir()
	first, other := service(t, root, "orders"), service(t, root, "invoices")
	query(t, db, other, nil, false, nil)
	warm(t, db, first, true)
	change(t, db)
	_, err := first.Invalidate(ctx, Lazy)
	require.NoError(t, err)
	name, stats := query(t, db, first, matcher(true), false, nil)
	require.Equal(t, "before", name)
	require.True(t, stats.FoundWarmup)
	_, err = first.Invalidate(ctx, All)
	require.NoError(t, err)
	name, _ = query(t, db, first, matcher(true), false, nil)
	require.Equal(t, "after", name)
	name, stats = query(t, db, other, nil, false, nil)
	require.Equal(t, "before", name)
	require.True(t, stats.FoundLazy)
}
func TestRefreshRetiresBothScopes(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "indexed"}[grouped], func(t *testing.T) {
			db := database(t)
			c := service(t, t.TempDir(), "orders")
			query(t, db, c, nil, false, nil)
			warm(t, db, c, grouped)
			change(t, db)
			name, _ := query(t, db, c, matcher(grouped), true, nil)
			require.Equal(t, "after", name)
			name, stats := query(t, db, c, matcher(grouped), false, nil)
			require.Equal(t, "after", name)
			require.True(t, stats.FoundLazy)
		})
	}
}
func TestTTLExpiresBothScopes(t *testing.T) {
	original := cache.Now
	t.Cleanup(func() { cache.Now = original })
	now := time.Now()
	cache.Now = func() time.Time { return now }
	db := database(t)
	c := service(t, t.TempDir(), "orders")
	query(t, db, c, nil, false, nil)
	warm(t, db, c, true)
	change(t, db)
	now = now.Add(2 * time.Minute)
	name, stats := query(t, db, c, matcher(true), false, nil)
	require.Equal(t, "after", name)
	require.False(t, stats.FoundAny())
}
func TestInvalidationFencesActiveLazyWriter(t *testing.T) {
	db := database(t)
	root := t.TempDir()
	first, second := service(t, root, "orders"), service(t, root, "orders")
	query(t, db, first, nil, false, func() { _, err := second.Invalidate(context.Background(), All); require.NoError(t, err) })
	change(t, db)
	name, stats := query(t, db, second, nil, false, nil)
	require.Equal(t, "after", name)
	require.False(t, stats.FoundAny())
}

type gatedCache struct {
	cache.Cache
	started, resume chan struct{}
}

func (c *gatedCache) IndexBy(ctx context.Context, db *sql.DB, column, sql string, args []interface{}, options ...interface{}) (int, error) {
	close(c.started)
	<-c.resume
	return c.Cache.IndexBy(ctx, db, column, sql, args, options...)
}
func TestInvalidationFencesActiveWarmupWriter(t *testing.T) {
	db := database(t)
	root := t.TempDir()
	c := service(t, root, "orders")
	gate := &gatedCache{Cache: c.Cache, started: make(chan struct{}), resume: make(chan struct{})}
	c.Cache = gate
	done := make(chan error, 1)
	go func() {
		_, err := c.IndexBy(context.Background(), db, "id", "SELECT id,name FROM items", nil)
		done <- err
	}()
	<-gate.started
	_, err := service(t, root, "orders").Invalidate(context.Background(), Warmup)
	require.NoError(t, err)
	close(gate.resume)
	require.NoError(t, <-done)
	change(t, db)
	name, stats := query(t, db, c, matcher(true), false, nil)
	require.Equal(t, "after", name)
	require.False(t, stats.FoundAny())
}
func TestGenerationStoreFailsClosed(t *testing.T) {
	db := database(t)
	root := t.TempDir()
	c := service(t, root, "orders")
	query(t, db, c, nil, false, nil)
	store := c.store.(*FileStore)
	require.NoError(t, store.fs.Upload(context.Background(), store.location+"/all", 0600, strings.NewReader("broken")))
	_, err := c.Get(context.Background(), "SELECT id,name FROM items WHERE id = ?", []any{1})
	require.ErrorContains(t, err, "invalid cache generation")
}

type onRotateStore struct {
	Store
	onRotate func()
}

func (s *onRotateStore) Rotate(ctx context.Context, scope Scope) (string, error) {
	token, err := s.Store.Rotate(ctx, scope)
	if err == nil {
		s.onRotate()
	}
	return token, err
}
func TestRefreshBypassesConcurrentNewWarmup(t *testing.T) {
	db := database(t)
	root := t.TempDir()
	first, second := service(t, root, "orders"), service(t, root, "orders")
	first.store = &onRotateStore{Store: first.store, onRotate: func() {
		warm(t, db, second, false)
		change(t, db)
	}}
	name, stats := query(t, db, first, matcher(false), true, nil)
	require.Equal(t, "after", name)
	require.False(t, stats.FoundWarmup)
}

func TestPartialWarmupStillFallsBackOutsideStoredWindow(t *testing.T) {
	ctx := context.Background()
	db := database(t)
	c := service(t, t.TempDir(), "orders")
	_, err := db.Exec("INSERT INTO items VALUES(1,'second')")
	require.NoError(t, err)
	identity := matcher(true)
	_, err = c.IndexBy(ctx, db, "id", "SELECT id,name FROM items LIMIT 1", nil, identity)
	require.NoError(t, err)
	name, stats := query(t, db, c, matcher(true), false, nil)
	require.Equal(t, "before", name)
	require.True(t, stats.FoundWarmup)
	outside := matcher(true)
	outside.Limit = 2
	name, stats = query(t, db, c, outside, false, nil)
	require.Equal(t, "second", name)
	require.False(t, stats.FoundAny())
}

func TestReadOnlyLookupObservesInvalidationWithoutCreatingPayload(t *testing.T) {
	ctx := context.Background()
	db := database(t)
	root := t.TempDir()
	c := service(t, root, "orders")
	query(t, db, c, nil, false, nil)
	entry, err := c.Lookup(ctx, "SELECT id,name FROM items WHERE id = ?", []any{1})
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.True(t, entry.Has())
	require.NoError(t, c.Close(ctx, entry))
	before, err := filepath.Glob(root + "/*.json")
	require.NoError(t, err)
	_, err = c.Invalidate(ctx, Lazy)
	require.NoError(t, err)
	entry, err = c.Lookup(ctx, "SELECT id,name FROM items WHERE id = ?", []any{1})
	require.NoError(t, err)
	require.Nil(t, entry)
	after, err := filepath.Glob(root + "/*.json")
	require.NoError(t, err)
	require.Equal(t, before, after)
}
