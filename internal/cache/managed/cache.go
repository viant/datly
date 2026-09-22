// Package managed adds owner-scoped invalidation to native SQLX caches.
package managed

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/viant/sqlx/io/read/cache"
)

type Scope string

const (
	All    Scope = "all"
	Lazy   Scope = "lazy"
	Warmup Scope = "warmup"
)

func (s Scope) Validate() error {
	if s != All && s != Lazy && s != Warmup {
		return fmt.Errorf("invalid cache scope %q: expected all, lazy, or warmup", s)
	}
	return nil
}

type Generation struct{ All, Lazy, Warmup string }
type Store interface {
	Read(context.Context) (Generation, error)
	Rotate(context.Context, Scope) (string, error)
}

type Cache struct {
	cache.Cache
	store          Store
	owner          string
	created        func(string, int)
	pending        sync.Map
	nativeCreation bool
}

// New optionally observes successful lazy entry publication (not cache misses).
func New(native cache.Cache, store Store, owner string, observers ...func(string, int)) *Cache {
	result := &Cache{Cache: native, store: store, owner: owner}
	if len(observers) > 0 {
		result.created = observers[0]
		if source, ok := native.(interface{ SetCreationObserver(func(string, int)) }); ok {
			source.SetCreationObserver(result.created)
			result.nativeCreation = true
		}
	}
	return result
}
func (c *Cache) Invalidate(ctx context.Context, scope Scope) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	return c.store.Rotate(ctx, scope)
}
func (c *Cache) identity(sql string, generation Generation, scope Scope) string {
	token := generation.Lazy
	if scope == Warmup {
		token = generation.Warmup
	}
	return fmt.Sprintf("/* datly-cache:%s:%s:%s:%s */\n%s", c.owner, scope, generation.All, token, sql)
}

// options resolves the unmodified native warmup identity before adding ownership.
// Read execution SQL is unchanged. Warmup execution receives the same comment
// as its identity so native completeness checks retain their original meaning.
func (c *Cache) options(options []interface{}, generation Generation) ([]interface{}, error) {
	result := append([]interface{}(nil), options...)
	for i, option := range result {
		matcher, ok := option.(*cache.ParmetrizedQuery)
		if !ok || matcher == nil {
			continue
		}
		copy := *matcher
		sql, args, _, err := copy.WarmupIdentity()
		if err != nil {
			return nil, err
		}
		copy.IdentitySQL = c.identity(sql, generation, Warmup)
		copy.IdentityArgs = args
		result[i] = &copy
	}
	return result, nil
}
func (c *Cache) Get(ctx context.Context, sql string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	refreshRequested := false
	for _, option := range options {
		if refresh, ok := option.(cache.Refresh); ok && bool(refresh) {
			refreshRequested = true
			break
		}
	}
	if args == nil {
		args = []interface{}{}
	}
	generation, err := c.store.Read(ctx)
	if err != nil {
		return nil, err
	}
	warmOptions, err := c.options(options, generation)
	if err != nil {
		return nil, err
	}
	if !refreshRequested {
		if entry, err := c.probeWarmup(ctx, c.identity(sql, generation, Warmup), args, warmOptions); entry != nil || err != nil {
			return entry, err
		}
	}
	lazyOptions := withoutMatchers(options)
	if refreshRequested {
		// Retire this exact warmup key as well as the selected broader matcher.
		// Other authored cases retain their publications and generations.
		retired, err := c.Cache.Get(ctx, c.identity(sql, generation, Warmup), args, cache.Refresh(true))
		if err != nil {
			return nil, err
		}
		if retired != nil {
			_ = c.Cache.Rollback(context.WithoutCancel(ctx), retired)
		}
		lazyOptions = warmOptions
	}
	entry, err := c.Cache.Get(ctx, c.identity(sql, generation, Lazy), args, lazyOptions...)
	if err == nil && entry != nil && !entry.Has() && !entry.ReadOnly && c.created != nil && !c.nativeCreation {
		c.pending.Store(entry, struct{}{})
	}
	return entry, err
}
func withoutMatchers(options []interface{}) []interface{} {
	result := make([]interface{}, 0, len(options))
	for _, option := range options {
		if _, ok := option.(*cache.ParmetrizedQuery); !ok {
			result = append(result, option)
		}
	}
	return result
}

// probeWarmup also checks the exact execution SQL. Some native consumers use
// exact prewarmed entries without supplying a matcher (partitions and summaries).
func (c *Cache) probeWarmup(ctx context.Context, sql string, args []interface{}, options []interface{}) (*cache.Entry, error) {
	stats := &cache.Stats{}
	var requested *cache.Stats
	probe := make([]interface{}, 0, len(options)+1)
	for _, option := range options {
		if value, ok := option.(*cache.Stats); ok {
			requested = value
			continue
		}
		probe = append(probe, option)
	}
	probe = append(probe, stats)
	var entry *cache.Entry
	var err error
	if lookup, ok := c.Cache.(cache.Lookup); ok {
		entry, err = lookup.Lookup(ctx, sql, args, probe...)
	} else {
		entry, err = c.Cache.Get(ctx, sql, args, probe...)
	}
	if err != nil || entry == nil || !entry.Has() {
		if entry != nil {
			if entry.Has() {
				_ = entry.Close()
			} else {
				_ = c.Cache.Rollback(context.WithoutCancel(ctx), entry)
			}
		}
		if err != nil && requested != nil {
			*requested = *stats
		}
		return nil, err
	}
	if stats.FoundLazy {
		entry.Windowed = true // exact SQL already includes its execution window
		stats.WarmupKey = stats.Key
		stats.MarkerKey = ""
	}
	stats.FoundLazy = false
	stats.FoundWarmup = true
	stats.Type = cache.TypeReadMulti
	if requested != nil {
		*requested = *stats
		entry.Stats = requested
	}
	return entry, nil
}
func (c *Cache) Lookup(ctx context.Context, sql string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	lookup, ok := c.Cache.(cache.Lookup)
	if !ok {
		return nil, cache.ErrLookupUnsupported
	}
	if args == nil {
		args = []interface{}{}
	}
	generation, err := c.store.Read(ctx)
	if err != nil {
		return nil, err
	}
	warmOptions, err := c.options(options, generation)
	if err != nil {
		return nil, err
	}
	if entry, err := c.probeWarmup(ctx, c.identity(sql, generation, Warmup), args, warmOptions); entry != nil || err != nil {
		return entry, err
	}
	return lookup.Lookup(ctx, c.identity(sql, generation, Lazy), args, withoutMatchers(options)...)
}
func (c *Cache) prepareWarmup(ctx context.Context, sql string, args []interface{}, options []interface{}) (string, []interface{}, error) {
	generation, err := c.store.Read(ctx)
	if err != nil {
		return "", nil, err
	}
	hasMatcher := false
	for _, option := range options {
		if matcher, ok := option.(*cache.ParmetrizedQuery); ok && matcher != nil {
			hasMatcher = true
		}
	}
	if !hasMatcher {
		options = append(append([]interface{}(nil), options...), &cache.ParmetrizedQuery{SQL: sql, Args: args, IdentitySQL: sql, IdentityArgs: args})
	}
	options, err = c.options(options, generation)
	if err != nil {
		return "", nil, err
	}
	// Tag execution and identity equally to preserve native completeness checks.
	return c.identity(sql, generation, Warmup), options, nil
}
func (c *Cache) IndexBy(ctx context.Context, db *sql.DB, column, sql string, args []interface{}, options ...interface{}) (int, error) {
	executionSQL, scoped, err := c.prepareWarmup(ctx, sql, args, options)
	if err != nil {
		return 0, err
	}
	// Preserve the provider's count contract, including Aerospike's index marker.
	return c.Cache.IndexBy(ctx, db, column, executionSQL, args, scoped...)
}
func (c *Cache) IndexByWithResult(ctx context.Context, db *sql.DB, column, sql string, args []interface{}, options ...interface{}) (*cache.IndexByResult, error) {
	executionSQL, scoped, err := c.prepareWarmup(ctx, sql, args, options)
	if err != nil {
		return nil, err
	}
	if indexer, ok := c.Cache.(cache.WarmupIndexer); ok {
		return indexer.IndexByWithResult(ctx, db, column, executionSQL, args, scoped...)
	}
	count, err := c.Cache.IndexBy(ctx, db, column, executionSQL, args, scoped...)
	return &cache.IndexByResult{GroupsWritten: count}, err
}

// Close reports an entry only after its native publication succeeds.
func (c *Cache) Close(ctx context.Context, entry *cache.Entry) error {
	_, pending := c.pending.LoadAndDelete(entry)
	err := c.Cache.Close(ctx, entry)
	if err == nil && pending && entry.WriteCloser != nil && len(entry.Meta.Fields) > 0 {
		c.created(string(Lazy), 1)
	}
	return err
}
func (c *Cache) Rollback(ctx context.Context, entry *cache.Entry) error {
	c.pending.Delete(entry)
	return c.Cache.Rollback(ctx, entry)
}
func (c *Cache) Delete(ctx context.Context, entry *cache.Entry) error {
	c.pending.Delete(entry)
	return c.Cache.Delete(ctx, entry)
}
func (c *Cache) UpdateType(ctx context.Context, entry *cache.Entry, values []interface{}) (bool, error) {
	ok, err := c.Cache.UpdateType(ctx, entry, values)
	if !ok || err != nil {
		c.pending.Delete(entry)
	}
	return ok, err
}

// InvalidateCache is the string-based capability exposed to host applications.
func (c *Cache) InvalidateCache(ctx context.Context, scope string) (string, error) {
	return c.Invalidate(ctx, Scope(scope))
}
