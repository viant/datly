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
			// Retire both scopes so a warmup hit cannot satisfy a forced refresh.
			if _, err := c.Invalidate(ctx, All); err != nil {
				return nil, err
			}
			break
		}
	}
	generation, err := c.store.Read(ctx)
	if err != nil {
		return nil, err
	}
	if refreshRequested {
		// A concurrent warmup might publish in the new generation. A forced
		// database read must bypass that publication too.
		filtered := make([]interface{}, 0, len(options))
		for _, option := range options {
			if _, matcher := option.(*cache.ParmetrizedQuery); !matcher {
				filtered = append(filtered, option)
			}
		}
		options = filtered
	}
	options, err = c.options(options, generation)
	if err != nil {
		return nil, err
	}
	entry, err := c.Cache.Get(ctx, c.identity(sql, generation, Lazy), args, options...)
	if err == nil && entry != nil && !entry.Has() && !entry.ReadOnly && c.created != nil && !c.nativeCreation {
		c.pending.Store(entry, struct{}{})
	}
	return entry, err
}
func (c *Cache) Lookup(ctx context.Context, sql string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	lookup, ok := c.Cache.(cache.Lookup)
	if !ok {
		return nil, cache.ErrLookupUnsupported
	}
	generation, err := c.store.Read(ctx)
	if err != nil {
		return nil, err
	}
	options, err = c.options(options, generation)
	if err != nil {
		return nil, err
	}
	return lookup.Lookup(ctx, c.identity(sql, generation, Lazy), args, options...)
}
func (c *Cache) IndexBy(ctx context.Context, db *sql.DB, column, sql string, args []interface{}, options ...interface{}) (int, error) {
	result, err := c.IndexByWithResult(ctx, db, column, sql, args, options...)
	if result == nil {
		return 0, err
	}
	return result.GroupsWritten, err
}
func (c *Cache) IndexByWithResult(ctx context.Context, db *sql.DB, column, sql string, args []interface{}, options ...interface{}) (*cache.IndexByResult, error) {
	generation, err := c.store.Read(ctx)
	if err != nil {
		return nil, err
	}
	hasMatcher := false
	for _, option := range options {
		if m, ok := option.(*cache.ParmetrizedQuery); ok && m != nil {
			hasMatcher = true
		}
	}
	if !hasMatcher {
		options = append(append([]interface{}(nil), options...), &cache.ParmetrizedQuery{SQL: sql, Args: args, IdentitySQL: sql, IdentityArgs: args})
	}
	options, err = c.options(options, generation)
	if err != nil {
		return nil, err
	}
	// Native AFS distinguishes complete from partial publications by comparing
	// execution SQL with identity SQL. Tag both equally; tagging identity alone
	// incorrectly makes every full warmup partial (including empty results).
	executionSQL := c.identity(sql, generation, Warmup)
	if indexer, ok := c.Cache.(cache.WarmupIndexer); ok {
		return indexer.IndexByWithResult(ctx, db, column, executionSQL, args, options...)
	}
	count, err := c.Cache.IndexBy(ctx, db, column, executionSQL, args, options...)
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
