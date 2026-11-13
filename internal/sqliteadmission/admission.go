// Package sqliteadmission coordinates owned SQLite writers across connector pools.
package sqliteadmission

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
)

type gate struct {
	token      chan struct{}
	references int
	key        string
}

var registry = struct {
	sync.Mutex
	handles map[*sql.DB]*gate
	gates   map[string]*gate
}{handles: map[*sql.DB]*gate{}, gates: map[string]*gate{}}

// Register runs at connector initialization. The returned cleanup belongs to the
// connector lifetime; active and queued owners retain their own references.
func Register(db *sql.DB, driver, dsn string) (func(), error) {
	if driver != "sqlite" && driver != "sqlite3" {
		return func() {}, nil
	}
	key, err := identity(db, dsn)
	if err != nil {
		return nil, err
	}
	registry.Lock()
	if registry.handles[db] != nil {
		registry.Unlock()
		return nil, fmt.Errorf("SQLite connector already registered")
	}
	g := registry.gates[key]
	if g == nil {
		g = &gate{token: make(chan struct{}, 1), key: key}
		registry.gates[key] = g
	}
	g.references++
	registry.handles[db] = g
	registry.Unlock()
	var once sync.Once
	return func() { once.Do(func() { registry.Lock(); delete(registry.handles, db); drop(g); registry.Unlock() }) }, nil
}
func identity(db *sql.DB, dsn string) (string, error) {
	if dsn == ":memory:" || dsn == "" {
		return fmt.Sprintf("private:%p", db), nil
	}
	name, query, _ := strings.Cut(dsn, "?")
	values, err := url.ParseQuery(query)
	if err != nil {
		return "", fmt.Errorf("invalid SQLite connector options")
	}
	if name == "file::memory:" || values.Get("mode") == "memory" {
		if values.Get("cache") != "shared" {
			return fmt.Sprintf("private:%p", db), nil
		}
		return "memory:" + name, nil
	}
	if strings.HasPrefix(name, "file:") {
		parsed, parseErr := url.Parse(name)
		if parseErr != nil {
			return "", fmt.Errorf("invalid SQLite database URI")
		}
		name = parsed.Path
		if parsed.Host != "" && parsed.Host != "localhost" {
			name = "//" + parsed.Host + parsed.Path
		}
		if parsed.Opaque != "" {
			name, err = url.PathUnescape(parsed.Opaque)
			if err != nil {
				return "", fmt.Errorf("invalid SQLite database path")
			}
		}
	}
	path, err := filepath.Abs(name)
	if err != nil {
		return "", fmt.Errorf("resolve SQLite database path: %w", err)
	}
	if resolved, e := filepath.EvalSymlinks(path); e == nil {
		path = resolved
	} else if parent, e := filepath.EvalSymlinks(filepath.Dir(path)); e == nil {
		path = filepath.Join(parent, filepath.Base(path))
	}
	return "file:" + filepath.Clean(path), nil
}
func drop(g *gate) {
	g.references--
	if g.references == 0 {
		delete(registry.gates, g.key)
	}
}

// Acquire does not hold a database connection while waiting. Unregistered and
// non-SQLite handles keep their existing behavior. Nested calls must acquire
// only when the owning root has no transaction yet.
func Acquire(ctx context.Context, db *sql.DB) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	registry.Lock()
	g := registry.handles[db]
	if g != nil {
		g.references++
	}
	registry.Unlock()
	if g == nil {
		return func() {}, nil
	}
	releaseRef := func() { registry.Lock(); drop(g); registry.Unlock() }
	select {
	case g.token <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-g.token
			releaseRef()
			return nil, err
		}
		var once sync.Once
		return func() { once.Do(func() { <-g.token; releaseRef() }) }, nil
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	}
}
