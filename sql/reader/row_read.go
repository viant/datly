package reader

import (
	"context"
	"database/sql"
	"errors"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/txread"
	"github.com/viant/datly/sql/reader/collector"

	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

// rowQuery groups the existing SQLX query inputs and native timing ownership.
// It is not a reader plan, binder, cache layer, or alternate execution engine.
type rowQuery struct {
	collector  *collector.Collector
	db         *sql.DB
	tx         *sql.Tx
	query      *cache.ParmetrizedQuery
	visit      func(any) error
	read       *viewRead
	id, parent string
}

func (r rowRead) query(ctx context.Context, q rowQuery) (err error) {
	execution := q.read.execution(q.query, q.id, q.parent)
	rows := 0
	delivered := 0
	queryReturned := false
	queryAttempted := false
	cacheAttempted := false
	defer func() {
		failure := err
		panicked := recover()
		if panicked != nil {
			failure = errReadPanic
		}
		if q.collector != nil {
			rows = q.collector.Len()
		}
		q.read.completeSQL(execution, r.stats, rows, failure)
		// A successful QueryAll also proves cache replay, including an empty
		// cached result. Delivered rows prove replay when a visitor then fails.
		if panicked == nil && queryReturned && (queryAttempted || cacheAttempted || err == nil || delivered > 0) {
			q.read.session.recorder.QueryRead(q.read.metric.View, execution, delivered, err)
		}
		q.read.emitSQL(execution, r.stats)
		if panicked != nil {
			panic(panicked)
		}
	}()
	grouped, groupErr := drainowner.BindingGroupReadAdmission(ctx)
	if groupErr != nil {
		return groupErr
	}
	options := r.options
	if grouped {
		options = append(append([]sqlxread.Option(nil), options...), sqlxread.WithCleanupErrorProvenance(), sqlxread.WithRetry(sqlxread.RetryPolicy{}))
	}
	if r.readCache != nil {
		observed := &observedReadCache{Cache: r.readCache, attempted: &cacheAttempted}
		var wrapped cache.Cache = observed
		if lookup, ok := r.readCache.(cache.Lookup); ok {
			wrapped = &observedLookupCache{observedReadCache: observed, lookup: lookup}
		}
		options = append(append([]sqlxread.Option(nil), options...), sqlxread.WithCache(wrapped))
	}
	if q.tx != nil {
		options = append(append([]sqlxread.Option(nil), options...), sqlxread.WithTx(q.tx))
	}
	var capture *relationKeyCapture
	if q.collector != nil {
		if columns := q.collector.SQLKeyColumns(); len(columns) > 0 {
			capture = &relationKeyCapture{collector: q.collector, columns: columns}
			options = append(append([]sqlxread.Option(nil), options...), sqlxread.WithRowMapper(capture.mapper))
		}
	}
	var reader *sqlxread.Reader
	sampleQuery := func() {
		if reader != nil && reader.Stmt() != nil {
			queryAttempted = true
		}
	}
	policy := r.retry
	if !grouped && policy.Recoverable != nil {
		original := policy.Recoverable
		policy.Recoverable = func(err error) bool {
			// SQLX invokes recovery before it closes and clears the statement.
			sampleQuery()
			return original(err)
		}
		options = append(append([]sqlxread.Option(nil), options...), sqlxread.WithRetry(policy))
	}
	reader, err = sqlxread.New(ctx, q.db, q.query.SQL, r.newRow, options...)
	if err != nil {
		return err
	}
	var buffered []any
	start := 0
	collectorRows := q.collector != nil
	if collectorRows {
		start = q.collector.Len()
	}
	visit := func(value any) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := q.visit(value); err != nil {
			return err
		}
		rows++
		return nil
	}
	err = func() error {
		release, err := txread.Acquire(ctx, q.tx)
		if err != nil {
			return err
		}
		defer release()
		defer func() {
			if stmt := reader.Stmt(); stmt != nil {
				_ = stmt.Close()
			}
		}()
		err = reader.QueryAll(ctx, func(value any) error {
			delivered++
			if capture != nil {
				if err := capture.snapshot(); err != nil {
					return err
				}
			}
			if q.tx == nil {
				return visit(value)
			}
			// Close the transactional cursor before codecs/hooks can invoke another
			// reader. Ordinary collector rows are already buffered in its destination.
			if !collectorRows || r.decoder != nil {
				buffered = append(buffered, value)
			}
			return nil
		}, q.query.Args...)
		sampleQuery()
		// These typed cache-only outcomes occur before any database fallback.
		if r.cacheOnly && (errors.Is(err, cache.ErrMiss) || errors.Is(err, cache.ErrLookupUnsupported)) {
			cacheAttempted = true
		}
		queryReturned = true
		return err
	}()
	if err == nil && q.tx != nil {
		for i := 0; i < delivered; i++ {
			var value any
			if collectorRows {
				// Value slices may have reallocated while scanning. Resolve their
				// final addresses rather than retaining transient allocation pointers.
				ptr, slice := q.collector.Slice()
				value = slice.ValuePointerAt(ptr, start+i)
				if r.decoder != nil {
					if err = r.decoder.Rebind(buffered[i], value); err != nil {
						break
					}
					value = buffered[i]
				}
			} else {
				value = buffered[i]
			}
			if err = visit(value); err != nil {
				break
			}
		}
	}
	return err
}

// observedReadCache preserves the SQLX cache contract while capturing only
// actual replay entry and failed cache lookup, independently of hit statistics.
type observedReadCache struct {
	cache.Cache
	attempted *bool
}

func (c *observedReadCache) Get(ctx context.Context, sql string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	entry, err := c.Cache.Get(ctx, sql, args, options...)
	if err != nil {
		*c.attempted = true
	}
	return entry, err
}
func (c *observedReadCache) AsSource(ctx context.Context, entry *cache.Entry) (cache.Source, error) {
	*c.attempted = true
	return c.Cache.AsSource(ctx, entry)
}

// Only caches that originally implement Lookup expose it through the adapter.
type observedLookupCache struct {
	*observedReadCache
	lookup cache.Lookup
}

func (c *observedLookupCache) Lookup(ctx context.Context, sql string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	*c.attempted = true
	return c.lookup.Lookup(ctx, sql, args, options...)
}
