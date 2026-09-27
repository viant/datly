package reader

import (
	"context"
	"database/sql"
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
	queried := false
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
		if queried {
			q.read.session.recorder.QueryRead(q.read.metric.View, execution, delivered, err)
		}
		if panicked != nil {
			panic(panicked)
		}
	}()
	options := r.options
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
	reader, err := sqlxread.New(ctx, q.db, q.query.SQL, r.newRow, options...)
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
		return reader.QueryAll(ctx, func(value any) error {
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
	}()
	queried = true
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
