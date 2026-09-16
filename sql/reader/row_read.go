package reader

import (
	"context"
	"database/sql"
	"github.com/viant/datly/sql/reader/collector"

	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

// rowQuery groups the existing SQLX query inputs and native timing ownership.
// It is not a reader plan, binder, cache layer, or alternate execution engine.
type rowQuery struct {
	collector  *collector.Collector
	db         *sql.DB
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
		if err := q.visit(value); err != nil {
			return err
		}
		rows++
		return nil
	}, q.query.Args...)
	queried = true
	return err
}
