package async

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io/read"
)

func QueryJobs(ctx context.Context, db *sql.DB, qualifiers ...*JobQualifier) ([]*Record, error) {
	SQL, args, err := BuildSelectSQL(ctx, db, qualifiers...)
	if err != nil {
		return nil, err
	}

	service, err := read.New(ctx, db, SQL, func() interface{} {
		return &Record{}
	})

	records := make([]*Record, 0)
	if err = service.QueryAll(ctx, func(row interface{}) error {
		records = append(records, row.(*Record))
		return nil
	}, args...); err != nil {
		return nil, err
	}
	return records, nil
}
