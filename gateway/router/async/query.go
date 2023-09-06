package async

import (
	"context"
	"database/sql"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/option"
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

func QueryJobs(ctx context.Context, db *sql.DB, qualifiers ...*JobQualifier) ([]*async.Job, error) {
	SQL, args, err := BuildSelectSQL(ctx, db, qualifiers...)
	if err != nil {
		return nil, err
	}

	service, err := read.New(ctx, db, SQL, func() interface{} {
		return &async.Job{}
	})

	records := make([]*async.Job, 0)
	if err = service.QueryAll(ctx, func(row interface{}) error {
		records = append(records, row.(*async.Job))
		return nil
	}, args...); err != nil {
		return nil, err
	}
	return records, nil
}

func QueryJobByID(ctx context.Context, db *sql.DB, id string) (*async.Job, error) {
	query, args, err := BuildSelectByID(ctx, db, id)
	if err != nil {
		return nil, err
	}

	service, err := read.New(ctx, db, query, func() interface{} {
		return &async.Job{}
	})

	var result []*async.Job
	if err = service.QuerySingle(ctx, func(row interface{}) error {
		result = append(result, row.(*async.Job))
		return nil
	}, args...); err != nil {
		return nil, err
	}

	if len(result) >= 1 {
		return result[0], nil
	}

	return nil, nil
}

func QueryAll(ctx context.Context, db *sql.DB, job *async.Job, sliceType *xunsafe.Slice) (interface{}, error) {
	slice := reflect.New(sliceType.Type)
	appender := sliceType.Appender(unsafe.Pointer(slice.Pointer()))

	if err := QueryInto(ctx, db, job, appender); err != nil {
		return nil, err
	}

	return slice.Elem().Interface(), nil
}

func QueryInto(ctx context.Context, db *sql.DB, job *async.Job, appender *xunsafe.Appender) error {
	reader, err := read.New(ctx, db, "SELECT * FROM "+*job.TableName, func() interface{} {
		return appender.Add()
	}, io.Resolve(io.NewResolver().Resolve), option.Tag(view.AsyncTagName))

	if err != nil {
		return err
	}

	if err = reader.QueryAll(ctx, func(row interface{}) error {
		return nil
	}); err != nil {
		return err
	}
	return nil
}
