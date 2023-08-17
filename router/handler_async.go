package router

import (
	"context"
	"fmt"
	async2 "github.com/viant/datly/router/async"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xunsafe"
	"reflect"
	"time"
)

type AsyncHandler struct {
	executor *HandlerExecutor
	session  *ReaderSession
}

func (a *AsyncHandler) Read(ctx context.Context, config *async.Config, options ...async.ReadOption) (*async.JobWithMeta, error) {
	opts := &async.ReadOptions{}
	for _, fn := range options {
		fn(opts)
	}

	if opts.Connector == "" {
		return nil, fmt.Errorf("async connector can't be empty")
	}

	if opts.Job == nil {
		opts.Job = &async.Job{}
	}

	if opts.Job.DestinationTable == "" {
		return nil, fmt.Errorf("destination table can't be empty")
	}

	params, err := a.executor.RequestParams(ctx)
	if err != nil {
		return nil, err
	}

	if err := InitRecord(ctx, opts.Job, a.executor.route, params); err != nil {
		return nil, err
	}

	if config == nil {
		config = &async.Config{}
	}

	connector, err := a.executor.route._router.Resource().Resource.Connector(opts.Connector)
	if err != nil {
		return nil, err
	}

	_, err = a.executor.route._async.EnsureTable(ctx, connector, &async2.TableConfig{
		RecordType:     a.executor.route.View.Schema.Type(),
		TableName:      opts.Job.DestinationTable,
		Dataset:        config.Dataset,
		CreateIfNeeded: opts.Job.DestinationCreateDisposition == async.CreateDispositionIfNeeded,
		GenerateAutoPk: true,
	})

	if err != nil {
		return nil, err
	}

	handler, err := a.executor.route._async.Handler(ctx, config)
	if err != nil {
		return nil, err
	}

	db, err := connector.DB()
	if err != nil {
		return nil, err
	}

	session, err := a.getSession(ctx)
	if err != nil {
		return nil, err
	}

	existingRecord, err := a.executor.route._router.insertAndExecuteJob(ctx, session, db, opts.Job, handler, opts.OnExist)
	if err != nil {
		return nil, err
	}

	return &async.JobWithMeta{
		Metadata: &async.JobMetadata{
			CacheHit: existingRecord != nil,
		},
		Record: firstNotNil(existingRecord, opts.Job),
	}, nil
}

func firstNotNil(jobs ...*async.Job) *async.Job {
	for _, job := range jobs {
		if job != nil {
			return job
		}
	}

	return nil
}

func (a *AsyncHandler) ReadInto(ctx context.Context, dst interface{}, job *async.Job, connectorName string) error {
	connector, err := a.executor.route._router.Resource().Resource.Connector(connectorName)
	if err != nil {
		return err
	}

	if job.State != async.StateDone {
		return fmt.Errorf("can't record state PENDING")
	}

	if job.EndTime != nil && (*job).EndTime.Before(time.Now()) {
		return fmt.Errorf("job expired")
	}

	rValue := reflect.ValueOf(dst)
	if rValue.Kind() != reflect.Ptr {
		return fmt.Errorf("async dst has to be a pointer")
	}

	elem := rValue.Type().Elem()
	if !types.IsMulti(elem) {
		return fmt.Errorf("async dst hast to be slice pointer")
	}

	slice := xunsafe.NewSlice(elem)
	appender := slice.Appender(xunsafe.AsPointer(dst))
	db, err := connector.DB()
	if err != nil {
		return err
	}

	return async2.QueryInto(ctx, db, job, appender)
}

func (a *AsyncHandler) getSession(ctx context.Context) (*ReaderSession, error) {
	if a.session != nil {
		return a.session, nil
	}

	return a.executor.route._router.prepareReaderSession(ctx, a.executor.response, a.executor.request, a.executor.route)
}
