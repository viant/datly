package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/service/executor/handler"
	"github.com/viant/xdatly/handler/async"
	"reflect"
)

type AsyncHandler struct {
	executor *handler.Executor
}

func (a *AsyncHandler) Type() reflect.Type {
	return nil
	//	return a.executor.route.View.Schema.Type()
}

func (a *AsyncHandler) Read(ctx context.Context, options ...async.ReadOption) (*async.JobWithMeta, error) {
	return nil, fmt.Errorf("unsupported")
	/*
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

			connector, err := a.executor.route._router.Resource().Resource.Connector(opts.Connector)
			if err != nil {
				return nil, err
			}

			_, err = a.executor.route._async.EnsureTable(ctx, connector, &async2.TableConfig{
				RecordType:     a.executor.route.View.Schema.Type(),
				TableName:      opts.Job.DestinationTable,
				Dataset:        opts.Job.DestinationDataset,
				CreateIfNeeded: opts.Job.DestinationCreateDisposition == async.CreateDispositionIfNeeded,
				GenerateAutoPk: true,
			})

			if err != nil {
				return nil, err
			}

			handler, err := a.executor.route._async.Handler(ctx, &async.Config{
				BucketURL: opts.Job.DestinationBucketURL,
				QueueName: opts.Job.DestinationQueueName,
				Dataset:   opts.Job.DestinationDataset,
			})

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
				Job: firstNotNil(existingRecord, opts.Job),
			}, nil
		}

		func firstNotNil(jobs ...*async.Job) *async.Job {
			for _, job := range jobs {
				if job != nil {
					return job
				}
			}
			return nil
	*/
}

func (a *AsyncHandler) ReadInto(ctx context.Context, dst interface{}, job *async.Job) error {

	return nil

	//if job.DestinationConnector == "" {
	//	return fmt.Errorf("unspecified Async database connector")
	//}
	//
	//connector, err := a.executor.route._router.Resource().Resource.Connector(job.DestinationConnector)
	//if err != nil {
	//	return err
	//}
	//
	//if job.Session != async.StateDone {
	//	return fmt.Errorf("can't record state PENDING")
	//}
	//
	//if job.EndTime != nil && (*job).EndTime.Before(time.Now()) {
	//	return fmt.Errorf("job expired")
	//}
	//
	//rValue := reflect.ValueOf(dst)
	//if rValue.Kind() != reflect.Ptr {
	//	return fmt.Errorf("async dst has to be a pointer")
	//}
	//
	//elem := rValue.Type().Elem()
	//if !types.IsMulti(elem) {
	//	return fmt.Errorf("async dst hast to be slice pointer")
	//}
	//
	//slice := xunsafe.NewSlice(elem)
	//appender := slice.Appender(xunsafe.AsPointer(dst))
	//db, err := connector.DB()
	//if err != nil {
	//	return err
	//}
	//
	//return async2.QueryInto(ctx, db, job, appender)
}
