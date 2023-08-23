package router

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/router/async"
	"github.com/viant/datly/router/async/handler/s3"
	"github.com/viant/datly/router/async/handler/sqs"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/sqlx/option"
	async2 "github.com/viant/xdatly/handler/async"
	"strings"
	"sync"
	"time"
)

type (
	Async struct {
		EnsureDBTable    bool
		Connector        *view.Connector
		PrincipalSubject string
		ExpiryTimeInS    int
		async2.Config

		_asyncHandler async.Handler
		_matchers     sync.Map
		_qualifier    *qualifier
	}

	qualifier struct {
		parameter *state.Parameter
		accessor  *types.Accessor
	}
)

func (a *Async) Init(ctx context.Context, resource *view.Resource, mainView *view.View) error {
	if a.Connector == nil {
		return fmt.Errorf("async connector can't be empty")
	}

	if err := a.Connector.Init(ctx, resource.GetConnectors()); err != nil {
		return err
	}

	if err := a.initAccessor(resource); err != nil {
		return err
	}

	a.inheritHandlerTypeIfNeeded()

	if err := a.initHandlerIfNeeded(ctx); err != nil {
		return err
	}

	return nil
}

func (a *Async) initAccessor(resource *view.Resource) error {
	if a.PrincipalSubject == "" {
		return nil
	}

	dotIndex := strings.Index(a.PrincipalSubject, ".")
	var paramName, path string
	if dotIndex != -1 {
		paramName, path = a.PrincipalSubject[:dotIndex], a.PrincipalSubject[dotIndex+1:]
	} else {
		paramName = a.PrincipalSubject
	}

	param, err := resource.LookupParameter(paramName)
	if err != nil {
		return err
	}

	var accessor *types.Accessor
	if path != "" {
		accessors := types.NewAccessors(&types.VeltyNamer{})
		accessors.InitPath(param.OutputType(), path)
		accessor, err = accessors.AccessorByName(path)

		if err != nil {
			return err
		}
	}

	a._qualifier = &qualifier{
		parameter: param,
		accessor:  accessor,
	}

	return nil
}

func (r *Route) JobsInserter(ctx context.Context, db *sql.DB) (*insert.Service, error) {
	return r.inserter(ctx, db, view.AsyncJobsTable)
}

func (r *Route) inserter(ctx context.Context, db *sql.DB, table string, options ...option.Option) (*insert.Service, error) {
	options = append(options, io.Resolve(io.NewResolver().Resolve))
	return insert.New(ctx, db, table, options...)
}

func (a *Async) JobsUpdater(ctx context.Context, db *sql.DB) (*update.Service, error) {
	return a.updater(ctx, db, view.AsyncJobsTable)
}

func (r *Route) RecordsInserter(ctx context.Context, route *Route, db *sql.DB) (*insert.Service, error) {
	return r.inserter(ctx, db, route.View.Async.Table, option.Tag(view.AsyncTagName))
}

func (a *Async) updater(ctx context.Context, db *sql.DB, table string) (*update.Service, error) {
	aDb, err := a.Connector.DB()
	if err != nil {
		return nil, err
	}

	return update.New(ctx, aDb, table)
}

func (a *Async) initHandlerIfNeeded(ctx context.Context) error {
	handler, err := a.detectHandlerType(ctx)
	if err != nil {
		return err
	}

	a._asyncHandler = handler

	return nil
}

func (a *Async) detectHandlerType(ctx context.Context) (async.Handler, error) {
	switch a.HandlerType {
	case async2.HandlerTypeS3:
		return s3.NewHandler(ctx, a.BucketURL)
	case async2.HandlerTypeSQS:
		return sqs.NewHandler(ctx, "datly-jobs")

	case async2.HandlerTypeUndefined:
		switch env.BuildType {
		case env.BuildTypeKindLambda:
			return sqs.NewHandler(ctx, "datly-async")

		default:
			return nil, nil
		}

	default:
		return nil, fmt.Errorf("unsupported async HandlerType %v", a.HandlerType)
	}
}

func (a *Async) inheritHandlerTypeIfNeeded() {
	switch env.BuildType {
	case env.BuildTypeKindLambdaSQS, env.BuildTypeKindLambdaS3:
		a.HandlerType = ""
		return
	}

	if a.HandlerType != "" {
		return
	}

	if a.BucketURL != "" {
		a.HandlerType = "S3"
		return
	}
}

func NewAsyncRecord(ctx context.Context, route *Route, request *RequestParams) (*async2.Job, error) {
	newRecord := &async2.Job{}
	if err := InitRecord(ctx, newRecord, route, request); err != nil {
		return nil, err
	}

	return newRecord, nil
}

func InitRecord(ctx context.Context, record *async2.Job, route *Route, request *RequestParams) error {
	if record.JobID == "" {
		recordID, err := uuid.NewUUID()
		if err != nil {
			return err
		}

		record.JobID = recordID.String()
	}

	record.State = async2.StateRunning
	if record.PrincipalSubject == nil {
		principalSubject, err := PrincipalSubject(ctx, route, request)
		if err != nil {
			return err
		}

		record.PrincipalSubject = principalSubject
	}

	var destinationTable string
	if rAsync := route.View.Async; rAsync != nil {
		destinationTable = rAsync.Table
	}

	if record.RequestRouteURI == "" {
		record.RequestRouteURI = route.URI
	}

	if record.RequestURI == "" {
		record.RequestURI = request.request.RequestURI
	}

	if record.RequestHeader == "" {
		headers := request.Header().Clone()
		Sanitize(request.request, route, headers, nil)

		marshal, err := json.Marshal(headers)
		if err != nil {
			return err
		}
		record.RequestHeader = string(marshal)
	}

	if record.RequestMethod == "" {
		record.RequestMethod = request.request.Method
	}

	if record.MainView == "" {
		record.MainView = route.View.Name
	}

	if record.JobType == "" {
		record.JobType = string(route.Service)
	}

	if record.DestinationCreateDisposition == "" {
		record.DestinationCreateDisposition = async2.CreateDispositionIfNeeded
	}

	if record.DestinationTable == "" {
		record.DestinationTable = destinationTable
	}

	if record.CreationTime.IsZero() {
		creationTime := time.Now()
		record.CreationTime = creationTime
	}

	if rAsync := route.Async; rAsync != nil {
		if record.DestinationConnector == "" {
			record.DestinationConnector = rAsync.Connector.Name
		}

		if record.DestinationDataset == "" {
			record.DestinationDataset = rAsync.Dataset
		}

		if record.DestinationQueueName == "" {
			record.DestinationQueueName = rAsync.QueueName
		}

		if record.DestinationBucketURL == "" {
			record.DestinationBucketURL = rAsync.BucketURL
		}
	}

	if record.DestinationConnector == "" {
		return fmt.Errorf("job DestinationConnector can't be empty")
	}

	return nil
}

func PrincipalSubject(ctx context.Context, route *Route, request *RequestParams) (*string, error) {
	rAsync := route.Async
	if rAsync == nil {
		return nil, nil
	}

	principal := rAsync._qualifier
	if principal == nil {
		return nil, nil
	}

	value, err := request.ExtractHttpParam(ctx, principal.parameter)
	if err == nil && principal.accessor != nil {
		value, _ = principal.accessor.Value(value)
	} else {
		return nil, nil
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	subj := string(bytes)
	return &subj, nil
}
