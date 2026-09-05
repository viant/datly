package jobs

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/datly/service/dbms"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xreflect"
	"reflect"
	"sync"
)

type Service struct {
	config     *dbms.TableConfig
	service    *dbms.Service
	connector  *view.Connector
	resource   *view.Resource
	reader     *reader.Service
	inserter   *insert.Service
	updater    *update.Service
	readerView *view.View
	sync.RWMutex
	failedJobs []*async.Job
}

func (s *Service) AddFailedJob(job *async.Job) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	if len(s.failedJobs) > 100 {
		s.failedJobs = s.failedJobs[5:]
	}
	s.failedJobs = append(s.failedJobs, job)
}

func (s *Service) matchFailedJob(matchKey string) (*async.Job, error) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	for _, candidate := range s.failedJobs {
		if candidate.MatchKey == matchKey {
			var err error
			if candidate.Error != nil {
				err = errors.New(*candidate.Error)
			} else {
				err = fmt.Errorf("job has status %s", candidate.Status)
			}
			return candidate, err
		}
	}
	return nil, nil
}

func (s *Service) EnsureJobTables(ctx context.Context) error {
	_, err := s.service.EnsureTable(ctx, s.connector, s.config)
	return err
}

func (s *Service) Init(ctx context.Context) error {
	s.reader = reader.New()
	s.resource = view.EmptyResource()
	s.resource.Connectors = append(s.resource.Connectors, s.connector)

	db, err := s.connector.DB()
	if err != nil {
		return err
	}
	if err = s.EnsureJobTables(ctx); err != nil {
		return err
	}
	if s.inserter, err = insert.New(ctx, db, s.config.TableName, io.Resolve(io.NewResolver().Resolve)); err != nil {
		return err
	}
	if s.updater, err = update.New(ctx, db, s.config.TableName, io.Resolve(io.NewResolver().Resolve)); err != nil {
		return err
	}
	aView := view.NewView(viewID, s.config.TableName,
		view.WithConnector(s.connector),
		view.WithCriteria("ID", "MatchKey", "CreationTime"),
		view.WithColumns(view.Columns{
			view.NewColumn("MatchKey", "varchar", xreflect.StringType, false),
			view.NewColumn("Status", "varchar", xreflect.StringType, false),
			view.NewColumn("Metrics", "text", xreflect.StringType, false),
			view.NewColumn("Connector", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("TableName", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("TableDataset", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("TableSchema", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("CreateDisposition", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("Template", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("WriteDisposition", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("Cache", "text", xreflect.StringPtrType, true),
			view.NewColumn("CacheKey", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("CacheSet", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("CacheNamespace", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("Method", "varchar", xreflect.StringType, false),
			view.NewColumn("URI", "varchar", xreflect.StringType, false),
			view.NewColumn("State", "text", xreflect.StringType, false),
			view.NewColumn("UserEmail", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("UserID", "varchar", xreflect.StringPtrType, true),
			view.NewColumn("MainView", "varchar", xreflect.StringType, false),
			view.NewColumn("Module", "varchar", xreflect.StringType, false),
			view.NewColumn("Labels", "varchar", xreflect.StringType, false),
			view.NewColumn("JobType", "varchar", xreflect.StringType, false),
			view.NewColumn("EventURL", "varchar", xreflect.StringType, false),
			view.NewColumn("Error", "text", xreflect.StringPtrType, true),
			view.NewColumn("CreationTime", "datetime", xreflect.TimeType, false),
			view.NewColumn("StartTime", "datetime", xreflect.TimePtrType, true),
			view.NewColumn("EndTime", "datetime", xreflect.TimePtrType, true),
			view.NewColumn("ExpiryTime", "datetime", xreflect.TimePtrType, true),
			view.NewColumn("WaitTimeInMcs", "int", xreflect.IntType, false),
			view.NewColumn("RuntimeInMcs", "int", xreflect.IntType, false),
			view.NewColumn("SQLQuery", "text", xreflect.StringType, false),
			view.NewColumn("Deactivated", "tinyint", xreflect.BoolPtrType, true),
			view.NewColumn("ID", "varchar", xreflect.StringType, false),
		}),
		view.WithViewType(s.config.RecordType),
	)
	s.resource.AddViews(aView)
	s.readerView = aView
	return aView.Init(ctx, s.resource)
}

// New returns a service
func New(connector *view.Connector) *Service {
	return &Service{service: dbms.New(),
		connector: connector,
		config: &dbms.TableConfig{
			RecordType:     reflect.TypeOf(&async.Job{}),
			TableName:      view.AsyncJobsTable,
			Dataset:        "",
			CreateIfNeeded: true,
			GenerateAutoPk: false,
		}}
}
