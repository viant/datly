package jobs

import (
	"context"
	"github.com/viant/datly/service/dbms"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/xdatly/handler/async"
	"reflect"
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
		view.WithCriteria("ID", "Ref", "CreationTime"),
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
