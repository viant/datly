package jobs

import (
	"context"
	"github.com/viant/datly/service/dbms"
	"github.com/viant/datly/view"
	async "github.com/viant/xdatly/handler/async"
	"reflect"
)

type Service struct {
	service   *dbms.Service
	connector *view.Connector
}

func (s *Service) EnsureJobTables(ctx context.Context) error {
	_, err := s.service.EnsureTable(ctx, s.connector, &dbms.TableConfig{
		RecordType:     reflect.TypeOf(&async.Job{}),
		TableName:      view.AsyncJobsTable,
		Dataset:        "",
		CreateIfNeeded: true,
		GenerateAutoPk: false,
	})
	return err
}

// New returns a service
func New(connector *view.Connector) *Service {
	return &Service{service: dbms.New(), connector: connector}
}
