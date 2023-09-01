package executor

import (
	"context"
	"database/sql"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata/info"
)

type ViewDBSource struct {
	view *view.View
}

func NewViewDBSource(view *view.View) *ViewDBSource {
	return &ViewDBSource{
		view: view,
	}
}

func (v *ViewDBSource) Db(_ context.Context) (*sql.DB, error) {
	return v.view.Db()
}

func (v *ViewDBSource) Dialect(ctx context.Context) (*info.Dialect, error) {
	return v.view.Connector.Dialect(ctx)
}

func (v *ViewDBSource) CanBatch(table string) bool {
	return v.view.TableBatches[table]
}

func (v *ViewDBSource) CanBatchGlobally() bool {
	return false
}
