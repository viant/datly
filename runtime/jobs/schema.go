package jobs

import (
	"context"
	"database/sql"
	"reflect"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/schema"
	"github.com/viant/sqlx/metadata/info"
)

// ensureTable supplies only Datly persistence policy. SQLX owns field mapping,
// identifier rendering, scalar DDL types and database provisioning.
func (c SQLConfig) ensureTable(ctx context.Context, db *sql.DB, dialect *info.Dialect) error {
	columns, err := io.StructColumns(reflect.TypeOf(Record{}), "sqlx")
	if err != nil {
		return err
	}
	for i, column := range columns {
		switch column.Name() {
		case "ID":
			// The public Job has no persistence tags; the original durable key is ID.
			columns[i] = io.NewColumn(column.Name(), "", column.ScanType(), io.WithTag(&io.Tag{PrimaryKey: true}), io.WithColumnLength(40))
		case "Deactivated":
			// Original read metadata permits NULL and normalizes it to active (false).
			columns[i] = io.NewColumn(column.Name(), "", column.ScanType(), io.WithColumnNullable(true))
		}
	}
	table := c.Table
	if table == "" {
		table = "DATLY_JOBS"
	}
	service := schema.Service{DB: db, Dialect: dialect}
	return service.EnsureTable(ctx, schema.Table{Name: table, Dataset: c.Dataset, Columns: columns})
}
