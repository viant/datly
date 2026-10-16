package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/registry"
)

type identifierProductDriver struct{ opens atomic.Int32 }

func (d *identifierProductDriver) Open(string) (driver.Conn, error) {
	d.opens.Add(1)
	return nil, fmt.Errorf("identifier product resolution must not open a connection")
}

type identifierProductConnector struct{ driver *identifierProductDriver }

func (c identifierProductConnector) Connect(context.Context) (driver.Conn, error) {
	return c.driver.Open("")
}

func (c identifierProductConnector) Driver() driver.Driver { return c.driver }

func TestIdentifierProductRequiresNoDatabaseIO(t *testing.T) {
	typ := reflect.TypeFor[identifierProductDriver]()
	require.NoError(t, registry.RegisterDriver(database.Product{Name: "BigQuery"}, typ.PkgPath(), typ.Name()))
	driver := &identifierProductDriver{}
	db := sql.OpenDB(identifierProductConnector{driver: driver})
	t.Cleanup(func() { _ = db.Close() })
	component := &SQLComponent{DB: db}
	require.NoError(t, component.RegisterConnector("metrics", db))
	require.NoError(t, component.RegisterConnector("alias", db))
	for _, connector := range []string{"", "metrics", "alias"} {
		product, err := component.IdentifierProduct(connector)
		require.NoError(t, err)
		require.Equal(t, "BigQuery", product)
	}
	_, err := component.IdentifierProduct("missing")
	require.ErrorContains(t, err, "not registered")
	_, err = (&SQLComponent{}).IdentifierProduct("")
	require.Error(t, err)
	require.Zero(t, driver.opens.Load())
}
