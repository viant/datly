package column

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type sqliteOrder struct {
	VendorID int    `sqlx:"name=VENDOR_ID"`
	Name     string `sqlx:"name=NAME"`
}

func TestDetector_Resolve_SQLiteWildcard(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "shape_detector.sqlite")
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(ctx, `CREATE TABLE VENDOR (VENDOR_ID INTEGER NOT NULL, NAME TEXT NOT NULL, STATUS TEXT)`)
	require.NoError(t, err)

	resource := view.EmptyResource()
	resource.Connectors = []*view.Connector{{Connection: view.Connection{DBConfig: view.DBConfig{Name: "db", Driver: "sqlite3", DSN: dsn}}}}

	aView := &view.View{
		Name:      "vendor",
		Table:     "VENDOR",
		Schema:    state.NewSchema(reflect.TypeOf(sqliteOrder{}), state.WithMany()),
		Template:  view.NewTemplate("SELECT * FROM VENDOR"),
		Connector: view.NewRefConnector("db"),
	}

	resolved, err := New().Resolve(ctx, resource, aView)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(resolved), 3)

	// Schema order is preserved, discovered extra columns are appended.
	assert.Equal(t, "VENDOR_ID", strings.ToUpper(resolved[0].Name))
	assert.Equal(t, "NAME", strings.ToUpper(resolved[1].Name))

	names := make([]string, 0, len(resolved))
	for _, item := range resolved {
		names = append(names, strings.ToUpper(item.Name))
	}
	assert.Contains(t, names, "STATUS")
}
