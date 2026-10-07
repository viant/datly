package column

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
)

// The wrapper keeps the real SQLite product/connection; only selected read
// metadata rows are controlled to prove fail-closed identity admission.
type physicalMetadataConnector struct {
	base     driver.Driver
	dsn      string
	sessions [][]driver.Value
	columns  [][]driver.Value
	calls    map[string]int
	fail     string
}

func (c *physicalMetadataConnector) Driver() driver.Driver { return c.base }
func (c *physicalMetadataConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &physicalMetadataConnection{Conn: conn, owner: c}, nil
}

type physicalMetadataConnection struct {
	driver.Conn
	owner *physicalMetadataConnector
}

func (c *physicalMetadataConnection) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &physicalMetadataStatement{Stmt: stmt, owner: c.owner, query: query}, nil
}

type physicalMetadataStatement struct {
	driver.Stmt
	owner *physicalMetadataConnector
	query string
}

func (s *physicalMetadataStatement) Query(args []driver.Value) (driver.Rows, error) {
	kind := metadataQueryKind(s.query)
	s.owner.calls[kind]++
	if s.owner.fail == kind {
		return nil, errors.New("controlled metadata read failure")
	}
	if kind == "session" && s.owner.sessions != nil {
		return &physicalMetadataRows{columns: []string{"PID", "USER_NAME", "CATALOG", "SCHEMA_NAME", "APP_NAME"}, values: s.owner.sessions}, nil
	}
	if kind == "columns" && s.owner.columns != nil {
		return &physicalMetadataRows{columns: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"}, values: s.owner.columns}, nil
	}
	return s.Stmt.Query(args)
}

type physicalMetadataRows struct {
	columns []string
	values  [][]driver.Value
	next    int
}

func (r *physicalMetadataRows) Columns() []string { return r.columns }
func (r *physicalMetadataRows) Close() error      { return nil }
func (r *physicalMetadataRows) Next(dest []driver.Value) error {
	if r.next == len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.next])
	r.next++
	return nil
}

func physicalIdentityTestDB(t *testing.T) (*Refiner, *physicalMetadataConnector, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "exclusive-identity.db")
	h := sqlite.New(t, sqlite.WithDSN(path))
	require.NoError(t, h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"))
	t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s", t.Name(), path)
	connector := &physicalMetadataConnector{base: h.DB.Driver(), dsn: path, calls: map[string]int{}}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { db.Close() })
	return New(Connections{"main": db}), connector, db
}
func TestPhysicalSourceIdentityFreshReadOnly(t *testing.T) {
	refiner, connector, db := physicalIdentityTestDB(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		actual, err := refiner.PhysicalSourceIdentity(ctx, "main", "records")
		require.NoError(t, err)
		require.Equal(t, PhysicalSourceIdentity{Connector: "main", Schema: "main", Table: "records"}, actual)
	}
	require.Equal(t, 2, connector.calls["product"])
	require.Equal(t, 2, connector.calls["session"])
	require.Equal(t, 2, connector.calls["columns"])
	var count int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM records").Scan(&count))
	require.Zero(t, count)
	connector.sessions = [][]driver.Value{{"", "", "", "changed", ""}}
	actual, err := refiner.PhysicalSourceIdentity(ctx, "main", "records")
	require.NoError(t, err)
	require.Equal(t, "changed", actual.Schema, "session identity cannot be cached")
}
func TestPhysicalSourceIdentityRejectsAmbiguousMissingAndConflictingMetadata(t *testing.T) {
	cases := []struct {
		name              string
		sessions, columns [][]driver.Value
		fail              string
	}{
		{name: "no-session", sessions: [][]driver.Value{}},
		{name: "empty-schema", sessions: [][]driver.Value{{"", "", "", "", ""}}},
		{name: "multiple-sessions", sessions: [][]driver.Value{{"", "", "", "main", ""}, {"", "", "", "attached", ""}}},
		{name: "no-columns", columns: [][]driver.Value{}},
		{name: "wrong-table", columns: [][]driver.Value{{"", "main", "other", "id", "INTEGER"}}},
		{name: "wrong-schema", columns: [][]driver.Value{{"", "other", "records", "id", "INTEGER"}}},
		{name: "wrong-catalog", columns: [][]driver.Value{{"other", "main", "records", "id", "INTEGER"}}},
		{name: "product-error", fail: "product"}, {name: "session-error", fail: "session"}, {name: "columns-error", fail: "columns"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, c, _ := physicalIdentityTestDB(t)
			c.sessions, c.columns, c.fail = tc.sessions, tc.columns, tc.fail
			_, err := r.PhysicalSourceIdentity(context.Background(), "main", "records")
			require.Error(t, err)
		})
	}
}
func TestPhysicalSourceIdentityInvalidAuthority(t *testing.T) {
	r, c, _ := physicalIdentityTestDB(t)
	for _, table := range []string{"", "main.records", "records r", "$Unsafe.Table", "records;DELETE", "_"} {
		_, err := r.PhysicalSourceIdentity(context.Background(), "main", table)
		require.Error(t, err)
	}
	for _, connector := range []string{"", " main", "missing", "$main"} {
		_, err := r.PhysicalSourceIdentity(context.Background(), connector, "records")
		require.Error(t, err)
	}
	_, err := r.PhysicalSourceIdentity(context.Background(), "main", "absent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "absent")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := c.calls["product"]
	_, err = r.PhysicalSourceIdentity(ctx, "main", "records")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, before, c.calls["product"])
	_, err = (*Refiner)(nil).PhysicalSourceIdentity(context.Background(), "main", "records")
	require.Error(t, err)
	_, err = New(nil).PhysicalSourceIdentity(context.Background(), "main", "records")
	require.Error(t, err)
	_, err = r.PhysicalSourceIdentity(nil, "main", "records")
	require.Error(t, err)
	require.False(t, strings.Contains(err.Error(), "dsn"))
}
