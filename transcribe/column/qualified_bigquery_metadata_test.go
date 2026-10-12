package column

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx/metadata/database"
	_ "github.com/viant/sqlx/metadata/product/bigquery"
	"github.com/viant/sqlx/metadata/sink"
)

// Controlled rows still pass through the real SQLX BigQuery registry, query
// builder and column materializer. Unexpected session reads fail immediately.
type qualifiedBQMetadataConnector struct {
	base                   driver.Driver
	catalog, schema, table string
	calls                  int
}

func TestBigQueryMetadataTwoPartPathPreservesConnectionSession(t *testing.T) {
	h := sqlite.New(t)
	c := &qualifiedBQMetadataConnector{base: h.DB.Driver(), catalog: "connection-project", schema: "other_dataset", table: "records"}
	db := sql.OpenDB(c)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	original := sink.Session{Catalog: "connection-project", Schema: "default_dataset", Username: "owner", Region: "region"}
	m := &discoveryMetadata{products: map[*sql.DB]database.Product{db: {Name: "BigQuery"}}, sessions: map[*sql.DB]sink.Session{db: original}}
	facts, err := m.loadTableConstraints(context.Background(), db, "other_dataset.records")
	require.NoError(t, err)
	require.Len(t, facts, 2)
	require.Equal(t, original, m.sessions[db])
	product := m.products[db]
	resolved, table, err := m.tableSession(context.Background(), db, &product, "`records`")
	require.NoError(t, err)
	require.Equal(t, "records", table)
	require.Equal(t, original, *resolved)
}

func TestBigQueryMetadataRejectsInvalidPathAndCancelledRead(t *testing.T) {
	h := sqlite.New(t)
	c := &qualifiedBQMetadataConnector{base: h.DB.Driver()}
	db := sql.OpenDB(c)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	m := &discoveryMetadata{products: map[*sql.DB]database.Product{db: {Name: "BigQuery"}}}
	for _, table := range []string{"project.dataset.records; DROP TABLE records", "project.dataset.records.extra", "`project..records`"} {
		_, err := m.loadTableConstraints(context.Background(), db, table)
		require.Error(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.loadTableConstraints(ctx, db, "project.dataset.records")
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, c.calls)
}

func (c *qualifiedBQMetadataConnector) Driver() driver.Driver { return c.base }
func (c *qualifiedBQMetadataConnector) Connect(context.Context) (driver.Conn, error) {
	return &qualifiedBQMetadataConnection{owner: c}, nil
}

type qualifiedBQMetadataConnection struct{ owner *qualifiedBQMetadataConnector }

func (c *qualifiedBQMetadataConnection) Prepare(query string) (driver.Stmt, error) {
	return &qualifiedBQMetadataStatement{owner: c.owner, query: query}, nil
}
func (*qualifiedBQMetadataConnection) Close() error { return nil }
func (*qualifiedBQMetadataConnection) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("no transactions")
}

type qualifiedBQMetadataStatement struct {
	owner *qualifiedBQMetadataConnector
	query string
}

func (*qualifiedBQMetadataStatement) Close() error  { return nil }
func (*qualifiedBQMetadataStatement) NumInput() int { return -1 }
func (*qualifiedBQMetadataStatement) Exec([]driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("no writes")
}
func (s *qualifiedBQMetadataStatement) Query(args []driver.Value) (driver.Rows, error) {
	c := s.owner
	if !strings.Contains(s.query, "FROM "+c.catalog+"."+c.schema+".INFORMATION_SCHEMA.COLUMNS") {
		return nil, fmt.Errorf("wrong namespace or unexpected metadata read: %s", s.query)
	}
	if len(args) != 1 || args[0] != c.table {
		return nil, fmt.Errorf("wrong metadata criteria: %#v", args)
	}
	c.calls++
	return &physicalMetadataRows{columns: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_KEY", "COLUMN_DEFAULT"}, values: [][]driver.Value{
		{c.catalog, c.schema, c.table, "id", "INT64", "NO", "PRI", "7"},
		{c.catalog, c.schema, c.table, "name", "STRING", "YES", "", ""},
	}}, nil
}

func TestQualifiedBigQueryConstraintsUseSourceNamespaceWithoutSessionQuery(t *testing.T) {
	h := sqlite.New(t)
	c := &qualifiedBQMetadataConnector{base: h.DB.Driver()}
	db := sql.OpenDB(c)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	m := &discoveryMetadata{products: map[*sql.DB]database.Product{db: {Name: "BigQuery"}}}
	for _, tc := range []struct{ source, catalog, schema, table string }{
		{"project-one.dataset_a.records", "project-one", "dataset_a", "records"},
		{"`project-two.dataset_b.other_records`", "project-two", "dataset_b", "other_records"},
		{"[project-three:dataset_c.legacy_records]", "project-three", "dataset_c", "legacy_records"},
	} {
		t.Run(tc.source, func(t *testing.T) {
			c.catalog, c.schema, c.table = tc.catalog, tc.schema, tc.table
			facts, err := m.loadTableConstraints(context.Background(), db, tc.source)
			require.NoError(t, err)
			require.Len(t, facts, 2)
			require.True(t, facts["id"].primaryKey)
			require.True(t, facts["id"].notNull)
			require.NotNil(t, facts["id"].defaultValue)
			require.Equal(t, "7", *facts["id"].defaultValue)
			require.False(t, facts["name"].notNull)
			require.Empty(t, m.sessions, "a source namespace must not poison the connection session cache")
		})
	}
	require.Equal(t, 3, c.calls, "each table still discovers fresh constraints")
}
