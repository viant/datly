package transcribe

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

// Reuses the controlled-native-metadata pattern from column's constraint tests.
// The SQLite driver identity selects the existing version probe; all queries are
// answered here through real SQLX metadata scanning. No database is opened and
// neither execution nor transactions have a write capability.
type ownerPrefixMetadata struct {
	prefix int64
	calls  map[string]int
}

func (*ownerPrefixMetadata) Driver() driver.Driver { return &sqlite3.SQLiteDriver{} }
func (m *ownerPrefixMetadata) Connect(context.Context) (driver.Conn, error) {
	return &ownerPrefixConnection{m}, nil
}

type ownerPrefixConnection struct{ metadata *ownerPrefixMetadata }

func (c *ownerPrefixConnection) Prepare(q string) (driver.Stmt, error) {
	return &ownerPrefixStatement{c.metadata, q}, nil
}
func (*ownerPrefixConnection) Close() error { return nil }
func (*ownerPrefixConnection) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("no transaction capability")
}

type ownerPrefixStatement struct {
	metadata *ownerPrefixMetadata
	query    string
}

func (*ownerPrefixStatement) Close() error  { return nil }
func (*ownerPrefixStatement) NumInput() int { return -1 }
func (*ownerPrefixStatement) Exec([]driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("no write capability")
}
func (s *ownerPrefixStatement) Query([]driver.Value) (driver.Rows, error) {
	var key string
	var names []string
	var values [][]driver.Value
	row := func(v ...driver.Value) { values = append(values, v) }
	switch q := s.query; {
	case strings.Contains(q, "sqlite_version"):
		key = "product"
		names = []string{"VERSION"}
		row("MySQL - 8.0.33")
	case strings.Contains(q, "processlist"):
		key = "session"
		names = []string{"PID", "USER_NAME", "CATALOG", "SCHEMA_NAME", "APP_NAME"}
		row("", "", "", "main", "")
	case strings.Contains(q, "INFORMATION_SCHEMA.COLUMNS"):
		key = "columns"
		names = []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "IS_NULLABLE", "COLUMN_KEY", "IS_AUTOINCREMENT"}
		row("", "main", "records", "id", int64(1), "int", "NO", "PRI", int64(1))
		row("", "main", "records", "url", int64(2), "varchar", "YES", "MUL", nil)
	case strings.Contains(q, "INFORMATION_SCHEMA.TABLES"):
		key = "tables"
		names = []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "ENGINE"}
		row("", "main", "records", "InnoDB")
	case strings.Contains(q, "CONSTRAINT_TYPE = 'PRIMARY KEY'"):
		key = "pk"
		names = ownerPrefixKeyColumns()
		row("PRIMARY", "PRIMARY KEY", "", "main", "records", "id", int64(1), "", "", "", int64(0), "", "", "")
	case strings.Contains(q, "CONSTRAINT_TYPE = 'FOREIGN KEY'"):
		key = "fk"
		names = ownerPrefixKeyColumns()
	case strings.Contains(q, "COUNT(*) AS INDEX_COLUMN_COUNT"):
		key = "indexes"
		names = []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_SCHEMA", "INDEX_NAME", "INDEX_UNIQUE", "INDEX_TYPE", "INDEX_COLUMN_COUNT"}
		row("", "main", "records", "main", "url_idx", "1", "BTREE", int64(1))
	case strings.Contains(q, "INDEX_PREFIX_LENGTH"):
		key = "members"
		names = []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "COLUMN_NAME", "INDEX_POSITION", "COLLATION", "INDEX_PREFIX_LENGTH"}
		row("", "main", "records", "url_idx", "url", int64(1), "A", s.metadata.prefix)
	default:
		return nil, fmt.Errorf("unexpected metadata query %q", q)
	}
	s.metadata.calls[key]++
	return &ownerPrefixRows{names: names, values: values}, nil
}
func ownerPrefixKeyColumns() []string {
	return []string{"CONSTRAINT_NAME", "CONSTRAINT_TYPE", "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME", "POSITION_IN_UNIQUE_CONSTRAINT", "ON_UPDATE", "ON_DELETE", "ON_MATCH"}
}

type ownerPrefixRows struct {
	names  []string
	values [][]driver.Value
	index  int
}

func (r *ownerPrefixRows) Columns() []string { return r.names }
func (*ownerPrefixRows) Close() error        { return nil }
func (r *ownerPrefixRows) Next(dest []driver.Value) error {
	if r.index == len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func TestFactoryOwnerPrefixDriftRejectedAtValidationAndPublication(t *testing.T) {
	for _, changed := range []int64{512, 0} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			ctx := t.Context()
			metadata := &ownerPrefixMetadata{prefix: 1024, calls: map[string]int{}}
			db := sql.OpenDB(metadata)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			refiner := column.New(column.Connections{"main": db})
			baseline, err := refiner.PhysicalTableConstraints(ctx, "main", "records")
			require.NoError(t, err)
			require.Len(t, baseline.Indexes, 1)
			require.False(t, baseline.Indexes[0].Unique)
			require.Equal(t, int64(1024), *baseline.Indexes[0].Members[0].PrefixLength)
			reference := &factoryOwnerReference{source: &Source{ColumnRefiner: refiner}, physical: []factoryOwnerPhysical{{connector: "main", facts: baseline}}}
			compiler := &Compiler{factoryReferences: []*factoryOwnerReference{reference}}
			publication := compiler.validateFactoryOwnerSources(ctx)
			require.NotNil(t, publication)
			require.NoError(t, reference.validate(ctx))
			require.NoError(t, publication(func(path string) string { return path }))
			sealed := *baseline.Indexes[0].Members[0].PrefixLength
			metadata.prefix = changed
			require.ErrorContains(t, reference.validate(ctx), "factory owner native schema changed after source compilation")
			require.ErrorContains(t, publication(func(path string) string { return path }), "factory owner native schema changed after source compilation")
			require.Equal(t, sealed, *reference.physical[0].facts.Indexes[0].Members[0].PrefixLength, "fresh discovery replaced the validated baseline")
			for _, kind := range []string{"product", "session", "columns", "tables", "pk", "fk", "indexes", "members"} {
				require.Equal(t, 5, metadata.calls[kind], "each validation must discover fresh complete facts: %s", kind)
			}
		})
	}
}
