package column

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	_ "github.com/viant/sqlx/metadata/product/mysql"
)

func nativeConstraintFixture(t *testing.T, schema string) (*Refiner, *sqlite.Harness, *spec.View) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "exclusive-constraints.db")
	db := sqlite.New(t, sqlite.WithDSN(path))
	t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s", t.Name(), path)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE alternate(id INTEGER PRIMARY KEY)", schema))
	r := New(Connections{"main": db.DB})
	view := &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records", SQL: "SELECT r.* FROM records r"}}
	nativeConstraintRefine(t, r, view)
	return r, db, view
}
func nativeConstraintRefine(t *testing.T, r *Refiner, view *spec.View) {
	t.Helper()
	c := &spec.Component{Name: "Private", RootView: view, Settings: &spec.Settings{Mutation: "patch", DefaultConnector: "main"}}
	require.NoError(t, r.RefineRoot(context.Background(), c, nil, nil))
}

func TestPhysicalSourceConstraintsNativeCompositeUniqueAndFreshness(t *testing.T) {
	ctx := context.Background()
	r, db, v := nativeConstraintFixture(t, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,a INTEGER REFERENCES parents(id),b INTEGER REFERENCES parents(id),created TEXT DEFAULT CURRENT_TIMESTAMP)")
	require.NoError(t, db.ExecStatements(ctx, "CREATE UNIQUE INDEX pair_unique ON records(a,b)"))
	a, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.Len(t, a.PrimaryKeys, 1)
	require.Len(t, a.ForeignKeys, 2)
	require.NotEqual(t, a.ForeignKeys[0].ConstrainPosition, a.ForeignKeys[1].ConstrainPosition)
	require.Len(t, a.Indexes, 1)
	require.True(t, a.Indexes[0].Unique)
	require.Equal(t, int64(2), a.Indexes[0].ColumnCount)
	require.Equal(t, "a", a.Indexes[0].Members[0].Column)
	require.Equal(t, "b", a.Indexes[0].Members[1].Column)
	b, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.Equal(t, a, b)
	// Returned copies cannot poison a later read.
	a.Indexes[0].Members[0].Column = "tampered"
	*a.NativeColumns[0].Default = "tampered"
	b, err = r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.Equal(t, "a", b.Indexes[0].Members[0].Column)
	require.NoError(t, db.ExecStatements(ctx, "DROP INDEX pair_unique", "CREATE INDEX pair_unique ON records(a,b)"))
	nonunique, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.False(t, nonunique.Indexes[0].Unique)
	require.NotEqual(t, b, nonunique)
	require.NoError(t, db.ExecStatements(ctx, "DROP INDEX pair_unique", "CREATE UNIQUE INDEX pair_unique ON records(b,a)"))
	reordered, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.Equal(t, "b", reordered.Indexes[0].Members[0].Column)
	require.NotEqual(t, b, reordered)
	require.NoError(t, db.ExecStatements(ctx, "DROP INDEX pair_unique"))
	removed, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.Empty(t, removed.Indexes)
	require.NotEqual(t, reordered, removed)
}

func TestPhysicalSourceConstraintsRejectsHiddenCompositeAndIndexAmbiguity(t *testing.T) {
	t.Run("hidden_composite_PK", func(t *testing.T) {
		r, _, v := nativeConstraintFixture(t, "CREATE TABLE records(id INTEGER,a INTEGER,PRIMARY KEY(id,a))")
		v.Source.SQL = "SELECT r.id FROM records r"
		v.Columns = nil
		nativeConstraintRefine(t, r, v)
		_, err := r.PhysicalSourceConstraints(context.Background(), "main", v)
		require.ErrorContains(t, err, "complete physical primary key")
	})
	for _, index := range []string{"CREATE UNIQUE INDEX bad ON records(a) WHERE a IS NOT NULL", "CREATE UNIQUE INDEX bad ON records(a,lower(b))", "CREATE UNIQUE INDEX bad ON records(lower(b))"} {
		t.Run(index, func(t *testing.T) {
			r, db, v := nativeConstraintFixture(t, "CREATE TABLE records(id INTEGER PRIMARY KEY,a INTEGER,b TEXT)")
			require.NoError(t, db.ExecStatements(context.Background(), index))
			_, err := r.PhysicalSourceConstraints(context.Background(), "main", v)
			require.Error(t, err)
		})
	}
}

func TestPhysicalSourceConstraintsProjectionAliasesAndOverrides(t *testing.T) {
	r, _, v := nativeConstraintFixture(t, "CREATE TABLE records(id INTEGER PRIMARY KEY,a INTEGER REFERENCES parents(id),b TEXT)")
	v.Source.SQL = "SELECT r.id AS record_key,r.a AS parent_key,r.b FROM records r"
	v.Columns = nil
	nativeConstraintRefine(t, r, v)
	facts, err := r.PhysicalSourceConstraints(context.Background(), "main", v)
	require.NoError(t, err)
	require.Equal(t, "id", facts.Projection[0].Physical)
	require.Equal(t, "record_key", facts.Projection[0].Result)
	// A Go-only outer rename retains the SQL alias through the existing Source/tag.
	v.Columns[0].Name = "RecordKey"
	facts, err = r.PhysicalSourceConstraints(context.Background(), "main", v)
	require.NoError(t, err)
	require.Equal(t, "RecordKey", facts.Projection[0].Projected)
	for _, mutation := range []string{"source", "mapping", "duplicate", "computed", "joined", "wildcard_duplicate"} {
		t.Run(mutation, func(t *testing.T) {
			copy := v.Clone()
			switch mutation {
			case "source":
				copy.Columns[0].Source = "a"
			case "mapping":
				copy.Columns[0].Tag = `sqlx:"a|record_key"`
			case "duplicate":
				copy.Source.SQL = "SELECT r.id AS record_key,r.a AS record_key,r.b FROM records r"
			case "wildcard_duplicate":
				copy.Source.SQL = "SELECT r.*,r.id FROM records r"
			case "computed":
				copy.Source.SQL = "SELECT r.id AS record_key,r.a+1 AS parent_key,r.b FROM records r"
			case "joined":
				copy.Source.SQL = "SELECT r.id AS record_key,r.a AS parent_key,r.b FROM records r JOIN parents p ON p.id=r.a"
			}
			_, err := r.PhysicalSourceConstraints(context.Background(), "main", copy)
			require.Error(t, err)
		})
	}
}

func TestPhysicalSourceConstraintsFKDriftFixedTagsAndInvalidAuthority(t *testing.T) {
	ctx := context.Background()
	r, db, v := nativeConstraintFixture(t, "CREATE TABLE records(id INTEGER PRIMARY KEY,a INTEGER REFERENCES parents(id),b TEXT)")
	original, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.NoError(t, db.ExecStatements(ctx, "DROP TABLE records", "CREATE TABLE records(id INTEGER PRIMARY KEY,a INTEGER REFERENCES alternate(id),b TEXT)"))
	fresh, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.NotEqual(t, original.ForeignKeys, fresh.ForeignKeys)
	require.True(t, strings.Contains(v.Columns[1].Tag, "parents"))
	require.Equal(t, "alternate", fresh.ForeignKeys[0].ReferenceTable)
	// Discovery returns the physical target separately from the unchanged tag;
	// actual native emitted-field admission checks the mismatch independently.
	for _, table := range []string{"main.records", "records;DROP TABLE parents", ""} {
		copy := v.Clone()
		copy.Source.Table = table
		_, err := r.PhysicalSourceConstraints(ctx, "main", copy)
		require.Error(t, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = r.PhysicalSourceConstraints(canceled, "main", v)
	require.Error(t, err)
	_, err = r.PhysicalSourceConstraints(nil, "main", v)
	require.Error(t, err)
	_, err = (*Refiner)(nil).PhysicalSourceConstraints(ctx, "main", v)
	require.Error(t, err)
	again, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(fresh, again))
}

// Controlled native metadata rows use the real SQLX registry and materializer.
// They never connect to MySQL and do not satisfy root's native MySQL gate.
type constraintMetadataConnector struct {
	base  driver.Driver
	rows  map[string]*physicalMetadataRows
	calls map[string]int
	fail  string
}

func (c *constraintMetadataConnector) Driver() driver.Driver { return c.base }
func (c *constraintMetadataConnector) Connect(context.Context) (driver.Conn, error) {
	return &constraintMetadataConnection{c}, nil
}

type constraintMetadataConnection struct{ owner *constraintMetadataConnector }

func (c *constraintMetadataConnection) Prepare(q string) (driver.Stmt, error) {
	return &constraintMetadataStatement{owner: c.owner, query: q}, nil
}
func (c *constraintMetadataConnection) Close() error { return nil }
func (c *constraintMetadataConnection) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("no transaction capability")
}

type constraintMetadataStatement struct {
	owner *constraintMetadataConnector
	query string
}

func (s *constraintMetadataStatement) Close() error  { return nil }
func (s *constraintMetadataStatement) NumInput() int { return -1 }
func (s *constraintMetadataStatement) Exec([]driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("no write capability")
}
func (s *constraintMetadataStatement) Query([]driver.Value) (driver.Rows, error) {
	q := s.query
	kind := ""
	switch {
	case strings.Contains(q, "sqlite_version"):
		kind = "product"
	case strings.Contains(q, "processlist"):
		kind = "session"
	case strings.Contains(q, "INFORMATION_SCHEMA.COLUMNS"):
		kind = "columns"
	case strings.Contains(q, "INFORMATION_SCHEMA.TABLES"):
		kind = "tables"
	case strings.Contains(q, "CONSTRAINT_TYPE = 'PRIMARY KEY'"):
		kind = "pk"
	case strings.Contains(q, "CONSTRAINT_TYPE = 'FOREIGN KEY'"):
		kind = "fk"
	case strings.Contains(q, "COUNT(*) AS INDEX_COLUMN_COUNT"):
		kind = "indexes"
	case strings.Contains(q, "INDEX_PREFIX_LENGTH"):
		kind = "members"
	}
	s.owner.calls[kind]++
	if s.owner.fail == kind {
		return nil, fmt.Errorf("controlled native read failure %s", kind)
	}
	r := s.owner.rows[kind]
	if r == nil {
		return nil, fmt.Errorf("unexpected native metadata query %q", q)
	}
	return &physicalMetadataRows{columns: r.columns, values: r.values}, nil
}
func constraintMySQLFixture(t *testing.T) (*Refiner, *constraintMetadataConnector, *spec.View) {
	t.Helper()
	base := sqlite.New(t, sqlite.WithDSN(filepath.Join(t.TempDir(), "exclusive-driver.db")))
	c := &constraintMetadataConnector{base: base.DB.Driver(), calls: map[string]int{}, rows: map[string]*physicalMetadataRows{}}
	put := func(kind string, columns []string, values ...[]driver.Value) {
		c.rows[kind] = &physicalMetadataRows{columns: columns, values: values}
	}
	put("product", []string{"VERSION"}, []driver.Value{"MySQL - 8.0.33"})
	put("session", []string{"PID", "USER_NAME", "CATALOG", "SCHEMA_NAME", "APP_NAME"}, []driver.Value{"", "", "", "main", ""})
	put("columns", []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "IS_NULLABLE", "COLUMN_KEY", "IS_AUTOINCREMENT"}, []driver.Value{"", "main", "records", "id", int64(1), "int", "NO", "PRI", int64(1)}, []driver.Value{"", "main", "records", "a", int64(2), "int", "YES", "MUL", nil})
	put("tables", []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "ENGINE"}, []driver.Value{"", "main", "records", "InnoDB"})
	keys := []string{"CONSTRAINT_NAME", "CONSTRAINT_TYPE", "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME", "POSITION_IN_UNIQUE_CONSTRAINT", "ON_UPDATE", "ON_DELETE", "ON_MATCH"}
	put("pk", keys, []driver.Value{"PRIMARY", "PRIMARY KEY", "", "main", "records", "id", int64(1), "", "", "", int64(0), "", "", ""})
	put("fk", keys, []driver.Value{"fk", "FOREIGN KEY", "", "main", "records", "a", int64(1), "main", "parents", "id", int64(1), "RESTRICT", "CASCADE", "NONE"})
	put("indexes", []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_SCHEMA", "INDEX_NAME", "INDEX_UNIQUE", "INDEX_TYPE", "INDEX_COLUMN_COUNT"}, []driver.Value{"", "main", "records", "main", "idx", "0", "BTREE", int64(1)})
	put("members", []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "COLUMN_NAME", "INDEX_POSITION", "COLLATION", "INDEX_PREFIX_LENGTH"}, []driver.Value{"", "main", "records", "idx", "a", int64(1), "A", int64(0)})
	db := sql.OpenDB(c)
	t.Cleanup(func() { db.Close() })
	v := &spec.View{Source: &spec.ViewSource{Table: "records", SQL: "SELECT r.id FROM records r"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}}}
	return New(Connections{"main": db}), c, v
}
func TestPhysicalSourceConstraintsControlledMySQLGuards(t *testing.T) {
	r, c, v := constraintMySQLFixture(t)
	a, err := r.PhysicalSourceConstraints(context.Background(), "main", v)
	require.NoError(t, err)
	require.Equal(t, "8.0.33", a.ProductVersion)
	require.Equal(t, "InnoDB", a.Engine)
	require.True(t, a.Indexes[0].Unique)
	b, err := r.PhysicalSourceConstraints(context.Background(), "main", v)
	require.NoError(t, err)
	require.Equal(t, a, b)
	for _, kind := range []string{"product", "session", "columns", "tables", "pk", "fk", "indexes", "members"} {
		require.Equal(t, 2, c.calls[kind])
	}
	cases := []struct {
		name, kind string
		mutate     func(*physicalMetadataRows)
	}{
		{"zero_pk", "pk", func(r *physicalMetadataRows) { r.values[0][6] = int64(0) }},
		{"zero_fk", "fk", func(r *physicalMetadataRows) { r.values[0][6] = int64(0) }},
		{"missing_target_position", "fk", func(r *physicalMetadataRows) { r.values[0][10] = int64(0) }},
		{"target_schema_only", "fk", func(r *physicalMetadataRows) { r.values[0][7] = "other" }},
		{"missing_action", "fk", func(r *physicalMetadataRows) { r.values[0][11] = "" }},
		{"unknown_action", "fk", func(r *physicalMetadataRows) { r.values[0][11] = "UNKNOWN" }},
		{"unknown_match", "fk", func(r *physicalMetadataRows) { r.values[0][13] = "UNKNOWN" }},
		{"duplicate_fk", "fk", func(r *physicalMetadataRows) { r.values = append(r.values, r.values[0]) }},
		{"non_innodb", "tables", func(r *physicalMetadataRows) { r.values[0][3] = "MyISAM" }},
		{"missing_engine", "tables", func(r *physicalMetadataRows) { r.values[0][3] = nil }},
		{"missing_table", "tables", func(r *physicalMetadataRows) { r.values = nil }},
		{"duplicate_table", "tables", func(r *physicalMetadataRows) { r.values = append(r.values, r.values[0]) }},
		{"unsupported_version", "product", func(r *physicalMetadataRows) { r.values[0][0] = "MySQL - 5.6.51" }},
		{"missing_schema", "session", func(r *physicalMetadataRows) { r.values[0][3] = "" }},
		{"missing_count", "indexes", func(r *physicalMetadataRows) { r.values[0][7] = nil }},
		{"hidden_member", "indexes", func(r *physicalMetadataRows) { r.values[0][7] = int64(2) }},
		{"unknown_unique", "indexes", func(r *physicalMetadataRows) { r.values[0][5] = "UNKNOWN" }},
		{"prefix", "members", func(r *physicalMetadataRows) { r.values[0][7] = int64(5) }},
		{"missing_prefix", "members", func(r *physicalMetadataRows) { r.values[0][7] = nil }},
		{"unknown_direction", "members", func(r *physicalMetadataRows) { r.values[0][6] = "UNKNOWN" }},
		{"gapped_position", "members", func(r *physicalMetadataRows) { r.values[0][5] = int64(2) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, c, v := constraintMySQLFixture(t)
			tc.mutate(c.rows[tc.kind])
			_, err := r.PhysicalSourceConstraints(context.Background(), "main", v)
			require.Error(t, err)
		})
	}
	for _, kind := range []string{"product", "session", "columns", "tables", "pk", "fk", "indexes", "members"} {
		t.Run("error_"+kind, func(t *testing.T) {
			r, c, v := constraintMySQLFixture(t)
			c.fail = kind
			_, err := r.PhysicalSourceConstraints(context.Background(), "main", v)
			require.Error(t, err)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := c.calls["product"]
	_, err = r.PhysicalSourceConstraints(ctx, "main", v)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, before, c.calls["product"])
}

func TestPhysicalSourceConstraintsRejectsUnresolvedResourcesBeforeDiscovery(t *testing.T) {
	for _, source := range []*spec.ViewSource{
		{Table: "records", URI: "records.sql"},
		{Table: "records", SQL: "SELECT * FROM records", Embeds: []*spec.EmbeddedSQLRef{{Path: "fields.sql", Raw: "${embed:fields.sql}"}}},
	} {
		r, c, view := constraintMySQLFixture(t)
		view.Source = source
		_, err := r.PhysicalSourceConstraints(context.Background(), "main", view)
		require.ErrorContains(t, err, "resolved SQL resource")
		require.Empty(t, c.calls, "unresolved ownership issued native metadata reads")
	}
	r, _, view := constraintMySQLFixture(t)
	view.Source.URI = "retained-provenance.sql"
	_, err := r.PhysicalSourceConstraints(context.Background(), "main", view)
	require.NoError(t, err, "resolved SQL with retained URI must preserve resolver precedence")
}
