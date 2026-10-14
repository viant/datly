package reader_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/io/read/cache"
)

type scalarCaption string
type scalarCount int16
type scannerCount int64

func (s *scannerCount) Scan(value any) error {
	var n sql.NullInt64
	if err := n.Scan(value); err != nil {
		return err
	}
	if !n.Valid {
		*s = -1
	} else {
		*s = scannerCount(n.Int64)
	}
	return nil
}

type nullableScalarRow struct {
	ID      int           `sqlx:"id"`
	Caption scalarCaption `sqlx:"caption"`
	Count   scalarCount   `sqlx:"count"`
	Enabled bool          `sqlx:"enabled"`
	Ratio   float64       `sqlx:"ratio"`
	Pointer *string       `sqlx:"pointer"`
	Scanned scannerCount  `sqlx:"scanned"`
}
type nullableScalarOutput struct{ Rows []nullableScalarRow }

func TestScalarNullsPreserveTypesAndCacheSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,caption TEXT,count INTEGER,enabled BOOLEAN,ratio REAL,pointer TEXT,scanned INTEGER)", "INSERT INTO records VALUES(1,NULL,NULL,NULL,NULL,NULL,NULL),(2,'two',7,1,1.5,'present',9),(3,NULL,NULL,NULL,NULL,NULL,NULL)"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ScalarRecords"}, RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT scanned,ratio,pointer,enabled,count,caption,id FROM records ORDER BY id"}}}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[nullableScalarOutput](), DirectViewField: "Rows"})
	require.NoError(t, err)
	store, err := (cacheconfig.Config{Identity: "scalar/records", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[nullableScalarOutput](), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{plan.Root.View: store}})
	require.NoError(t, err)
	for pass := 0; pass < 2; pass++ {
		actual, err := execution.Read(ctx, &struct{}{}, nil, nil)
		require.NoError(t, err)
		rows := actual.(*nullableScalarOutput).Rows
		require.Len(t, rows, 3)
		require.Equal(t, nullableScalarRow{ID: 1, Scanned: -1}, rows[0])
		require.Equal(t, nullableScalarRow{ID: 3, Scanned: -1}, rows[2])
		require.Equal(t, scalarCaption("two"), rows[1].Caption)
		require.Equal(t, scalarCount(7), rows[1].Count)
		require.True(t, rows[1].Enabled)
		require.Equal(t, 1.5, rows[1].Ratio)
		require.Equal(t, "present", *rows[1].Pointer)
		require.Equal(t, scannerCount(9), rows[1].Scanned)
		if pass == 0 {
			require.NoError(t, db.ExecStatements(ctx, "DROP TABLE records"))
		}
	}
}
