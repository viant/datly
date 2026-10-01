package builder

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"testing"
)

func TestGeneratedProjectionIdentifiers(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, `CREATE TABLE reserved_values("select" TEXT, "KEY" TEXT, id INTEGER)`, `INSERT INTO reserved_values VALUES('selected', 'key value', 1)`))
	for _, mode := range []string{"build", "bound"} {
		for _, tc := range []struct{ name, source, column, want string }{
			{"star", `SELECT * FROM reserved_values`, "select", "selected"},
			{"qualified star", `SELECT sc.* FROM reserved_values sc`, "KEY", "key value"},
			{"mixed star", `SELECT sc.*, 'literal KEY' AS extra FROM reserved_values sc`, "KEY", "key value"},
			{"authored literal", `SELECT sc.*, 'literal KEY' AS extra FROM reserved_values sc`, "extra", "literal KEY"},
			{"group wrapper", `SELECT "KEY", COUNT(*) AS count FROM reserved_values GROUP BY "KEY"`, "KEY", "key value"},
			{"quoted output", `SELECT * FROM reserved_values`, `"select"`, "selected"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				view := &data.View{Columns: []*data.Column{{Name: "select"}, {Name: "KEY"}, {Name: "id"}}}
				opts := []BuilderOption{WithBuilderSQL(tc.source), WithBuilderView(view), WithBuilderProjection([]string{tc.column}), WithBuilderDialect(&info.Dialect{Product: database.Product{Name: "SQLite"}})}
				var q *cache.ParmetrizedQuery
				var err error
				if mode == "bound" {
					q, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: tc.source}, opts...)
				} else {
					q, err = NewBuilder().Build(ctx, opts...)
				}
				require.NoError(t, err)
				var got string
				require.NoError(t, db.DB.QueryRowContext(ctx, q.SQL, q.Args...).Scan(&got), q.SQL)
				require.Equal(t, tc.want, got)
				if tc.name == "authored literal" {
					require.Contains(t, q.SQL, "'literal KEY' AS extra")
				}
				if tc.name == "group wrapper" {
					require.Contains(t, q.SQL, tc.source)
				}
			})
		}
	}
	for _, tc := range []struct{ product, column, want string }{
		{"MySQL", "KEY", "`KEY`"}, {"PostgreSQL", "KEY", `"key"`}, {"PostgreSQL", `"KEY"`, `"KEY"`}, {"", "id", "id"},
	} {
		t.Run(tc.product+tc.column, func(t *testing.T) {
			opts := []BuilderOption{WithBuilderSQL("SELECT sc.* FROM reserved_values sc"), WithBuilderView(&data.View{Columns: []*data.Column{{Name: tc.column}}}), WithBuilderProjection([]string{tc.column})}
			if tc.product != "" {
				opts = append(opts, WithBuilderDialect(&info.Dialect{Product: database.Product{Name: tc.product}}))
			}
			q, err := NewBuilder().Build(ctx, opts...)
			require.NoError(t, err)
			require.Equal(t, "SELECT "+tc.want+" FROM (SELECT sc.* FROM reserved_values sc) AS datly_view", q.SQL)
		})
	}
}

func TestGeneratedNullProjectionIdentifiers(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, `CREATE TABLE null_reserved("select" TEXT)`, `INSERT INTO null_reserved VALUES(NULL)`))
	source := `SELECT sc.* FROM null_reserved sc`
	view := &data.View{Columns: []*data.Column{{Name: "select", Nullable: true, NullFallback: "'literal KEY'"}}}
	q, err := NewBuilder().Build(ctx, WithBuilderSQL(source), WithBuilderView(view), WithBuilderProjection([]string{"select"}), WithBuilderDialect(&info.Dialect{Product: database.Product{Name: "SQLite"}}))
	require.NoError(t, err)
	require.Contains(t, q.SQL, `COALESCE("select", 'literal KEY') AS "select"`)
	require.Contains(t, q.SQL, source)
	var got string
	require.NoError(t, db.DB.QueryRowContext(ctx, q.SQL).Scan(&got))
	require.Equal(t, "literal KEY", got)
}
