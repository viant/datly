package executionhint

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestExecutionHintLiveMySQL(t *testing.T) {
	dsn := os.Getenv("SQLX_SCOPED_MYSQL_DSN")
	if dsn == "" {
		t.Skip("live MySQL fixture not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var before, effective, after string
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT @@optimizer_switch").Scan(&before))
	query, err := Render("SELECT @@optimizer_switch", "MySQL", option(false))
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&effective))
	require.Contains(t, effective, "condition_fanout_filter=off")
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT @@optimizer_switch").Scan(&after))
	require.Equal(t, before, after, "statement hint must not leak into the pooled session")
	plan, err := conn.QueryContext(ctx, "EXPLAIN "+query)
	require.NoError(t, err)
	require.NoError(t, plan.Close())
	warnings, err := conn.QueryContext(ctx, "SHOW WARNINGS")
	require.NoError(t, err)
	defer warnings.Close()
	for warnings.Next() {
		var level, message string
		var code int
		require.NoError(t, warnings.Scan(&level, &code, &message))
		if strings.EqualFold(level, "warning") || strings.EqualFold(level, "error") {
			t.Errorf("hint warning %d: %s", code, message)
		}
	}
	require.NoError(t, warnings.Err())
}
