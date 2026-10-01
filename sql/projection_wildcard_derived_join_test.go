package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestPreparedWildcardDerivedPrimaryAndExplicitJoinedFields(t *testing.T) {
	source := `SELECT result.* FROM (SELECT CAST(s.expires_at AS CHAR) AS observed,s.*,u.username,u.display_name FROM (SELECT s.* FROM sessions s WHERE s.id=1) s LEFT JOIN users u ON u.id=s.user_id) result`
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "sessions"}}, Columns: []*data.Column{{Name: "Id", Column: "id"}, {Name: "UserId", Column: "user_id"}, {Name: "ExpiresAt", Column: "expires_at"}, {Name: "Observed", Column: "observed"}, {Name: "Username", Column: "username"}, {Name: "DisplayName", Column: "display_name"}}}
	h := testharness.NewSQLiteHarness(t)
	require.NoError(t, h.ExecStatements(context.Background(), `CREATE TABLE sessions(id INTEGER,user_id INTEGER,expires_at TEXT)`, `CREATE TABLE users(id INTEGER,username TEXT,display_name TEXT)`, `INSERT INTO sessions VALUES(1,7,'expired'),(2,8,'unrelated')`, `INSERT INTO users VALUES(7,'owner','Owner')`))
	projected, err := (SelectorProjection{SQL: source, View: view}).Prepare([]string{"id", "observed", "username"})
	require.NoError(t, err)
	query := projected.Render(projected.Source)
	var id int
	var observed, username string
	require.NoError(t, h.DB.QueryRow(query).Scan(&id, &observed, &username))
	require.Equal(t, 1, id)
	require.Equal(t, "expired", observed)
	require.Equal(t, "owner", username)
	for _, table := range []string{"", "users"} {
		view.Spec.Source.Table = table
		_, err := (SelectorProjection{SQL: source, View: view}).Prepare([]string{"id"})
		require.Error(t, err, "wrong physical schema must not be borrowed")
	}
	view.Spec.Source.Table = "sessions"
	for _, query := range []string{
		`SELECT result.* FROM (SELECT s.*,u.username,u.username FROM (SELECT s.* FROM sessions s) s LEFT JOIN users u ON u.id=s.user_id) result`,
		`SELECT result.* FROM (SELECT s.*,unknown.username FROM (SELECT s.* FROM sessions s) s LEFT JOIN users u ON u.id=s.user_id) result`,
		`SELECT result.* FROM (SELECT *,u.username FROM (SELECT s.* FROM sessions s) s LEFT JOIN users u ON u.id=s.user_id) result`,
		`SELECT result.* FROM (SELECT s.*,s.id FROM (SELECT s.* FROM sessions s) s LEFT JOIN users u ON u.id=s.user_id) result`,
		`SELECT result.* FROM (SELECT s.*,u.username FROM (SELECT s.* FROM sessions s UNION SELECT s.* FROM sessions s) s LEFT JOIN users u ON u.id=s.user_id) result`,
	} {
		_, err := (SelectorProjection{SQL: query, View: view}).Prepare([]string{"id"})
		require.Error(t, err, query)
	}
}
