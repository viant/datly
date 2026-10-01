package locking

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func dialect(name string) *info.Dialect { return &info.Dialect{Product: database.Product{Name: name}} }
func TestPhysicalRowLockPlacement(t *testing.T) {
	source := `SELECT rows.id FROM (SELECT r.id FROM records r WHERE r.id=? ORDER BY r.id LIMIT 1) rows`
	got, err := Apply(source, "records r", dialect("mysql"), true)
	require.NoError(t, err)
	require.Contains(t, got, "LIMIT 1\nFOR UPDATE) rows")
	require.False(t, strings.HasSuffix(got, "FOR UPDATE"))
	require.Equal(t, 1, strings.Count(got, "FOR UPDATE"))
	require.Equal(t, 1, strings.Count(got, "?"))
	got, err = ApplyOrdered(source, "records r", "r.id", dialect("sqlite"), true)
	require.NoError(t, err)
	require.NotContains(t, got, "FOR UPDATE")
	require.Contains(t, got, "ORDER BY r.id ASC, r.id")
}
func TestPhysicalRowLockGuards(t *testing.T) {
	for _, tc := range []struct {
		name, source, target, dialect string
		active                        bool
	}{
		{"transaction required", "SELECT id FROM records r", "records r", "mysql", false},
		{"unknown dialect", "SELECT id FROM records r", "records r", "other", true},
		{"missing source", "SELECT id FROM other r", "records r", "mysql", true},
		{"ambiguous source", "SELECT a.id FROM (SELECT r.id FROM records r) a JOIN (SELECT r.id FROM records r) b ON b.id=a.id", "records r", "mysql", true},
		{"grouped source", "SELECT count(*) FROM records r GROUP BY r.id", "records r", "mysql", true},
		{"aggregate source", "SELECT count(*) FROM records r", "records r", "mysql", true},
		{"union source", "SELECT id FROM records r UNION SELECT id FROM other", "records r", "mysql", true},
		{"metadata injection", "SELECT id FROM records r", "records r;DROP TABLE x", "mysql", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Apply(tc.source, tc.target, dialect(tc.dialect), tc.active)
			require.Error(t, err)
		})
	}
}
func TestPhysicalRowLockQuotedTextCommentsAndOrdering(t *testing.T) {
	source := `SELECT r.id,'FOR UPDATE' AS text FROM records r WHERE r.id=? -- trailing`
	got, err := ApplyOrdered(source, "records r", "r.id", dialect("mysql"), true)
	require.NoError(t, err)
	require.Contains(t, got, "ORDER BY r.id ASC")
	require.Contains(t, got, "-- trailing\nFOR UPDATE")
	_, err = ApplyOrdered("SELECT id FROM records r", "records r", "other.id", dialect("mysql"), true)
	require.Error(t, err)
}

func TestPhysicalRowLockMainSourceOwnsNestedSameAlias(t *testing.T) {
	source := `SELECT data_rows.id FROM (SELECT t.id FROM records t WHERE EXISTS(SELECT t.id FROM records t WHERE t.id=?)) data_rows`
	got, err := Apply(source, "records t", dialect("mysql"), true)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(got, "FOR UPDATE"))
	require.Contains(t, got, "WHERE t.id=?)\nFOR UPDATE) data_rows")
}
