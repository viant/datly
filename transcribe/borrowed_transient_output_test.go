package transcribe

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
)

func TestBorrowedPhysicalProjectionUsesTransientOutputIdentity(t *testing.T) {
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE flights(id INTEGER PRIMARY KEY,status INTEGER)"))
	original := &spec.View{Name: "Flights", Source: &spec.ViewSource{Table: "flights", SQL: "SELECT f.*,f.STATUS AS PersistedStatus,f.DerivedStatus AS Status FROM flights f"}, Columns: []*spec.Column{
		{Name: "ID", Source: "id", PrimaryKey: true, Type: spec.TypeRef{Name: "int"}},
		{Name: "STATUS", Source: "status", Output: "PersistedStatus", Type: spec.TypeRef{Name: "int"}},
		{Name: "DerivedStatus", Source: "DerivedStatus", Output: " Status ", Type: spec.TypeRef{Name: "int"}, Tag: `sqlx:"-"`},
	}}
	before, err := json.Marshal(original)
	require.NoError(t, err)
	physical := original.Clone()
	require.NoError(t, borrowedPhysicalProjection(physical))
	require.NotContains(t, physical.Source.SQL, "DerivedStatus")
	require.Contains(t, physical.Source.SQL, "*")
	require.Contains(t, physical.Source.SQL, "PersistedStatus")
	require.Len(t, physical.Columns, 2)
	facts, err := column.New(column.Connections{"main": db.DB}).PhysicalSourceConstraints(t.Context(), "main", physical)
	require.NoError(t, err)
	found := false
	for _, mapping := range facts.Projection {
		if mapping.Result == "PersistedStatus" {
			require.Equal(t, "status", mapping.Physical)
			found = true
		}
	}
	require.True(t, found, "physical status mapping disappeared")
	after, err := json.Marshal(original)
	require.NoError(t, err)
	require.Equal(t, before, after, "detached analysis changed full row contract or SQL")
}
