package compile

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestReaderRowLockCapabilityIsMetadataOnly(t *testing.T) {
	view, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records"}, SQL: `SELECT r.id, row_lock(r,'records r','r.id') FROM records r`})
	require.NoError(t, err)
	require.Equal(t, "records r", view.RowLock)
	require.Equal(t, "r.id", view.RowLockOrder)
	require.NotContains(t, strings.ToLower(view.Source.SQL), "row_lock")
	require.NotContains(t, view.Source.SQL, "FOR UPDATE")
	for _, c := range view.Columns {
		require.NotEqual(t, "row_lock", strings.ToLower(c.Name))
	}
}
func TestReaderRowLockCapabilityRejectsInvalidMetadata(t *testing.T) {
	for _, source := range []string{`SELECT r.id,row_lock(missing,'records r') FROM records r`, `SELECT r.id,row_lock(r,'records r;drop table x') FROM records r`, `SELECT r.id,row_lock(r,'records r','other.id') FROM records r`, `SELECT r.id,row_lock(r,'records r'),row_lock(r,'records r') FROM records r`, `SELECT r.id,row_lock(r,'records r') AS leaked FROM records r`} {
		_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records"}, SQL: source})
		require.Error(t, err, source)
	}
}
