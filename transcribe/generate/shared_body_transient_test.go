package generate

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestSharedBodyTransientPhysicalOwnership(t *testing.T) {
	owner := &spec.View{Columns: []*spec.Column{{Name: "HiddenAlias", Source: "SECRET", Tag: `sqlx:"-" json:"-" diff:"-" internal:"true"`}, {Name: "Visible", Source: "TARGET"}, {Name: "NoProvenance", Tag: `sqlx:"-"`}, {Name: "Expression", Source: "LOGIC", Expression: "NULL", Tag: `sqlx:"-"`}}}
	got, e := sharedBodyTransientColumns(owner)
	require.NoError(t, e)
	require.True(t, got["secret"])
	require.False(t, got["target"])
	require.False(t, got["noprovenance"])
	require.False(t, got["logic"])
	owner.Columns = append(owner.Columns, &spec.Column{Name: "CompetingAlias", Source: "SECRET"})
	_, e = sharedBodyTransientColumns(owner)
	require.ErrorContains(t, e, "ambiguous body physical source")
}
