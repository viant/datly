package compile

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestReaderOptionalColumnDeclaration(t *testing.T) {
	for _, annotation := range []string{"optional(r.value)", "optional(r.value),optional(r.value)", "optional(r.value),CAST(r.value AS int64)", "CAST(r.value AS int64),optional(r.value)"} {
		t.Run(annotation, func(t *testing.T) {
			view := &spec.View{Name: "Records", Source: &spec.ViewSource{}, Columns: []*spec.Column{{Name: "value", Source: "value", NotNull: true, Type: spec.TypeRef{Name: "int"}}}}
			before, err := json.Marshal(view)
			require.NoError(t, err)
			got, err := NewReader().Compile(ReadInput{View: view, SQL: "SELECT r.value," + annotation + " FROM records r WHERE r.id=? ORDER BY r.value"})
			require.NoError(t, err)
			require.Len(t, got.Columns, 1)
			column := got.Columns[0]
			require.True(t, column.Optional)
			require.False(t, column.Required)
			require.True(t, column.EffectiveType().Pointer)
			require.True(t, column.NotNull, "Go pointer shape must not change physical constraints")
			require.NotContains(t, got.Source.SQL, "optional(")
			require.Contains(t, got.Source.SQL, "WHERE r.id = ?")
			after, err := json.Marshal(view)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after))
		})
	}
}

func TestReaderOptionalRejectsInvalidOrConflictingDeclarations(t *testing.T) {
	for _, sql := range []string{
		"SELECT r.value,optional() FROM records r",
		"SELECT r.value,optional(value) FROM records r",
		"SELECT r.value,optional(r.value,true) FROM records r",
		"SELECT r.value,optional(r.missing) FROM records r",
		"SELECT r.value,optional(r.value) AS renamed FROM records r",
		"SELECT r.value FROM records r WHERE optional(r.value)",
		"SELECT r.value,optional(r.value),required(r.value) FROM records r",
		"SELECT r.value,required(r.value),optional(r.value) FROM records r",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records", Source: &spec.ViewSource{}}, SQL: sql})
			require.Error(t, err)
		})
	}
}

func TestReaderRelatedTypeHolderUsesIndependentSQLNamespace(t *testing.T) {
	got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Parents", Source: &spec.ViewSource{}}, SQL: "SELECT p.*,u.*,type(u,'UsageView','Usage') FROM parents p LEFT JOIN usage_totals u ON p.id=u.parent_id AND 1=1"})
	require.NoError(t, err)
	require.Len(t, got.Relations, 1)
	r := got.Relations[0]
	require.Equal(t, "Usage", r.Holder)
	require.Equal(t, "u", r.Name)
	require.Equal(t, "u", r.View.Namespace)
	require.Equal(t, "UsageView", r.View.TypeName)
	require.NotContains(t, got.Source.SQL, "type(")
	for _, sql := range []string{
		"SELECT p.*,type(p,'Parent','RootField') FROM parents p",
		"SELECT p.*,u.*,type(u,'UsageView','invalid.field') FROM parents p LEFT JOIN usage_totals u ON p.id=u.parent_id",
		"SELECT p.*,u.*,type(u,'UsageView','func') FROM parents p LEFT JOIN usage_totals u ON p.id=u.parent_id",
	} {
		_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Parents", Source: &spec.ViewSource{}}, SQL: sql})
		require.Error(t, err)
	}
}
