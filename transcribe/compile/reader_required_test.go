package compile

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestReaderRequiredColumnDeclaration(t *testing.T) {
	for _, annotations := range []string{
		"required(r.value)",
		"required(r.value),required(r.value)",
		"required(r.value),cast(r.value AS *string)",
		"cast(r.value AS *string),required(r.value)",
	} {
		t.Run(annotations, func(t *testing.T) {
			view := &spec.View{Name: "Records", Source: &spec.ViewSource{}, Columns: []*spec.Column{{Name: "value", Source: "value", Nullable: true, Type: spec.TypeRef{Name: "string"}}}}
			before, err := json.Marshal(view)
			require.NoError(t, err)
			got, err := NewReader().Compile(ReadInput{View: view, SQL: "SELECT r.value," + annotations + " FROM records r WHERE r.id = ? ORDER BY r.value LIMIT 3"})
			require.NoError(t, err)
			require.Len(t, got.Columns, 1)
			column := got.Columns[0]
			require.True(t, column.Required)
			require.False(t, column.Nullable)
			require.False(t, column.NotNull, "required is not a database constraint")
			require.Equal(t, strings.Contains(annotations, "cast("), column.EffectiveType().Pointer)
			require.NotContains(t, strings.ToLower(got.Source.SQL), "required(")
			require.Contains(t, got.Source.SQL, "r.id = ?")
			require.Contains(t, got.Source.SQL, "ORDER BY r.value")
			require.Contains(t, got.Source.SQL, "LIMIT 3")
			after, err := json.Marshal(view)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after), "compilation must not mutate input metadata")
		})
	}
}

func TestReaderRequiredChildColumn(t *testing.T) {
	got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Parents", Source: &spec.ViewSource{}}, SQL: `SELECT p.*, child.*, required(child.value)
FROM (SELECT id FROM parents) p
LEFT JOIN (SELECT parent_id,value FROM children) child ON p.id=child.parent_id`})
	require.NoError(t, err)
	require.Len(t, got.Relations, 1)
	child := got.Relations[0].View
	found := false
	for _, column := range child.Columns {
		if column.Name == "value" {
			found = true
			require.True(t, column.Required)
			require.False(t, column.Nullable)
		}
	}
	require.True(t, found)
	require.NotContains(t, strings.ToLower(got.Source.SQL+child.Source.SQL), "required(")
}

func TestReaderRequiredRejectsInvalidDeclarations(t *testing.T) {
	for _, SQL := range []string{
		"SELECT r.value, required() FROM records r",
		"SELECT r.value, required(r.value, true) FROM records r",
		"SELECT r.value, required(value) FROM records r",
		"SELECT r.value, required(other.value) FROM records r",
		"SELECT r.value, required(r.missing) FROM records r",
		"SELECT required(r.value) FROM records r",
		"SELECT r.value, required(r.value) AS renamed FROM records r",
		"SELECT COALESCE(required(r.value), 0) FROM records r",
		"SELECT r.value FROM records r WHERE required(r.value)",
	} {
		t.Run(SQL, func(t *testing.T) {
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records", Source: &spec.ViewSource{}}, SQL: SQL})
			require.Error(t, err)
		})
	}
}
