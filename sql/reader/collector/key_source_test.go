package collector

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
)

func TestCompileRelationKeySources(t *testing.T) {
	type row struct {
		ID      int `sqlx:"id"`
		Hidden  int `sqlx:"-"`
		Hook    int `sqlx:"-" relationKey:"hook"`
		Invalid int `sqlx:"-" relationKey:"typo"`
		Empty   int `relationKey:""`
	}
	for _, tc := range []struct {
		field  string
		source KeySource
		typed  bool
	}{
		{"ID", KeySourceField, true}, {"Hidden", KeySourceColumn, false},
		{"Absent", KeySourceColumn, false}, {"Hook", KeySourceHook, true},
	} {
		t.Run(tc.field, func(t *testing.T) {
			metadata := data.Links{{Field: tc.field, Column: "id"}}
			links, err := compileLinks(metadata, reflect.TypeFor[*row]())
			require.NoError(t, err)
			require.Equal(t, tc.source, links[0].KeySource)
			require.Equal(t, tc.typed, links[0].XField != nil)
			require.Equal(t, tc.field, metadata[0].Field)
		})
	}
	for _, name := range []string{"Invalid", "Empty"} {
		_, err := compileLinks(data.Links{{Field: name}}, reflect.TypeFor[row]())
		require.ErrorContains(t, err, "invalid relationKey")
	}
}

func TestRelationIndexIdentityUsesEffectiveSource(t *testing.T) {
	type row struct {
		Scanned     int `sqlx:"scanned"`
		Hidden      int `sqlx:"-"`
		OtherHidden int `sqlx:"-"`
		Hook        int `sqlx:"-" relationKey:"hook"`
		OtherHook   int `sqlx:"-" relationKey:"hook"`
	}
	link := func(field string) *Link {
		links, err := compileLinks(data.Links{{Namespace: "parents", Column: "key", Field: field}}, reflect.TypeFor[row]())
		require.NoError(t, err)
		return links[0]
	}
	for _, tc := range []struct {
		left, right string
		same        bool
	}{
		{"Hook", "Hook", true},
		{"Hidden", "OtherHidden", true},
		{"Hidden", "Absent", true},
		{"Hook", "OtherHook", false},
		{"Hook", "Hidden", false},
		{"Hook", "Scanned", false},
		{"Hidden", "Scanned", false},
	} {
		t.Run(tc.left+"/"+tc.right, func(t *testing.T) {
			left, right := link(tc.left), link(tc.right)
			require.Equal(t, tc.same, relationIndexIdentity(left) == relationIndexIdentity(right))
			require.Equal(t, tc.same, relationCompositeSignature(Links{left}) == relationCompositeSignature(Links{right}))
		})
	}
}
