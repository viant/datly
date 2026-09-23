package compile

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestReaderRelationHolderTags(t *testing.T) {
	for _, target := range []string{"privateTimeline", "PrivateTimeline", "`privateTimeline`"} {
		t.Run(target, func(t *testing.T) {
			view, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Parents"}, SQL: `SELECT p.*, privateTimeline.*, nested.*,
tag(` + target + `,'json:"-"'), tag(privateTimeline,'internal:"true"'), tag(privateTimeline,'json:"-"'),
tag(nested,'json:"nestedValues"'), tag(p.id,'json:"id"')
FROM parents p
JOIN (SELECT parent_id, id FROM timeline) privateTimeline ON p.id = privateTimeline.parent_id
JOIN (SELECT owner_id FROM details) nested ON privateTimeline.id = nested.owner_id`})
			require.NoError(t, err)
			require.Len(t, view.Relations, 1)
			relation := view.Relations[0]
			require.Equal(t, "-", reflect.StructTag(relation.Tag).Get("json"))
			require.Equal(t, "true", reflect.StructTag(relation.Tag).Get("internal"))
			require.Equal(t, `json:"nestedValues"`, relation.View.Relations[0].Tag)
			require.Equal(t, `json:"id"`, view.Columns[0].Tag)
			require.NotContains(t, view.Source.SQL, "tag(")
			require.NotContains(t, relation.View.Source.SQL, "tag(")
			require.NotEmpty(t, relation.On)
			require.NotEmpty(t, relation.View.Source.SQL)
			cloned := view.Clone()
			require.Equal(t, relation.Tag, cloned.Relations[0].Tag)
			cloned.Relations[0].Tag = `json:"changed"`
			require.NotEqual(t, relation.Tag, cloned.Relations[0].Tag)
			encoded, err := json.Marshal(view)
			require.NoError(t, err)
			var restored spec.View
			require.NoError(t, json.Unmarshal(encoded, &restored))
			require.Equal(t, relation.Tag, restored.Relations[0].Tag)
		})
	}
}

func TestReaderRelationHolderTagValidation(t *testing.T) {
	for _, tc := range []struct{ declaration, want string }{
		{`tag(missing,'json:"-"')`, "no canonical relation"},
		{`tag(p,'json:"-"')`, "no canonical relation"},
		{`tag(privateTimeline,'json:"-" trailing')`, "tag privateTimeline"},
		{`tag(privateTimeline,' ')`, "non-empty string literal"},
		{`tag(privateTimeline,'json:"-"'),tag(privateTimeline,'json:"exposed"')`, "conflicting json"},
		{`cast(privateTimeline AS string)`, "qualified view column"},
	} {
		t.Run(tc.declaration, func(t *testing.T) {
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Parents"}, SQL: `SELECT p.*,privateTimeline.*,` + tc.declaration + ` FROM parents p JOIN timeline privateTimeline ON p.id=privateTimeline.parent_id`})
			require.ErrorContains(t, err, tc.want)
		})
	}
	root := &spec.View{Relations: []*spec.Relation{
		{Name: "first", Holder: "Shared", View: &spec.View{Name: "First"}},
		{Name: "second", Holder: "Shared", View: &spec.View{Name: "Second"}},
	}}
	_, err := relationTagTarget(root, "shared")
	require.ErrorContains(t, err, "multiple canonical relations")
}
