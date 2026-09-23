package generate

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
)

func TestGeneratedRelationHolderTags(t *testing.T) {
	for _, authored := range []string{`json:"-" internal:"true"`, `json:"ExplicitName"`, `json:",omitempty"`, `internal:"true"`, ""} {
		t.Run(authored, func(t *testing.T) {
			child := &spec.View{Name: "PrivateTimeline", Source: &spec.ViewSource{SQL: "SELECT parent_id FROM timeline", Bindings: &spec.ViewBindings{Connector: "warehouse", CacheName: "fast", CacheWarmup: "timelineWarmup"}}}
			relation := &spec.Relation{Name: "PrivateTimeline", Holder: "PrivateTimeline", Tag: authored, View: child, Cardinality: spec.CardinalityMany,
				On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}}
			plan := testPlan(t, &spec.Component{Name: "Parents", Settings: &spec.Settings{CaseFormat: "lc"}, RootView: &spec.View{Name: "Parents", Relations: []*spec.Relation{relation}}})
			field := plan.Views[0].Fields[0]
			require.Equal(t, "[]*PrivateTimelineView", field.Type)
			tags := reflect.StructTag(field.Tag)
			want, exists := reflect.StructTag(authored).Lookup("json")
			if !exists {
				want = "privateTimeline"
			}
			require.Equal(t, want, tags.Get("json"))
			require.Equal(t, reflect.StructTag(authored).Get("internal"), tags.Get("internal"))
			require.NotEmpty(t, tags.Get("on"))
			require.Contains(t, tags.Get("sql"), "SELECT parent_id FROM timeline")
			view, err := tag.ParseView(tags.Get("view"))
			require.NoError(t, err)
			require.Equal(t, "warehouse", view.Connector)
			require.Equal(t, "fast", view.Cache)
			require.Equal(t, "timelineWarmup", view.CacheWarmup)
			require.Equal(t, authored, relation.Tag, "generation must not mutate metadata")
		})
	}
}

func TestRelationHolderTagsCannotReplaceExecutionMetadata(t *testing.T) {
	for _, authored := range []string{`view:"other"`, `sql:"SELECT 1"`, `on:"Other:ID=ID:ID"`} {
		_, err := New(Input{Component: &spec.Component{Name: "Root", RootView: &spec.View{Name: "Root", Relations: []*spec.Relation{{
			Name: "Child", Holder: "Child", Tag: authored, View: &spec.View{Name: "Child"},
		}}}}}).Plan()
		require.ErrorContains(t, err, "conflicts with canonical relation metadata")
	}
}
