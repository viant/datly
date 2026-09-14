package reader

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/reader/collector"
	structjson "github.com/viant/structology/encoding/json"
	xstate "github.com/viant/xdatly/state"
)

func TestOutputSelectionSharedViewFollowsEveryRelationPath(t *testing.T) {
	type shared struct {
		ID   int    `sqlx:"id" json:"id"`
		Name string `sqlx:"name" json:"label"`
	}
	type root struct {
		First  []shared `json:"a"`
		Second []shared `json:"b"`
	}
	rowType := reflect.TypeFor[shared]()
	child := &ViewPlan{View: &data.View{Spec: spec.View{Name: "shared"}}, Collector: &collector.View{Schema: collector.NewSchema(rowType)}}
	plan := &Plan{DirectOutput: true, Root: &ViewPlan{View: &data.View{Spec: spec.View{Name: "root"}}, Relations: []*RelationPlan{
		{Relation: &data.Relation{Holder: "First"}, Target: child},
		{Relation: &data.Relation{Holder: "Second"}, Target: child},
	}}}
	session := &Session{Artifact: plan, OutputType: reflect.TypeFor[[]root]()}
	filter, err := session.selectedOutput(invocationSelectors{child.View: &xstate.Selector{Fields: []string{"name"}}})
	require.NoError(t, err)
	value := []root{{First: []shared{{ID: 1, Name: "first"}}, Second: []shared{{ID: 2, Name: "second"}}}}
	got, err := structjson.MarshalStandard(value, structjson.WithPathFieldExcluder(filter))
	require.NoError(t, err)
	require.JSONEq(t, `[{"a":[{"label":"first"}],"b":[{"label":"second"}]}]`, string(got))
}
