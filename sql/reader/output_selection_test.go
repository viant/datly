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

func TestOutputSelectionNormalizesQuotedSQLSelectorNames(t *testing.T) {
	type row struct {
		Category     string `sqlx:"category" json:"category"`
		FeatureCount int    `sqlx:"feature_count" json:"feature_count"`
		Ignored      string `sqlx:"ignored" json:"ignored"`
	}
	plan := &Plan{
		DirectOutput: true,
		Root: &ViewPlan{
			View:      &data.View{Spec: spec.View{Name: "selector_features"}},
			Collector: &collector.View{Schema: collector.NewSchema(reflect.TypeFor[row]())},
		},
	}
	session := &Session{Artifact: plan, OutputType: reflect.TypeFor[[]row]()}
	filter, err := session.selectedOutput(invocationSelectors{plan.Root.View: &xstate.Selector{Fields: []string{"`category`", "`feature_count`"}}})
	require.NoError(t, err)
	value := []row{{Category: "C", FeatureCount: 3, Ignored: "hidden"}}
	got, err := structjson.MarshalStandard(value, structjson.WithPathFieldExcluder(filter))
	require.NoError(t, err)
	require.JSONEq(t, `[{"category":"C","feature_count":3}]`, string(got))
}

func TestOutputSelectionRejectsUnmatchedRequestedScalarField(t *testing.T) {
	type row struct {
		Category string `sqlx:"category" json:"category"`
	}
	plan := &Plan{
		DirectOutput: true,
		Root: &ViewPlan{
			View:      &data.View{Spec: spec.View{Name: "selector_features"}},
			Collector: &collector.View{Schema: collector.NewSchema(reflect.TypeFor[row]())},
		},
	}
	session := &Session{Artifact: plan, OutputType: reflect.TypeFor[[]row]()}
	_, err := session.selectedOutput(invocationSelectors{plan.Root.View: &xstate.Selector{Fields: []string{"missing"}}})
	require.ErrorContains(t, err, "matched 0 of 1")
}
