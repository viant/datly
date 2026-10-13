package translator

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
)

func TestRelationLinks_UsesAllRelationPairs(t *testing.T) {
	relation := testCompositeRelation()

	relLinks, refLinks := relationLinks(relation)
	require.Len(t, relLinks, 2)
	require.Len(t, refLinks, 2)

	require.Equal(t, "FeatureType", relLinks[0].Field)
	require.Equal(t, "FEATURE_TYPE", relLinks[0].Column)
	require.Equal(t, "FeatureValue", relLinks[1].Field)
	require.Equal(t, "FEATURE_VALUE", relLinks[1].Column)

	require.Equal(t, "FeatureType", refLinks[0].Field)
	require.Equal(t, "FeatureType", refLinks[0].Column)
	require.Equal(t, "Value", refLinks[1].Field)
	require.Equal(t, "Value", refLinks[1].Column)
}

type translatorParentRecord struct {
	FeatureType       string
	FeatureValue      string
	SignalPerformance *translatorChildRecord
}

type translatorChildRecord struct {
	FeatureType string
	Value       string
}

func testCompositeRelation() *inference.Relation {
	return &inference.Relation{
		Name:        "signalPerformance",
		Cardinality: state.One,
		Pairs: []*inference.RelationPair{
			{
				ParentField: &inference.Field{
					Field:  view.Field{Name: "FeatureType", Schema: state.NewSchema(reflect.TypeOf(""))},
					Column: &sqlparser.Column{Name: "FEATURE_TYPE"},
				},
				KeyField: &inference.Field{
					Field:  view.Field{Name: "FeatureType", Schema: state.NewSchema(reflect.TypeOf(""))},
					Column: &sqlparser.Column{Name: "FeatureType"},
				},
			},
			{
				ParentField: &inference.Field{
					Field:  view.Field{Name: "FeatureValue", Schema: state.NewSchema(reflect.TypeOf(""))},
					Column: &sqlparser.Column{Name: "FEATURE_VALUE"},
				},
				KeyField: &inference.Field{
					Field:  view.Field{Name: "Value", Schema: state.NewSchema(reflect.TypeOf(""))},
					Column: &sqlparser.Column{Name: "Value"},
				},
			},
		},
		ParentField: &inference.Field{
			Field:  view.Field{Name: "FeatureType", Schema: state.NewSchema(reflect.TypeOf(""))},
			Column: &sqlparser.Column{Name: "FEATURE_TYPE"},
		},
		KeyField: &inference.Field{
			Field:  view.Field{Name: "FeatureType", Schema: state.NewSchema(reflect.TypeOf(""))},
			Column: &sqlparser.Column{Name: "FeatureType"},
		},
		Spec: &inference.Spec{
			Namespace: "signalPerformance",
			Type:      &inference.Type{Name: "SignalPerformanceView"},
		},
	}
}

func TestRelationLinks_PreservesCompositeJoinPairs_Init(t *testing.T) {
	// Ensure the translator-emitted links initialize into a real datly relation object
	// with composite join semantics intact.
	parent := view.NewView("audience", "",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithViewType(reflect.TypeOf(&translatorParentRecord{})),
		view.WithTemplate(&view.Template{}),
	)
	child := view.NewView("signalPerformance", "viant-mediator.forecaster.signal_performance",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithViewType(reflect.TypeOf(&translatorChildRecord{})),
		view.WithTemplate(&view.Template{}),
	)
	relLinks, refLinks := relationLinks(testCompositeRelation())
	require.NoError(t, view.WithOneToOne("SignalPerformance", relLinks, view.NewReferenceView(refLinks, child))(parent))
	require.NoError(t, parent.Init(context.Background(), view.EmptyResource()))
	require.Len(t, parent.With, 1)
	require.Len(t, parent.With[0].On, 2)
	require.Len(t, parent.With[0].Of.On, 2)
	require.True(t, parent.With[0].IsComposite())
}
