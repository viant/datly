package compiler

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestDerivedViewConnectorInheritance(t *testing.T) {
	type leaf struct {
		ID int `sqlx:"id"`
	}
	type child struct {
		ID   int   `sqlx:"id"`
		Leaf *leaf `view:"leaf" sql:"SELECT id FROM leaf" on:"ID:id=ID:id"`
	}
	type root struct {
		ID        int    `sqlx:"id"`
		Inherited *child `view:"inherited" sql:"SELECT id FROM inherited" on:"ID:id=ID:id"`
		Explicit  *child `view:"explicit,connector=ci_ads" sql:"SELECT id FROM explicit" on:"ID:id=ID:id"`
	}
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "bq_metrics"}, RootView: &spec.View{Name: "root", Source: &spec.ViewSource{SQL: "SELECT id FROM root"}}}
	views, err := buildDirectDataViews(component, reflect.TypeFor[[]root]())
	require.NoError(t, err)
	require.Equal(t, "bq_metrics", views.root.Connector)
	require.Len(t, views.root.Relations, 2)
	for i, want := range []string{"bq_metrics", "ci_ads"} {
		child := views.root.Relations[i].Of.View
		require.Equal(t, want, child.Connector)
		require.Equal(t, want, child.Relations[0].Of.View.Connector)
	}
	require.Empty(t, component.RootView.Relations, "authored spec must not be mutated")
}

func TestSharedViewConnectorInheritance(t *testing.T) {
	edge := func(v *data.View) *data.Relation { return &data.Relation{Of: &data.RelationRef{View: v}} }
	for _, explicit := range []string{"", "shared"} {
		shared := &data.View{Connector: explicit}
		root := &data.View{Connector: "a", Relations: []*data.Relation{edge(shared), edge(&data.View{Connector: "b", Relations: []*data.Relation{edge(shared)}})}}
		err := inheritViewConnectors(root)
		if explicit == "" {
			require.ErrorContains(t, err, "conflicting connectors")
		} else {
			require.NoError(t, err)
			require.Equal(t, explicit, shared.Connector)
		}
	}
}
