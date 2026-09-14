package bootstrap

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/builder"
	"reflect"
	"testing"
)

func TestInferredColumnsDoNotAuthorizeSelectorAliases(t *testing.T) {
	type row struct {
		BID int `sqlx:"b_id"`
	}
	type output struct{ Rows []*row }
	for _, authored := range []bool{false, true} {
		component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/users"}}, RootView: &spec.View{Name: "users", Source: &spec.ViewSource{SQL: "SELECT b_id FROM users"}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
		if authored {
			component.RootView.Columns = []*spec.Column{{Name: "bid", Source: "b_id"}}
		}
		artifact, err := BuildArtifact(ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), DirectViewField: "Rows"})
		require.NoError(t, err)
		view := artifact.Reader.Root.View
		require.Equal(t, !authored, view.Spec.Columns[0].NameInferred)
		p := dsql.SelectorProjection{SQL: view.Spec.Source.SQL, View: view}
		_, err = p.Columns([]string{"bid"})
		if authored {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
		// Table generation must not turn an inferred Go spelling into emitted SQL.
		view.Spec.Source = &spec.ViewSource{Table: "users"}
		q, err := builder.NewBuilder().Build(context.Background(), builder.WithBuilderView(view))
		require.NoError(t, err)
		if authored {
			require.Contains(t, q.SQL, "b_id AS bid")
		} else {
			require.Equal(t, "SELECT b_id FROM users", q.SQL)
		}
	}
}
