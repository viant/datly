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

func TestSelectorAliasAuthorizesOutputFieldProjection(t *testing.T) {
	type row struct {
		AdvertiserID int     `sqlx:"advertiser_id" selectorAlias:"advertiserId"`
		Spend        float64 `sqlx:"spend"`
	}
	type output struct {
		Rows []*row
	}
	component := &spec.Component{
		Routes:   []*spec.Route{{Method: "GET", Path: "/metrics"}},
		RootView: &spec.View{Name: "metrics", Source: &spec.ViewSource{SQL: "SELECT advertiser_id, spend FROM metrics"}},
		Parameters: []*spec.Parameter{
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}

	artifact, err := BuildArtifact(ArtifactInput{
		Component:       component,
		InputType:       reflect.TypeFor[struct{}](),
		OutputType:      reflect.TypeFor[output](),
		DirectViewField: "Rows",
	})
	require.NoError(t, err)

	view := artifact.Reader.Root.View
	require.Len(t, view.Spec.Columns, 2)
	require.Equal(t, "advertiserId", view.Spec.Columns[0].Name)
	require.Equal(t, "advertiser_id", view.Spec.Columns[0].Source)
	require.False(t, view.Spec.Columns[0].NameInferred)
	require.Equal(t, "spend", view.Spec.Columns[1].Source)
	require.True(t, view.Spec.Columns[1].NameInferred)

	projection := dsql.SelectorProjection{SQL: view.Spec.Source.SQL, View: view}
	_, err = projection.Columns([]string{"advertiserId"})
	require.NoError(t, err)
	_, err = projection.Columns([]string{"advertiser_id"})
	require.NoError(t, err)
}

func TestSelectorAliasRejectsUnresolvedOrAmbiguousSource(t *testing.T) {
	type row struct {
		AdvertiserID int     `sqlx:"advertiser_id" selectorAlias:"advertiserId"`
		Spend        float64 `sqlx:"spend"`
	}
	type output struct {
		Rows []*row
	}
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "unknown source",
			sql:  "SELECT spend FROM metrics",
			want: `not found column advertiserid`,
		},
		{
			name: "ambiguous source",
			sql:  "SELECT a.advertiser_id, b.advertiser_id, spend FROM metrics a JOIN metrics b ON a.advertiser_id = b.advertiser_id",
			want: `duplicate output column "advertiser_id"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{
				Routes:   []*spec.Route{{Method: "GET", Path: "/metrics"}},
				RootView: &spec.View{Name: "metrics", Source: &spec.ViewSource{SQL: tc.sql}},
				Parameters: []*spec.Parameter{
					{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
				},
			}

			artifact, err := BuildArtifact(ArtifactInput{
				Component:       component,
				InputType:       reflect.TypeFor[struct{}](),
				OutputType:      reflect.TypeFor[output](),
				DirectViewField: "Rows",
			})
			require.NoError(t, err)
			_, err = (dsql.SelectorProjection{SQL: artifact.Reader.Root.View.Spec.Source.SQL, View: artifact.Reader.Root.View}).Columns([]string{"advertiserId"})
			require.ErrorContains(t, err, tc.want)
		})
	}
}
