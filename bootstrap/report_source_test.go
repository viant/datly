package bootstrap

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

type reportSourceRow struct {
	Country string              `sqlx:"country" groupable:"true"`
	Region  string              `sqlx:"region" groupable:"true"`
	Amount  int                 `sqlx:"amount"`
	Lookup  *reportSourceLookup `on:"Country:v.country=Country:country,Region:v.region=Region:region" sql:"SELECT country, region, label FROM region_lookup"`
}
type reportSourceLookup struct{ Country, Region, Label string }
type reportSourceOutput struct {
	Rows []*reportSourceRow `parameter:",kind=output,in=view"`
}

func TestReportSourceIncludesCompiledRelationsWithoutMutatingArtifact(t *testing.T) {
	on := true
	artifact, err := BuildArtifact(ArtifactInput{
		Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Regions"},
			Routes:   []*spec.Route{{Method: "GET", Path: "/regions"}},
			RootView: &spec.View{Name: "regions", Groupable: &on, Source: &spec.ViewSource{SQL: "SELECT v.country, v.region, SUM(v.amount) AS amount FROM spend v GROUP BY v.country, v.region"}},
		}, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[reportSourceOutput](), DirectViewField: "Rows",
	})
	require.NoError(t, err)
	require.Empty(t, artifact.Component.RootView.Relations)
	snapshot := artifact.ReportSourceComponent()
	require.Len(t, snapshot.RootView.Relations, 1)
	relation := snapshot.RootView.Relations[0]
	require.Equal(t, "Lookup", relation.Holder)
	require.Equal(t, []*spec.RelationLink{
		{ParentNamespace: "v", ParentColumn: "country", ChildColumn: "country"},
		{ParentNamespace: "v", ParentColumn: "region", ChildColumn: "region"},
	}, relation.On)
	relation.On[0].ParentColumn = "changed"
	relation.View.Source.SQL = "changed"
	snapshot.RootView.Source.SQL = "changed"
	again := artifact.ReportSourceComponent()
	require.Equal(t, "country", again.RootView.Relations[0].On[0].ParentColumn)
	require.NotEqual(t, "changed", again.RootView.Relations[0].View.Source.SQL)
	require.NotEqual(t, "changed", again.RootView.Source.SQL)
	require.Empty(t, artifact.Component.RootView.Relations)
}
