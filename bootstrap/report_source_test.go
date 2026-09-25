package bootstrap

import (
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
)

type reportSourceRow struct {
	Country string              `sqlx:"country" groupable:"true"`
	Region  string              `sqlx:"region" groupable:"true"`
	Amount  int                 `sqlx:"amount"`
	Lookup  *reportSourceLookup `on:"Country:v.country=Country:country,Region:v.region=Region:region" sql:"SELECT country, region, label FROM region_lookup"`
}

func TestHandlerReportSourceIsMetadataOnly(t *testing.T) {
	on := true
	resources := resource.New()
	require.NoError(t, resources.Register("report", fstest.MapFS{
		"root.sql": &fstest.MapFile{Data: []byte("SELECT v.country, v.region, SUM(v.amount) AS amount FROM spend v GROUP BY v.country, v.region")},
	}))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Regions"},
		Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}},
		Routes:   []*spec.Route{{Method: "GET", Path: "/regions", Handler: "NewRegions"}},
		RootView: &spec.View{Name: "regions", Groupable: &on, Source: &spec.ViewSource{URI: "report:root.sql"}},
	}
	input := ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[reportSourceOutput](), DirectViewField: "Rows", Resources: resources}
	reader, err := BuildArtifact(input)
	require.NoError(t, err)
	input.HandlerOwnedOutput = true
	owned, err := BuildArtifact(input)
	require.NoError(t, err)
	require.Nil(t, owned.Reader)
	require.Nil(t, owned.ReaderCompilation())
	require.Empty(t, owned.Component.RootView.Relations)
	require.Empty(t, owned.Component.RootView.Source.SQL)
	require.Empty(t, owned.Component.Parameters, "output parameters must not enter the handler's execution contract")
	got := owned.ReportSourceComponent()
	require.Equal(t, reader.ReportSourceComponent().RootView.Relations, got.RootView.Relations)
	require.Equal(t, reader.ReportSourceComponent().RootView.Source, got.RootView.Source)
	got.RootView.Relations[0].On[0].ParentColumn = "changed"
	got.RootView.Source.SQL = "changed"
	again := owned.ReportSourceComponent()
	require.Equal(t, "country", again.RootView.Relations[0].On[0].ParentColumn)
	require.NotEqual(t, "changed", again.RootView.Source.SQL)

	// Only report-enabled handlers require report SQL resources.
	input.Resources = nil
	_, err = BuildArtifact(input)
	require.ErrorContains(t, err, "compile handler report metadata")
	component.Settings.Report.Enabled = false
	plain, err := BuildArtifact(input)
	require.NoError(t, err)
	require.Nil(t, plain.Reader)
	require.Empty(t, plain.ReportSourceComponent().RootView.Relations)
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
