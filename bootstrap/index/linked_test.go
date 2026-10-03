package index

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
)

func TestBuildLinkedCarriesNestedOutputWarmup(t *testing.T) {
	const scope = "example.com/performance"
	components := []*spec.Component{
		{Key: spec.Key{Kind: spec.KindComponent, Scope: scope, Name: "advertiser_performance"}, RootView: &spec.View{Name: "advertiserPerformance"}},
		{Key: spec.Key{Kind: spec.KindComponent, Scope: scope, Name: "Plain"}},
		{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/other", Name: "advertiser_performance"}},
		{Key: spec.Key{Kind: spec.KindComponent, Scope: scope, Name: "Fallback"}},
	}
	routes := []*bootstrap.RouteSource{
		{PackagePath: scope, FieldName: "Reader", Tag: tag.Component{Name: "advertiser_performance"}, LinkedOutputType: reflect.TypeFor[indexedChildWarmupOutput]()},
		{PackagePath: scope, FieldName: "Plain", LinkedOutputType: reflect.TypeFor[struct{ ID int }]()},
		{PackagePath: scope, FieldName: "Fallback", LinkedOutputType: reflect.TypeFor[*indexedChildWarmupOutput]()},
		nil,
	}
	snapshot, err := BuildLinked([]string{scope}, components, routes...)
	require.NoError(t, err)
	entries := snapshot.Entries()
	require.Len(t, entries, 4)
	for _, entry := range entries {
		key := entry.Key()
		want := key.Scope == scope && key.Name != "Plain"
		require.Equal(t, want, entry.Warmup, key.String())
		if key.Name == "advertiser_performance" && key.Scope == scope {
			require.Empty(t, entry.Component.RootView.Relations)
			require.Nil(t, entry.Component.CacheWarmup())
		}
	}
	require.Empty(t, components[0].RootView.Relations, "indexing must not materialize the reader")
	require.Nil(t, components[0].CacheWarmup())
	// Existing metadata-only callers remain supported without reflected routes.
	_, err = BuildLinked([]string{scope}, components)
	require.NoError(t, err)
}

func TestBuildLinkedRejectsInvalidNestedWarmupTags(t *testing.T) {
	type output struct {
		Rows []struct {
			Children []struct{ ID int } `view:"children,limit=invalid,cacheWarmup=warm"`
		}
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/api", Name: "Read"}}
	snapshot, err := BuildLinked([]string{component.Key.Scope}, []*spec.Component{component}, &bootstrap.RouteSource{
		PackagePath: component.Key.Scope, FieldName: "Read", LinkedOutputType: reflect.TypeFor[output](),
	})
	require.Nil(t, snapshot)
	require.ErrorContains(t, err, "index component example.com/api.Read output warmup tags")
}

func TestBuildLinkedRetainsMetadataWarmupWithoutReflectedRoutes(t *testing.T) {
	for _, component := range []*spec.Component{
		{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/api", Name: "Root"}, Settings: &spec.Settings{Cache: &spec.CacheSettings{Warmup: &spec.CacheWarmupSettings{}}}},
		{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/api", Name: "Delegated"}, Settings: &spec.Settings{WarmupTarget: &spec.RouteRef{Method: "GET", Path: "/private"}}},
	} {
		snapshot, err := BuildLinked([]string{component.Key.Scope}, []*spec.Component{component})
		require.NoError(t, err)
		require.Len(t, snapshot.Entries(), 1)
		require.True(t, snapshot.Entries()[0].Warmup, component.Key.String())
	}
}
