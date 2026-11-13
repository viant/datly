package bootstrap

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

func TestCompiledReaderResolvesNamedHostCache(t *testing.T) {
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Rows []*row }
	for _, tc := range []struct {
		name     string
		settings *spec.CacheSettings
		wantErr  string
	}{
		{name: "valid", settings: &spec.CacheSettings{Enabled: true, Provider: "afs", Location: t.TempDir(), TTL: "1m"}},
		{name: "missing", wantErr: "location is required"},
		{name: "disabled", settings: &spec.CacheSettings{Location: t.TempDir(), TTL: "1m"}, wantErr: "no enabled configuration"},
		{name: "missing location", settings: &spec.CacheSettings{Enabled: true, TTL: "1m"}, wantErr: "location is required"},
		{name: "invalid TTL", settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "bad"}, wantErr: "TTL must be a positive duration"},
		{name: "invalid provider", settings: &spec.CacheSettings{Enabled: true, Provider: "unknown", Location: t.TempDir(), TTL: "1m"}, wantErr: "provider is not supported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			zero := 0
			component := &spec.Component{
				Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/cache", Name: "Records"},
				Routes:   []*spec.Route{{Method: "GET", Path: "/records"}},
				Settings: &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "shared", Warmup: &spec.CacheWarmupSettings{IndexColumn: "id", Limit: &zero}}},
				RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT 1 AS id"}},
			}
			artifact, err := BuildArtifact(ArtifactInput{
				Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), DirectViewField: "Rows",
			})
			require.NoError(t, err)
			config := ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}}
			if tc.settings != nil {
				config.CacheSettings = map[string]*spec.CacheSettings{"shared": tc.settings}
			}
			execution, err := artifact.ReaderCompilation().NewExecution(config)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, execution)
			} else {
				require.NoError(t, err)
				require.NotNil(t, execution)
			}
			// Host settings remain runtime dependencies, not authored metadata.
			for _, settings := range []*spec.CacheSettings{component.Settings.Cache, artifact.Component.Settings.Cache} {
				require.Equal(t, "shared", settings.Name)
				require.Empty(t, settings.Provider)
				require.Empty(t, settings.Location)
				require.Empty(t, settings.TTL)
				require.NotNil(t, settings.Warmup.Limit)
				require.Zero(t, *settings.Warmup.Limit)
			}
		})
	}
}
