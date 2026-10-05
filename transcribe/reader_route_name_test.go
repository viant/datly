package transcribe

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
)

const namedReaderBody = `#package('example.com/generated/generated')
#setting($_ = $route('/events', 'GET'))
SELECT events.*, CAST(events.id AS int) FROM (SELECT id FROM events) events`

func TestReaderHeaderNamePreservesComponentAndViewIdentity(t *testing.T) {
	for _, header := range []string{`/* {"Name":"EventList"} */`, `/* {"Name":""} */`, `/* {"Description":"events"} */`, ""} {
		source := &Source{Name: "events", Scope: "example.com/generated/events", Text: header + "\n" + namedReaderBody}
		result, err := NewCompiler().Compile(context.Background(), source)
		require.NoError(t, err)
		require.Equal(t, "events", result.Component.Name)
		require.Equal(t, "events", result.Component.RootView.Name)
		require.Equal(t, "events", result.Component.Key.Name)
		want := "events"
		if header == `/* {"Name":"EventList"} */` {
			want = "EventList"
		}
		require.Equal(t, want, result.Component.Routes[0].Name)
		control, err := NewCompiler().Compile(context.Background(), &Source{Name: source.Name, Scope: source.Scope, Text: namedReaderBody})
		require.NoError(t, err)
		require.Equal(t, control.Component.RootView, result.Component.RootView)
	}
	for _, header := range []string{`/* {"Name":false} */`, `/* {"Name":"Events",} */`, `/* {"Name":"Events","name":"Other"} */`, `/* {"Name":"Events","Type":false} */`} {
		_, err := NewCompiler().Compile(context.Background(), &Source{Name: "events", Text: header + "\n" + namedReaderBody})
		require.Error(t, err)
	}
}

func TestReaderHeaderNameNativeGenerationAndBootstrap(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := &Source{Name: "events", Scope: "example.com/generated/events", Text: `/* {"Name":"EventList"} */` + "\n" + namedReaderBody}
	generated, err := (Generator{Operation: "get"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	require.Equal(t, "EventList", generated.Result.Plan.Routes[0].Name)
	components, err := bootstrap.DiscoverComponentsFromPackages(context.Background(), root, []string{generated.Package.PkgPath}, nil)
	require.NoError(t, err)
	require.Len(t, components, 1)
	component, err := components[0].Resolve(nil, nil)
	require.NoError(t, err)
	require.Equal(t, "events", component.Name)
	require.Equal(t, "EventList", component.Routes[0].Name)
}
