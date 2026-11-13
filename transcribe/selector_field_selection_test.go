package transcribe

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/generate"
	"testing"
)

func TestGeneratedSelectorFieldSelectionRoundTrip(t *testing.T) {
	for _, mode := range []string{"query", "outputOnly", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			compiled, err := NewCompiler().Compile(context.Background(), &Source{Name: "Records", Scope: "example.com/records", Text: fmt.Sprintf(`#setting($_ = $route('/records','GET'))
#set($_ = $Fields<[]string>(query/fields).Optional().QuerySelector('r'))
#set($_ = $Data<?>(output/view))
SELECT r.id, selector_field_selection(r,'%s') FROM records r`, mode)})
			if mode == "unknown" {
				require.ErrorContains(t, err, "unsupported selector field selection mode")
				return
			}
			require.NoError(t, err)
			require.Equal(t, spec.FieldSelectionMode(mode), compiled.Component.RootView.Selector.FieldSelection)
			require.NotContains(t, compiled.Component.RootView.Source.SQL, "selector_field_selection")
			generator := generate.New(generate.Input{Component: compiled.Component})
			dir := t.TempDir()
			first, err := generator.Generate(dir)
			require.NoError(t, err)
			second, err := generator.Generate(dir)
			require.NoError(t, err)
			require.Equal(t, first.Files, second.Files)
			input, err := generator.RuntimeInputType()
			require.NoError(t, err)
			output, err := generator.RuntimeOutputType()
			require.NoError(t, err)
			for _, f := range first.Plan.Output.Fields {
				if f.Name == "Data" {
					require.Contains(t, f.Tag, "selectorFieldSelection="+mode)
				}
			}
			// Reload policy solely from generated output tags; the original DQL policy
			// cannot mask a missing metadata round trip.
			component := &spec.Component{Key: compiled.Component.Key, Name: compiled.Component.Name, Routes: compiled.Component.Routes}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: input, OutputType: output, DirectViewField: "Data"})
			require.NoError(t, err)
			require.Equal(t, spec.FieldSelectionMode(mode), artifact.Reader.Root.View.Spec.Selector.FieldSelection)
		})
	}
}
