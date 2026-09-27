package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
)

type recorderBoundReader struct {
	dexec.Reader
	recorder *observability.Recorder
}

func (r *recorderBoundReader) WithRecorder(recorder *observability.Recorder) dexec.Reader {
	copy := *r
	copy.recorder = recorder
	return &copy
}

func TestRuntimeReaderRecorderPreparationIsDetached(t *testing.T) {
	component := componentSpec("Observed", "GET", "/observed", nil)
	artifact := componentArtifact(t, component, reflect.TypeFor[struct{}](), reflect.TypeFor[struct{}]())
	originalRecorder := observability.NewRecorder(nil)
	originalReader := &recorderBoundReader{recorder: originalRecorder}
	registered := &RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[struct{}](), Reader: originalReader}
	owner, err := NewObservability(ObservabilityConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Shutdown(context.Background())) })
	for _, mode := range []string{"eager", "lazy single", "lazy family"} {
		t.Run(mode, func(t *testing.T) {
			var rt *Runtime
			var err error
			if mode == "eager" {
				rt, err = NewRuntime([]*RegisteredComponent{registered}, WithManagedObservability(owner))
			} else {
				rt, err = NewIndexedRuntime([]*spec.Component{component}, nil, remoteFamilyLoader{component: registered}, WithManagedObservability(owner))
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
			for attempt := 0; attempt < 2; attempt++ {
				var loaded *RegisteredComponent
				if mode == "lazy family" {
					family, err := rt.LoadComponents(context.Background(), component.Key)
					require.NoError(t, err)
					require.Len(t, family, 1)
					loaded = family[0]
				} else {
					loaded, err = rt.LoadComponent(context.Background(), component.Key)
					require.NoError(t, err)
				}
				require.NotSame(t, registered, loaded)
				reader := loaded.Reader.(*recorderBoundReader)
				require.NotSame(t, originalReader, reader)
				require.Same(t, owner.Recorder, reader.recorder)
				require.NotEmpty(t, loaded.Providers, "static capabilities must still be attached")
				require.Same(t, originalReader, registered.Reader)
				require.Same(t, originalRecorder, originalReader.recorder)
				require.Empty(t, registered.Providers)
			}
		})
	}
}
