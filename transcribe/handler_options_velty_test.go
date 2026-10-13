package transcribe

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/handlergen"
)

func TestTranscribeGeneratedHandlerRejectsCompetingHandlerLogic(t *testing.T) {
	tests := []struct {
		name   string
		source *Source
	}{
		{
			name: "supplied Velty",
			source: &Source{Name: "Events", Text: writeGenerationDQL(),
				VeltyHandler: &gen.VeltyHandlerAsset{Template: `$dml.Insert("EVENTS", $Input.Events)`}},
		},
		{
			name: "authored service",
			source: &Source{Name: "Events", Text: `#setting($_ = $route('/events', 'POST'))
#define($_ = $Events<*EventsView>(body/Data))
$dml.Insert("EVENTS", $Input.Events)`},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			_, err := NewCompiler().Transcribe(context.Background(), Request{
				Source: testCase.source, Destination: root,
				Options: Options{Handler: HandlerOptions{Target: HandlerVelty, Operation: WritePost, Table: "EVENTS"}},
			})
			var compileError *CompileError
			if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 ||
				compileError.Diagnostics[0].Code != "DQL-HANDLER" || !strings.Contains(err.Error(), "mutually exclusive") {
				t.Fatalf("Compile() error = %#v", err)
			}
		})
	}
}

func writeGenerationDQL() string {
	return `#setting($_ = $route('/events', 'POST'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many'))
#define($_ = $Data<[]*EventsView>(output/body))
SELECT ID, NAME FROM EVENTS`
}

func generateVeltyTarget(component *spec.Component, bindings gen.ViewBindings, options *HandlerOptions) (*gen.VeltyHandlerAsset, error) {
	if options == nil {
		return nil, errors.New("handler options are required")
	}
	selected := *options
	selected.Target = HandlerVelty
	input := &gen.Input{Component: component, ViewBindings: bindings}
	generation := handlergen.New(&handlergen.Context{Component: component, ViewBindings: bindings}, input, Options{Contracts: ContractsAuto, Handler: selected})
	if err := generation.Prepare(); err != nil {
		return nil, err
	}
	return input.VeltyHandler, nil
}
