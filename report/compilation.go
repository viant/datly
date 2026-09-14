package report

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

// ComponentArtifact is one compiled component awaiting concrete runtime
// capabilities such as a SQL reader or custom handler.
type ComponentArtifact struct {
	artifact          *bootstrap.Artifact
	inputType         reflect.Type
	outputType        reflect.Type
	report            bool
	predefinedHandler handler.Handler
}

// Component returns detached canonical metadata.
func (a *ComponentArtifact) Component() *spec.Component {
	if a == nil {
		return nil
	}
	return a.artifact.Component.Clone()
}

// InputContract returns the immutable compiled input contract.
func (a *ComponentArtifact) InputContract() *registry.InputContract {
	if a == nil {
		return nil
	}
	return a.artifact.Input
}

// ReaderCompilation returns the bootstrap-owned immutable reader products.
func (a *ComponentArtifact) ReaderCompilation() *bootstrap.CompiledReader {
	if a == nil {
		return nil
	}
	return a.artifact.ReaderCompilation()
}

func (a *ComponentArtifact) InputType() reflect.Type {
	if a == nil {
		return nil
	}
	return a.inputType
}

func (a *ComponentArtifact) OutputType() reflect.Type {
	if a == nil {
		return nil
	}
	return a.outputType
}

func (a *ComponentArtifact) IsReport() bool {
	if a == nil {
		return false
	}
	return a.report
}

// Compilation is a complete, unpublished base-and-report artifact set.
type Compilation struct {
	artifacts []*ComponentArtifact
	types     *typecatalog.Catalog
}

func (c *Compilation) Artifacts() []*ComponentArtifact {
	if c == nil {
		return nil
	}
	result := make([]*ComponentArtifact, len(c.artifacts))
	for index, artifact := range c.artifacts {
		copy := *artifact
		result[index] = &copy
	}
	return result
}

func (c *Compilation) Types() (*typecatalog.Catalog, error) {
	if c == nil || c.types == nil {
		return nil, fmt.Errorf("report compilation type catalog is unavailable")
	}
	return c.types.Clone()
}

// CompileArtifacts compiles base artifacts, derives reports from their exact
// contracts, and compiles the derived artifacts against one isolated catalog.
func (c *ProjectCompiler) CompileArtifacts(inputs []bootstrap.ArtifactInput) (*Compilation, error) {
	if c == nil {
		return nil, fmt.Errorf("report project compiler is required")
	}
	builder, err := bootstrap.NewArtifactBuilder(c.config.Registry)
	if err != nil {
		return nil, err
	}
	types, err := builder.Catalog(c.config.Types)
	if err != nil {
		return nil, err
	}
	ordered, err := orderedArtifactInputs(inputs)
	if err != nil {
		return nil, err
	}
	result := &Compilation{types: types}
	sources := make([]Source, 0, len(ordered))
	sourceInputs := make(map[string]bootstrap.ArtifactInput, len(ordered))
	for _, input := range ordered {
		input.Types = types
		artifact, buildErr := builder.Build(input)
		if buildErr != nil {
			return nil, fmt.Errorf("compile base component %s: %w", input.Component.Key.String(), buildErr)
		}
		result.artifacts = append(result.artifacts, &ComponentArtifact{
			artifact: artifact, inputType: input.InputType, outputType: input.OutputType,
		})
		// Derivation must inspect the source already resolved by the reader
		// compiler (including SQL URI/embed resources), not reopen resources or
		// change the base artifact's authored SQL/cache metadata.
		sourceComponent := artifact.Component.Clone()
		if sourceComponent.RootView != nil && artifact.Reader != nil && artifact.Reader.Root != nil {
			sourceComponent.RootView.Source = artifact.Reader.Root.View.Spec.Source.Clone()
		}
		sources = append(sources, Source{Component: sourceComponent, Input: artifact.Input, OutputType: input.OutputType})
		sourceInputs[artifact.Component.Key.String()] = input
	}
	project, err := NewProjectCompiler(ProjectConfig{Types: types, Authority: c.config.Authority}).Compile(sources)
	if err != nil {
		return nil, err
	}
	for _, derived := range project.Derived() {
		sourceInput, ok := sourceInputs[derived.Plan.Target().Component.String()]
		if !ok {
			return nil, fmt.Errorf("report source input is unavailable for %s", derived.Plan.Target().Component.String())
		}
		artifact, buildErr := builder.Build(bootstrap.ArtifactInput{
			Component: derived.Component, InputType: derived.InputType, OutputType: derived.OutputType, HandlerOwnedOutput: true,
			CodecFactory: sourceInput.CodecFactory, Types: types, Resources: sourceInput.Resources,
		})
		if buildErr != nil {
			return nil, fmt.Errorf("compile derived report component %s: %w", derived.Component.Key.String(), buildErr)
		}
		result.artifacts = append(result.artifacts, &ComponentArtifact{
			artifact: artifact, inputType: derived.InputType, outputType: derived.OutputType,
			report: true, predefinedHandler: derived.Handler,
		})
	}
	return result, nil
}

func projectCatalog(source *typecatalog.Catalog) (*typecatalog.Catalog, error) {
	if source == nil {
		return typecatalog.NewCatalog(), nil
	}
	result, err := source.Clone()
	if err != nil {
		return nil, fmt.Errorf("clone project type catalog: %w", err)
	}
	return result, nil
}

func orderedArtifactInputs(inputs []bootstrap.ArtifactInput) ([]bootstrap.ArtifactInput, error) {
	result := append([]bootstrap.ArtifactInput(nil), inputs...)
	seen := map[string]bool{}
	for _, input := range result {
		if input.Component == nil {
			return nil, fmt.Errorf("base component is required")
		}
		identity := input.Component.Key.String()
		if seen[identity] {
			return nil, fmt.Errorf("duplicate base component %s", identity)
		}
		seen[identity] = true
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Component.Key.String() < result[j].Component.Key.String()
	})
	return result, nil
}
