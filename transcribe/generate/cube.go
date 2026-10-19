package generate

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/report"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	authoring "github.com/viant/datly/transcribe/compile"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

type CubePlan struct {
	Name          string
	InputName     string
	InputType     reflect.Type
	GenerateInput bool
	Component     *spec.Component
	Definition    report.Definition
}

func (r *planResolver) resolveCubes() error {
	p := r.plan
	if !r.requireConcreteHelpers || p == nil || p.ShapesOnly || p.Report == nil || !p.Report.Enabled {
		return nil
	}
	component := r.input.Component.Clone()
	if component.RootView == nil || component.RootView.Groupable == nil || !*component.RootView.Groupable {
		return nil
	}
	// Source identity comes from the generated holder's package and component;
	// an authored namespace is never a shared global cube package.
	component.Key = spec.Key{Kind: spec.KindComponent, Scope: p.Package, Name: p.ComponentName}
	component.Settings.Report.LinkedFacade = false
	materializer := newRuntimeInputMaterializer(p, r.types)
	input, err := materializer.inputType()
	if err != nil {
		return fmt.Errorf("cube source input: %w", err)
	}
	output, err := materializer.outputType()
	if err != nil {
		return fmt.Errorf("cube source output: %w", err)
	}
	if p.Input.Ownership == ContractLinked {
		input, err = materializer.descriptorType(p.Input.Type)
		if err != nil {
			return err
		}
	}
	if p.Output.Ownership == ContractLinked {
		output, err = materializer.descriptorType(p.Output.Type)
		if err != nil {
			return err
		}
	}
	component, err = (bootstrap.ContractResolver{Component: component, InputType: xshape.Linked(input).Descriptor(), OutputType: xshape.Linked(output).Descriptor()}).Resolve()
	if err != nil {
		return err
	}
	// Linked rows can reach generation without canonical columns. Admit their
	// existing root projection before deriving reports, then retain its SQL
	// metadata for the reader snapshot emitted later in this same plan.
	if err = bootstrap.CompileOutputColumns(component, output); err != nil {
		return err
	}
	if err = authoring.BackfillReportMetadata(component, r.input.Resources); err != nil {
		return err
	}
	if component.RootView.Columns != nil {
		r.input.Component.RootView.Columns = make([]*spec.Column, len(component.RootView.Columns))
		for i, column := range component.RootView.Columns {
			r.input.Component.RootView.Columns[i] = column.Clone()
		}
	}
	component, err = bootstrap.ResolveReportSourceWithContracts(component, input, output, r.input.Resources)
	if err != nil {
		return err
	}
	// A generated reader's root selector fields live on its companion, not
	// its business Input. The cube has its own projection and paging controls;
	// omit only those exact side declarations from this authoring description.
	// Keep the source component and emitted companion intact for native MCP.
	description := component.Clone()
	// An admitted companion is validated and projected by the native description
	// compiler; retain its complete canonical parameter association.
	if component.SelectorCompanion == nil {
		ownedSide := map[string]bool{}
		for _, field := range p.QuerySelectors {
			metadata, err := tag.ParseField(reflect.StructField{Name: field.Name, Tag: reflect.StructTag(field.Tag)})
			if err != nil {
				return err
			}
			if metadata.Binding != nil {
				parameter := &spec.Parameter{Name: metadata.Binding.Name, Source: spec.BindSource{Kind: metadata.Binding.Location.Kind, Name: metadata.Binding.Location.In}}
				ownedSide[parameter.Identity()] = true
			}
		}
		description.Parameters = nil
		for _, parameter := range component.Parameters {
			if parameter != nil && ownedSide[parameter.Identity()] && report.OwnsRootSelector(component, parameter) {
				continue
			}
			description.Parameters = append(description.Parameters, parameter)
		}
	}
	described, err := bootstrap.DescribeInputContract(description, input, materializer.identifierType)
	if err != nil {
		return fmt.Errorf("describe cube source input: %w", err)
	}
	types := typecatalog.NewCatalog()
	if linked := strings.TrimSpace(component.Settings.Report.LinkedInputType); linked != "" {
		typ, err := materializer.descriptorType(linked)
		if err != nil {
			return err
		}
		if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: typ, PkgPath: typ.PkgPath(), Name: typ.Name()}); err != nil {
			return err
		}
	}
	// Cube composition remains a runtime-derived capability over this exact
	// source. Only the ordinary cube facade is emitted as linked Go here.
	component.Settings.Report.Compose = nil
	project, err := report.NewProjectCompiler(report.ProjectConfig{Types: types}).Compile([]report.Source{{Component: component, Input: described, OutputType: output}})
	if err != nil {
		return err
	}
	for _, derived := range project.Derived() {
		name := derived.Component.Key.Name
		inputName := name + "Input"
		generateInput := strings.TrimSpace(component.Settings.Report.LinkedInputType) == ""
		if !generateInput {
			inputName = component.Settings.Report.LinkedInputType
		}
		p.Cubes = append(p.Cubes, CubePlan{Name: name, InputName: inputName, InputType: derived.InputType, GenerateInput: generateInput, Component: derived.Component, Definition: derived.Definition})
	}
	if len(p.Cubes) > 0 {
		p.CubeDestination = p.Generation.File("cube", lowerSnake(p.ComponentName)+"_cube.go")
		p.Report.LinkedFacade = true
	}
	return nil
}
