package translator

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/config/codec"
	"github.com/viant/datly/config/codec/jsontab"
	"github.com/viant/datly/config/codec/transfer"
	"github.com/viant/datly/config/codec/xmlfilter"
	"github.com/viant/datly/config/codec/xmltab"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

func (s *Service) updateOutputParameters(resource *Resource, rootViewlet *Viewlet) error {
	if tmpl := rootViewlet.View.Template; tmpl != nil && tmpl.Summary != nil {
		return nil //NOT YEY supported for summary
	}
	if resource.Rule.IsGeneratation {
		return nil
	}

	typesRegistry := s.newTypeRegistry(resource, rootViewlet)

	var err error

	if err := resource.OutputState.EnsureReflectTypes(resource.rule.ModuleLocation); err != nil {
		return err
	}

	outputParameters := s.ensureOutputParameters(resource, resource.OutputState)
	if dataParameter := outputParameters.LookupByLocation(state.KindOutput, "data"); dataParameter != nil {
		s.updateParameterWithComponentOutputType(dataParameter, rootViewlet)
	}

	component.EnsureOutputKindParameterTypes(outputParameters, nil)
	for _, parameter := range outputParameters {
		if err = s.adjustOutputParameter(resource, parameter, typesRegistry); err != nil {
			return err
		}
	}
	resource.Rule.Route.Output.Type.Parameters = outputParameters
	return nil
}

func (s *Service) updateExplicitOutputType(resource *Resource, rootViewlet *Viewlet, outputParameters state.Parameters) error {
	outputTypeDef := outputTypeDefinition(resource)
	if outputTypeDef == nil {
		return nil
	}
	typesRegistry := s.newTypeRegistry(resource, rootViewlet)

	if rootViewlet.TypeDefinition != nil {
		typesRegistry.Register(rootViewlet.TypeDefinition.Name, xreflect.WithTypeDefinition(rootViewlet.TypeDefinition.DataType))
	}

	outputResource := resource.Resource
	outputResource.SetTypes(typesRegistry)
	resourcelet := view.NewResourcelet(&outputResource, &rootViewlet.View.View)
	compactedParameters := resource.OutputState.ViewParameters()
	compactedParameters.FlagOutput()

	for _, parameter := range outputParameters {
		err := s.updateOutputParameterSchema(parameter, typesRegistry)
		if err != nil {
			return err
		}
	}

	for _, parameter := range outputParameters.FilterByKind(state.KindGroup) {
		if err := parameter.Init(context.Background(), resourcelet); err != nil {
			return err
		}
		parameter.Schema.Name = parameter.Name + "Group"
		resource.AppendTypeDefinition(&view.TypeDefinition{Name: parameter.Schema.Name, DataType: parameter.Schema.Type().String()})
	}

	outputType, err := compactedParameters.ReflectType(resource.rule.ModuleLocation, typesRegistry.Lookup, false)
	if err != nil {
		return fmt.Errorf("failed to build outputType: %w", err)
	}
	resource.Rule.Route.Output.Type.Parameters = compactedParameters
	outputTypeDef.DataType = outputType.String()
	resource.Rule.Route.Output.Type.Schema = &state.Schema{Name: outputTypeDef.Name}
	return nil
}

func (s *Service) updateOutputParameterSchema(parameter *state.Parameter, typesRegistry *xreflect.Types) error {
	if len(parameter.Group) > 0 {
		for _, item := range parameter.Group {
			if err := s.updateOutputParameterSchema(item, typesRegistry); err != nil {
				return err
			}
		}
		return nil
	}
	if len(parameter.Repeated) > 0 {
		for _, item := range parameter.Repeated {
			if err := s.updateOutputParameterSchema(item, typesRegistry); err != nil {
				return err
			}
		}
		return nil
	}
	if parameter.Schema.Type() != nil && parameter.Schema.Type().Kind() != reflect.Interface {
		return nil
	}
	typeName := parameter.Schema.DataType
	if typeName == "" {
		typeName = parameter.Schema.Name
	}
	rType, err := types.LookupType(typesRegistry.Lookup, typeName)
	if err != nil {
		return fmt.Errorf("failed to build output, %s %w", parameter.Name, err)
	}
	parameter.Schema.SetType(rType)
	return nil
}

func (s *Service) adjustOutputParameter(resource *Resource, parameter *state.Parameter, types *xreflect.Types) (err error) {
	if len(parameter.Repeated) > 0 {
		for _, repeated := range parameter.Repeated {
			if err = s.adjustOutputParameter(resource, repeated, types); err != nil {
				return err
			}
		}
		err = s.adjustTransferOutputType(parameter, types, resource)
		itemTypeName := parameter.Repeated[0].OutputSchema().Name
		if !strings.HasPrefix(itemTypeName, "*") {
			itemTypeName = "*" + itemTypeName
		}
		parameter.Schema = &state.Schema{Cardinality: state.Many, Name: itemTypeName}
		return err
	}
	if len(parameter.Group) > 0 {
		for _, group := range parameter.Group {
			if err = s.adjustOutputParameter(resource, group, types); err != nil {
				return err
			}
		}
		rType, _ := parameter.Group.ReflectType(resource.rule.ModuleLocation, types.Lookup, false)
		parameter.Schema = state.NewSchema(rType)
		return nil
	}
	if err = s.adjustTransferOutputType(parameter, types, resource); err != nil {
		return err
	}
	return nil
}

func outputTypeDefinition(resource *Resource) *view.TypeDefinition {
	var outputTypeDef *view.TypeDefinition
	if param := resource.Rule.OutputParameter; param != nil {
		paramSchema := param.Schema.DataType
		if paramSchema == "" {
			paramSchema = param.Schema.Name
		}
		return resource.LookupTypeDef(paramSchema)
	}
	return outputTypeDef
}

func (s *Service) newTypeRegistry(resource *Resource, rootViewlet *Viewlet) *xreflect.Types {
	if rootViewlet.typeRegistry != nil {
		return rootViewlet.typeRegistry
	}

	types := xreflect.NewTypes(xreflect.WithRegistry(config.Config.Types))
	for _, aType := range resource.Resource.Types {
		_ = types.Register(aType.Name, xreflect.WithTypeDefinition(aType.DataType))
	}
	if aType := rootViewlet.TypeDefinition; aType != nil {
		_ = types.Register(aType.Name, xreflect.WithTypeDefinition(aType.DataType))

	}
	rootViewlet.typeRegistry = types
	return types
}

func (s *Service) adjustTransferOutputType(parameter *state.Parameter, types *xreflect.Types, resource *Resource) error {

	if output := parameter.Output; output != nil && output.Name == codec.KeyTransfer {
		destTypeName := output.Args[0]
		dest, err := types.Lookup(destTypeName)
		if err != nil {
			return fmt.Errorf("failed to init transfer codec for type %v: parameter: %v, %w", destTypeName, parameter.Name, err)
		}
		if len(output.Args) == 1 {
			parameter.Output.Schema = &state.Schema{Name: destTypeName}
			return nil
		}
		adjustedTypeName := output.Args[1]
		adjustedType, err := s.adjustTransferCodecType(resource, parameter, types, dest)
		if err != nil {
			return err
		}
		parameter.Output.Schema = &state.Schema{Name: adjustedTypeName}
		resource.AppendTypeDefinition(&view.TypeDefinition{
			Name:     adjustedTypeName,
			DataType: adjustedType.String(),
		})
		_ = types.Register(adjustedTypeName, xreflect.WithReflectType(adjustedType))
	}

	return nil
}

func (s *Service) adjustTransferCodecType(resource *Resource, parameter *state.Parameter, types *xreflect.Types, dest reflect.Type) (reflect.Type, error) {
	destType := structology.NewStateType(dest)
	selectors := destType.MatchByTag(transfer.TagName)
	var adjustedDest inference.State
	source := parameter.Schema.Type()
	//TODO add args
	sourceType := structology.NewStateType(source)
	for _, selector := range selectors {
		tag := transfer.ParseTag(selector.Tag().Get(transfer.TagName))
		sourceSelector := sourceType.Lookup(tag.From)
		if sourceSelector == nil {
			return nil, fmt.Errorf("invalid transfer, field: %v does not have coresponding source field %v, %s ", selector.Name(), tag.From, source.String())
		}
		outputType := sourceSelector.Type()
		if tag.AsXmlTab {
			outputType = reflect.TypeOf(&xmltab.Result{})
		}
		if tag.AsXmlFilter {
			outputType = reflect.TypeOf(&xmlfilter.Result{})
		}
		if tag.AsJsonTab {
			outputType = reflect.TypeOf(&jsontab.Result{})
		}
		adjustedDest.Append(&inference.Parameter{
			Parameter: state.Parameter{
				Reference: shared.Reference{},
				Name:      selector.Path(),
				Schema:    state.NewSchema(outputType),
				Tag:       string(selector.Tag() + " " + sourceSelector.Tag()),
				In:        state.NewQueryLocation(selector.Name()),
			}})
	}
	var err error
	if adjustedDest, err = adjustedDest.Compact(resource.rule.ModuleLocation); err != nil {
		return nil, fmt.Errorf("failed to rewrite transfer type: %v %w", parameter.Name, err)
	}
	adjustedType, err := adjustedDest.ViewParameters().ReflectType(resource.rule.ModuleLocation, types.Lookup, false)
	if adjustedDest, err = adjustedDest.Compact(resource.rule.ModuleLocation); err != nil {
		return nil, fmt.Errorf("failed to adjust transfer type: %v %w", parameter.Name, err)
	}
	return adjustedType, nil
}

func (s *Service) ensureOutputParameters(resource *Resource, outputState inference.State) state.Parameters {
	outputParameters := outputState.ViewParameters()
	if len(outputParameters) == 0 {
		if field := resource.Rule.Route.Output.Field; field != "" {
			outputParameters = append(outputParameters, component.DataOutputParameter(field))
			outputParameters = append(outputParameters, component.DefaultStatusOutputParameter())

		} else {
			outputParameters = append(outputParameters, component.DefaultDataOutputParameter())
		}
	}
	return outputParameters
}

func (s *Service) updateParameterWithComponentOutputType(dataParameter *state.Parameter, rootViewlet *Viewlet) {
	dataParameter.Schema.Name = TypeDefinitionName(rootViewlet)
	dataParameter.Schema.DataType = "*" + TypeDefinitionName(rootViewlet)
	cardinality := string(state.Many)
	setter.SetStringIfEmpty(&cardinality, string(rootViewlet.Cardinality))
	dataParameter.Schema.Cardinality = state.Cardinality(cardinality)
}
