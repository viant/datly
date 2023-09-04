package translator

import (
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/config/codec"
	"github.com/viant/datly/config/codec/transfer"
	"github.com/viant/datly/config/codec/xmltab"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"github.com/viant/xreflect"
	"reflect"
)

func (s *Service) updateOutputParameters(resource *Resource, rootViewlet *Viewlet) error {
	if tmpl := rootViewlet.View.Template; tmpl != nil && tmpl.Summary != nil {
		return nil //NOT YEY supported for summary
	}
	if resource.Rule.IsGeneratation {
		return nil
	}

	types := s.newTypeRegistry(resource, rootViewlet)

	outputState, err := resource.OutputState.Compact(resource.rule.Module)
	if err != nil {
		return err
	}
	outputParameters := s.ensureOutputParameters(resource, outputState)
	dataParameter := outputParameters.LookupByLocation(state.KindOutput, "data")
	s.updateOutputParameterType(dataParameter, rootViewlet)

	for _, parameter := range outputParameters {
		if err = s.adjustTransferOutputType(parameter, types, resource); err != nil {
			return err
		}
	}
	resource.Rule.Route.Output.Type.Parameters = outputParameters
	return nil
}

func (s *Service) newTypeRegistry(resource *Resource, rootViewlet *Viewlet) *xreflect.Types {
	types := xreflect.NewTypes(xreflect.WithRegistry(config.Config.Types))
	for _, aType := range resource.Resource.Types {
		_ = types.Register(aType.Name, xreflect.WithTypeDefinition(aType.DataType))
	}
	if aType := rootViewlet.TypeDefinition; aType != nil {
		_ = types.Register(aType.Name, xreflect.WithTypeDefinition(aType.DataType))

	}
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
		adjustedType, err := s.adjustTransferType(parameter, types, dest)
		if err != nil {
			return err
		}
		parameter.Output.Schema = &state.Schema{Name: adjustedTypeName}
		resource.AppendTypeDefinition(&view.TypeDefinition{
			Name:     adjustedTypeName,
			DataType: adjustedType.String(),
		})
	}

	return nil
}

func (s *Service) adjustTransferType(parameter *state.Parameter, types *xreflect.Types, dest reflect.Type) (reflect.Type, error) {
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
		if tag.AsXmltab {
			outputType = reflect.TypeOf(&xmltab.Result{})
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
	if adjustedDest, err = adjustedDest.Compact("autogen"); err != nil {
		return nil, fmt.Errorf("failed to rewrite transfer type: %v %w", parameter.Name, err)
	}
	adjustedType, err := adjustedDest.ViewParameters().ReflectType("autogen", types.Lookup, false)
	if adjustedDest, err = adjustedDest.Compact("autogen"); err != nil {
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
	if dataParameter := outputParameters.LookupByLocation(state.KindOutput, "data"); dataParameter == nil {
		outputParameters = append(outputParameters, component.DefaultDataOutputParameter())
	}
	return outputParameters
}

func (s *Service) updateOutputParameterType(dataParameter *state.Parameter, rootViewlet *Viewlet) {
	dataParameter.Schema.Name = TypeDefinitionName(rootViewlet)
	dataParameter.Schema.DataType = "*" + TypeDefinitionName(rootViewlet)
	cardinality := string(state.Many)
	setter.SetStringIfEmpty(&cardinality, string(rootViewlet.Cardinality))
	dataParameter.Schema.Cardinality = state.Cardinality(cardinality)
}
