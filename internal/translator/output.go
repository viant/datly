package translator

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/locator/output/keys"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/extension/codec"
	"github.com/viant/datly/view/extension/codec/transfer"
	"github.com/viant/datly/view/state"
	"github.com/viant/scy/auth"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/structology"
	"github.com/viant/xdatly/handler/response/tabular/tjson"
	"github.com/viant/xdatly/handler/response/tabular/xml"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

func (s *Service) updateOutputParameters(resource *Resource, rootViewlet *Viewlet) (err error) {
	s.updateOutputFieldTypes(resource)
	if tmpl := rootViewlet.View.Template; tmpl != nil && tmpl.Summary != nil {
		//	return nil //NOT YEY supported for summary
	}
	if resource.Rule.IsGeneratation {
		return nil
	}

	typesRegistry := s.newTypeRegistry(resource, rootViewlet)

	for _, parameter := range resource.OutputState.FilterByKind(state.KindState) {
		if stateParameter := resource.State.Lookup(parameter.In.Name); stateParameter != nil {
			res := view.NewResources(&resource.Resource, &rootViewlet.View.View)
			if err = stateParameter.Init(context.Background(), res); err != nil {
				return err
			}

			parameter.Schema = stateParameter.OutputSchema()
		}
	}

	if err = resource.OutputState.EnsureReflectTypes(resource.rule.ModuleLocation); err != nil {
		return err
	}

	outputParameters := s.ensureOutputParameters(resource, resource.OutputState)
	dataParameter := outputParameters.LookupByLocation(state.KindOutput, keys.ViewData)
	if dataParameter != nil {
		s.updateParameterWithComponentOutputType(dataParameter, rootViewlet)
	}

	contract.EnsureParameterTypes(outputParameters, nil, resource.Rule.Doc.Parameters, resource.Rule.Doc.Filter)
	for _, parameter := range outputParameters {
		if schema := parameter.Schema; schema != nil && schema.Name == "$ViewType" {
			schema.DataType = strings.Replace(schema.DataType, "interface{}", "*"+dataParameter.Schema.Name, 1)
			schema.Name = schema.DataType
			parameter.Tag += fmt.Sprintf(` typeName:"%s"`, dataParameter.Schema.Name)
		}
		if err = s.adjustParameterSetting(resource, parameter, typesRegistry); err != nil {
			return err
		}
	}

	if resource.Rule.Route.Output.Type.Schema == nil {
		resource.Rule.Route.Output.Type.Schema = &state.Schema{}
	}
	resource.Rule.Route.Output.Type.Package = resource.rule.Package()
	resource.Rule.Route.Output.Type.Parameters = outputParameters
	outputTypeName := state.SanitizeTypeName(rootViewlet.Name) + "Output"
	rType, err := resource.typeRegistry.Lookup(outputTypeName, xreflect.WithPackage(resource.rule.Package()))
	if err == nil && rType.Name() != "" {
		resource.Rule.Route.Output.Type.Name = outputTypeName
	}
	return nil
}

func (s *Service) updateOutputFieldTypes(resource *Resource) {
	for _, parameter := range resource.OutputState {
		switch parameter.In.Kind {
		case state.KindAsync:
			contract.UpdateParameterAsyncType(&parameter.Parameter)
		case state.KindOutput:
			contract.UpdateOutputParameterType(&parameter.Parameter)
		case state.KindMeta:
			contract.UpdateParameterMetaType(&parameter.Parameter)
		case state.KindRequestBody:
			if baseParameter := resource.State.FilterByKind(state.KindRequestBody); len(baseParameter) > 0 {
				parameter.Schema = baseParameter[0].Schema.Clone()
			}
		}
	}
}

func (s *Service) updateCodecParamters(ctx context.Context, resource *Resource) error {
	parameters := resource.State
	if err := resource.ensurePathParametersSchema(ctx, parameters); err != nil {
		return err
	}
	for _, parameter := range parameters {
		if err := s.adjustParameterSetting(resource, &parameter.Parameter, resource.typeRegistry); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) updateExplicitOutputType(resource *Resource, rootViewlet *Viewlet, outputParameters state.Parameters) error {
	outputTypeDef := outputTypeDefinition(resource)
	if outputTypeDef == nil {
		return nil
	}
	typesRegistry := s.newTypeRegistry(resource, rootViewlet)

	if rootViewlet.TypeDefinition != nil {
		err := typesRegistry.Register(rootViewlet.TypeDefinition.Name, xreflect.WithTypeDefinition(rootViewlet.TypeDefinition.DataType))
		if err != nil {
			return err
		}
	}

	outputResource := resource.Resource
	outputResource.SetTypes(typesRegistry)
	resourcelet := view.NewResources(&outputResource, &rootViewlet.View.View)
	compactedParameters := resource.OutputState.Parameters()
	compactedParameters.FlagOutput()

	for _, parameter := range outputParameters {
		err := s.updateOutputParameterSchema(parameter, typesRegistry, resource)
		if err != nil {
			return err
		}
	}

	for _, parameter := range outputParameters.FilterByKind(state.KindObject) {
		if err := parameter.Init(context.Background(), resourcelet); err != nil {
			return err
		}
		parameter.Schema.Name = parameter.Name
		resource.AppendTypeDefinition(&view.TypeDefinition{Name: parameter.Schema.Name, Package: resource.rule.Package(), DataType: parameter.Schema.Type().String()})
	}

	outputType, err := compactedParameters.ReflectType(resource.rule.Package(), typesRegistry.Lookup)
	if err != nil {
		return fmt.Errorf("failed to build outputType: %w", err)
	}

	resource.Rule.Route.Output.Type.Parameters = compactedParameters
	outputTypeDef.DataType = outputType.String()
	return nil
}

func (s *Service) updateOutputParameterSchema(parameter *state.Parameter, typesRegistry *xreflect.Types, resource *Resource) error {
	if len(parameter.Object) > 0 {
		for _, item := range parameter.Object {
			if err := s.updateOutputParameterSchema(item, typesRegistry, resource); err != nil {
				return err
			}
		}
		return nil
	}
	if len(parameter.Repeated) > 0 {
		for _, item := range parameter.Repeated {
			if err := s.updateOutputParameterSchema(item, typesRegistry, resource); err != nil {
				return err
			}
		}
		return nil
	}
	if parameter.Schema.Type() != nil && parameter.Schema.Type().Kind() != reflect.Interface {
		resource.addParameterType(parameter)
		return nil
	}
	rType, err := types.LookupType(typesRegistry.Lookup, parameter.Schema.TypeName())
	if err != nil {
		return fmt.Errorf("failed to build output, %s %w", parameter.Name, err)
	}
	parameter.Schema.SetType(rType)
	return nil
}

func (s *Service) adjustParameterSetting(resource *Resource, parameter *state.Parameter, types *xreflect.Types) (err error) {
	if len(parameter.Repeated) > 0 {
		for _, repeated := range parameter.Repeated {
			if err = s.adjustParameterSetting(resource, repeated, types); err != nil {
				return err
			}
		}
		if err = s.adjustParameterType(parameter, types, resource); err != nil {
			return err
		}

		if err = s.adjustCodecType(parameter, types, resource); err != nil {
			return err
		}
		itemTypeName := parameter.Repeated[0].OutputSchema().Name
		if !strings.HasPrefix(itemTypeName, "*") {
			itemTypeName = "*" + itemTypeName
		}
		parameter.Schema = &state.Schema{Cardinality: state.Many, Name: itemTypeName}
		return err
	}
	if len(parameter.Object) > 0 {
		for _, group := range parameter.Object {
			if err = s.adjustParameterSetting(resource, group, types); err != nil {
				return err
			}
		}
		rType, _ := parameter.Object.ReflectType(resource.rule.ModuleLocation, types.Lookup)
		parameter.Schema = state.NewSchema(rType)
		return nil
	}

	if err = s.adjustCodecType(parameter, types, resource); err != nil {
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
	if resource.typeRegistry != nil {
		return resource.typeRegistry
	}

	types := xreflect.NewTypes(xreflect.WithRegistry(extension.Config.Types))
	for _, aType := range resource.Resource.Types {
		_ = types.Register(aType.Name, xreflect.WithTypeDefinition(aType.DataType), xreflect.WithPackage(resource.rule.Package()))
		_ = types.Register(aType.Name, xreflect.WithTypeDefinition(aType.DataType))

	}
	if aType := rootViewlet.TypeDefinition; aType != nil {
		_ = types.Register(aType.Name, xreflect.WithTypeDefinition(aType.DataType), xreflect.WithPackage(resource.rule.Package()))

	}
	resource.typeRegistry = types
	return types
}

func (s *Service) adjustParameterType(parameter *state.Parameter, types *xreflect.Types, resource *Resource) error {
	if schema := parameter.Schema; schema.Name != "" && schema.Type() == nil {
		rType, err := types.Lookup(schema.Name, xreflect.WithPackage(schema.Package))
		if err != nil {
			return err
		}
		schema.SetType(rType)
	}
	return nil
}

func (s *Service) adjustCodecType(parameter *state.Parameter, types *xreflect.Types, resource *Resource) error {
	output := parameter.Output
	if output == nil {
		return nil
	}
	switch output.Name {
	case codec.KeyTransfer:
		if len(output.Args) == 0 {
			return fmt.Errorf("%v invalid arguments count", output.Name)
		}
		err := s.adjustTransferCodecOutput(parameter, types, resource, output)
		if err != nil {
			return err
		}
	case codec.Encode:
		if len(output.Args) == 0 {
			return fmt.Errorf("%v invalid arguments count", output.Name)
		}
		destTypeName := output.Args[0]
		if output.Schema == nil {
			output.Schema = &state.Schema{}
		}
		output.Schema.Name = destTypeName
	case codec.KeyFirebaseAuth:
		if len(output.Args) < 2 {
			return fmt.Errorf("%v invalid arguments count", output.Name)
		}
		if output.Schema == nil {
			output.Schema = &state.Schema{}
		}
		if output.Schema.Type() == nil {
			output.Schema.SetType(reflect.TypeOf(&auth.Token{}))
		}
	case codec.KeyJwtClaim:
		if output.Schema == nil {
			output.Schema = &state.Schema{}
		}
		if output.Schema.Type() == nil {
			output.Schema.SetType(reflect.TypeOf(&jwt.Claims{}))
		}
	}

	return nil
}

func (s *Service) adjustTransferCodecOutput(parameter *state.Parameter, typesRegistry *xreflect.Types, resource *Resource, output *state.Codec) (err error) {
	var destType reflect.Type
	destTypeName := output.Args[0]
	if parameter.Output != nil && parameter.Output.Schema != nil {
		outputSchema := parameter.Output.Schema
		if destType = outputSchema.Type(); destType != nil {
			destType = types.EnsureStruct(destType)
		}
		setter.SetStringIfEmpty(&outputSchema.Name, destTypeName)
	}

	pkg := resource.typePackages[destTypeName]
	if destType == nil {
		if destType, err = typesRegistry.Lookup(destTypeName, xreflect.WithPackage(pkg)); err != nil {
			return fmt.Errorf("failed to init transfer codec for type %v: parameter: %v, %w", destTypeName, parameter.Name, err)
		}

	}
	if len(output.Args) == 1 {
		parameter.Output.Schema = &state.Schema{Name: destTypeName}
		return nil
	}

	adjustedTypeName := output.Args[1]
	adjustedType, err := s.adjustTransferCodecType(resource, parameter, typesRegistry, destType)
	if err != nil {
		return err
	}
	parameter.Output.Schema = &state.Schema{Name: adjustedTypeName}
	parameter.Output.Schema.SetType(adjustedType)
	resource.addParameterType(parameter)

	resource.AppendTypeDefinition(&view.TypeDefinition{
		Name:     adjustedTypeName,
		DataType: adjustedType.String(),
	})
	_ = typesRegistry.Register(adjustedTypeName, xreflect.WithReflectType(adjustedType))
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
		switch tag.Codec {
		case codec.KeyFilters:
			outputType = reflect.TypeOf(predicate.NamedFilters{})
		case codec.KeyXmlTab:
			outputType = reflect.TypeOf(&xml.Tabular{})
		case codec.KeyJsonTab:
			outputType = reflect.TypeOf(&tjson.Tabular{})
		case codec.KeyXmlFilter:
			outputType = reflect.TypeOf(&xml.FilterHolder{})
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
	adjustedType, err := adjustedDest.Parameters().ReflectType(resource.rule.ModuleLocation, types.Lookup)
	if adjustedDest, err = adjustedDest.Compact(resource.rule.ModuleLocation); err != nil {
		return nil, fmt.Errorf("failed to adjust transfer type: %v %w", parameter.Name, err)
	}
	return adjustedType, nil
}

func (s *Service) ensureOutputParameters(resource *Resource, outputState inference.State) state.Parameters {
	outputParameters := outputState.Parameters()
	if len(outputParameters) == 0 {
		if field := resource.Rule.Route.Output.Field; field != "" {
			outputParameters = append(outputParameters, contract.DataOutputParameter(field))
			outputParameters = append(outputParameters, contract.DefaultStatusOutputParameter())

		} else {
			outputParameters = append(outputParameters, contract.DefaultDataOutputParameter())
		}
	}
	return outputParameters
}

func (s *Service) updateParameterWithComponentOutputType(dataParameter *state.Parameter, rootViewlet *Viewlet) {
	typeName := rootViewlet.View.Schema.Name
	if typeName == "" || typeName == "string" {
		typeName = view.DefaultTypeName(rootViewlet.Name)
		rootViewlet.View.Schema.Name = typeName
	}
	setter.SetStringIfEmpty(&dataParameter.Schema.Name, typeName)
	setter.SetStringIfEmpty(&dataParameter.Schema.Package, rootViewlet.Resource.rule.Package())
	dataParameter.Schema.DataType = "*" + typeName
	cardinality := string(state.Many)

	setter.SetStringIfEmpty(&cardinality, string(rootViewlet.Cardinality))
	viewSchema := rootViewlet.View.Schema
	dataParameter.Schema.Cardinality = state.Cardinality(cardinality)
	if viewSchema != nil && viewSchema.Cardinality != "" {
		dataParameter.Schema.Cardinality = viewSchema.Cardinality
	}
	dataParameter.SetTypeNameTag()
}
