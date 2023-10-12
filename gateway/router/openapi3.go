package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/gateway/router/openapi3"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	format2 "github.com/viant/structology/format"
	"github.com/viant/structology/format/text"
	"net/http"
	"reflect"
	"time"
)

const (
	successSchemaDescription = "Success object schema"
	errorSchemaDescription   = "Error object schema"

	applicationJson = "application/json"

	stringOutput  = "string"
	objectOutput  = "object"
	arrayOutput   = "array"
	integerOutput = "integer"
	numberOutput  = "number"
	booleanOutput = "boolean"

	int32Format  = "int32"
	int64Format  = "int64"
	doubleFormat = "double"
	empty        = ""
)

var (
	timeType  = reflect.TypeOf(time.Time{})
	errorType = reflect.TypeOf(httputils.Error{})
)

type (
	generator struct {
		commonSchemas    openapi3.Schemas
		_schemasIndex    map[string]*openapi3.Schema
		commonParameters openapi3.ParametersMap
		_parametersIndex map[string]*openapi3.Parameter
	}

	schemaNamed struct {
		schema       *state.Schema
		defaultName  string
		resultSchema bool
	}

	paramLocation struct {
		name string
		in   string
	}
)

func (g *generator) GenerateSpec(ctx context.Context, repoComponents *repository.Service, info openapi3.Info, providers ...*repository.Provider) (*openapi3.OpenAPI, error) {
	components := g.generateComponents()

	paths, err := g.generatePaths(ctx, repoComponents, providers)
	if err != nil {
		return nil, err
	}

	components.Schemas = g.commonSchemas
	components.Parameters = g.commonParameters

	return &openapi3.OpenAPI{
		OpenAPI:      "3.0.1",
		Components:   *components,
		Info:         &info,
		Paths:        paths,
		Security:     nil,
		Servers:      nil,
		Tags:         nil,
		ExternalDocs: nil,
	}, nil
}

func GenerateOpenAPI3Spec(ctx context.Context, components *repository.Service, info openapi3.Info, providers ...*repository.Provider) (*openapi3.OpenAPI, error) {
	return (&generator{
		_schemasIndex:    map[string]*openapi3.Schema{},
		commonSchemas:    map[string]*openapi3.Schema{},
		commonParameters: map[string]*openapi3.Parameter{},
		_parametersIndex: map[string]*openapi3.Parameter{},
	}).GenerateSpec(ctx, components, info, providers...)
}

func (g *generator) generateComponents() *openapi3.Components {
	return &openapi3.Components{
		Extension: nil,
		//TODO: view params or resource params
		Parameters:      nil,
		Headers:         nil,
		RequestBodies:   nil,
		Responses:       nil,
		SecuritySchemes: nil,
		Examples:        nil,
		Links:           nil,
		Callbacks:       nil,
	}
}

func (g *generator) generateSchemas(component *repository.Component) (openapi3.Schemas, error) {
	result := openapi3.Schemas{}

	schemas := g.getViewSchemas(component.View)
	var err error
	for _, schemaWrapper := range schemas {
		typeName := g.typeName(component, schemaWrapper.schema.Type(), schemaWrapper.defaultName)
		kind := schemaWrapper.schema.Type().Kind()
		if kind != reflect.Slice && kind != reflect.Struct {
			continue
		}

		result[schemaWrapper.defaultName], err = g.getOrGenerateSchema(component, schemaWrapper.schema.Type(), !schemaWrapper.resultSchema, typeName, g.typeName(component, schemaWrapper.schema.Type(), schemaWrapper.defaultName))
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (g *generator) typeName(component *repository.Component, schemaType reflect.Type, defaultValue string) string {
	types := component.TypeRegistry()
	aType := types.Info(schemaType)
	if aType != nil {
		return aType.Name
	}
	return defaultValue
}

func (g *generator) getViewSchemas(aView *view.View) []*schemaNamed {
	schemas := []*schemaNamed{
		{
			schema:       aView.Schema,
			defaultName:  aView.Name,
			resultSchema: true,
		},
	}

	g.addSchemaParam(&schemas, aView.Selector.CriteriaParameter)
	g.addSchemaParam(&schemas, aView.Selector.OrderByParameter)
	g.addSchemaParam(&schemas, aView.Selector.OffsetParameter)
	g.addSchemaParam(&schemas, aView.Selector.LimitParameter)
	for _, parameter := range aView.Template.Parameters {
		g.addSchemaParam(&schemas, parameter)
	}
	return schemas
}

func (g *generator) addSchemaParam(schemas *[]*schemaNamed, param *state.Parameter) {
	if param == nil {
		return
	}
	*schemas = append(*schemas, &schemaNamed{schema: param.Schema, defaultName: param.Name})
}

func (g *generator) getOrGenerateSchema(component *repository.Component, rType reflect.Type, formatFieldName bool, description, schemaName string) (*openapi3.Schema, error) {
	if schemaName == "" {
		return g.generateSchema(component, rType, "", formatFieldName, description, nil, "")
	}

	schema, ok := g._schemasIndex[schemaName]
	if !ok {
		generatedSchema, err := g.generateSchema(component, rType, "", formatFieldName, description, nil, "")
		if err != nil {
			return nil, err
		}
		g._schemasIndex[schemaName] = generatedSchema
		return generatedSchema, err
	}

	if schema != nil {
		originalSchema := *schema
		g.commonSchemas[schemaName] = &originalSchema
		*schema = openapi3.Schema{Ref: "#/components/schemas/" + schemaName}
		g._schemasIndex[schemaName] = nil
	}

	return &openapi3.Schema{Ref: "#/components/schemas/" + schemaName}, nil
}

func (g *generator) generateSchema(component *repository.Component, rType reflect.Type, dateFormat string, formatFieldName bool, description string, tag *json.DefaultTag, path string) (*openapi3.Schema, error) {
	schema := &openapi3.Schema{
		Description: description,
	}
	if tag != nil {
		schema.Nullable = tag.IsNullable()
	}

	if err := g.addToSchema(schema, component, rType, dateFormat, formatFieldName, tag, path); err != nil {
		return nil, err
	}

	return schema, nil
}

func (g *generator) addToSchema(schema *openapi3.Schema, component *repository.Component, rType reflect.Type, dateFormat string, isOutputSchema bool, tag *json.DefaultTag, path string) error {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	switch rType.Kind() {
	case reflect.Slice:
		var err error
		schema.Items, err = g.generateSchema(component, rType.Elem(), dateFormat, isOutputSchema, "", nil, path)
		if err != nil {
			return err
		}
		schema.Type = arrayOutput
	case reflect.Struct:
		if rType == timeType {
			schema.Type = stringOutput
			if dateFormat == "" {
				dateFormat = time.RFC3339
			}
			schema.Format = dateFormat
			break
		}

		schema.Properties = openapi3.Schemas{}
		schema.Type = objectOutput
		numField := rType.NumField()
		for i := 0; i < numField; i++ {
			aField := rType.Field(i)
			if aField.PkgPath != "" {
				continue
			}

			defaultTag, err := json.NewDefaultTag(aField)
			if err != nil {
				return err
			}

			aTag, err := format2.Parse(aField.Tag, "json", "default")
			if err != nil {
				return err
			}

			if aTag.Ignore {
				continue
			}

			if defaultTag.Embedded {
				schema.AdditionalPropertiesAllowed = setter.BoolPtr(true)
				continue
			}

			fieldPath := aField.Name
			if path != "" {
				fieldPath = path + "." + fieldPath
			}

			if _, ok := component.Output.Excluded()[fieldPath]; ok {
				continue
			}

			if aField.Anonymous {
				if err := g.addToSchema(schema, component, aField.Type, dateFormat, isOutputSchema, tag, fieldPath); err != nil {
					return err
				}
				continue
			}

			fieldName := aField.Name
			if defaultTag.IgnoreCaseFormatter {
				fieldName = aField.Name
			} else if isOutputSchema {
				fieldName = text.CaseFormatUpperCamel.Format(aField.Name, component.Output.CaseFormat)
			}

			schema.Properties[fieldName], err = g.generateSchema(component, aField.Type, defaultTag.Format, isOutputSchema, "", defaultTag, fieldPath)
			if err != nil {
				return err
			}

			if defaultTag.IsRequired() {
				schema.Required = append(schema.Required, fieldName)
			}
		}
	default:
		if rType.Kind() == reflect.Interface {
			schema.AnyOf = openapi3.SchemaList{
				{
					Type: stringOutput,
				},
				{
					Type: objectOutput,
				},
				{
					Type: arrayOutput,
				},
				{
					Type: numberOutput,
				},
				{
					Type: booleanOutput,
				},
			}
			break
		}
		var err error
		schema.Type, schema.Format, err = g.toOpenApiType(rType)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *generator) toOpenApiType(rType reflect.Type) (string, string, error) {
	switch rType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
		return integerOutput, int64Format, nil
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return integerOutput, int32Format, nil
	case reflect.Float64, reflect.Float32:
		return numberOutput, doubleFormat, nil
	case reflect.Bool:
		return booleanOutput, empty, nil
	case reflect.String:
		return stringOutput, empty, nil
	}

	return empty, empty, fmt.Errorf("unsupported openapi3 type %v", rType.String())
}

func (g *generator) generatePaths(ctx context.Context, components *repository.Service, providers []*repository.Provider) (openapi3.Paths, error) {
	paths := openapi3.Paths{}

	for _, provider := range providers {

		component, err := provider.Component(ctx)
		if err != nil {
			return nil, err
		}

		pathItem := &openapi3.PathItem{}
		operation, err := g.generateOperation(ctx, components, component, component.Method)
		if err != nil {
			return nil, err
		}

		switch component.Method {
		case http.MethodGet:
			pathItem.Get = operation
		case http.MethodPost:
			pathItem.Post = operation
		case http.MethodDelete:
			pathItem.Delete = operation
		case http.MethodPut:
			pathItem.Put = operation
		}

		paths[component.URI] = pathItem
	}
	return paths, nil
}

func (g *generator) generateOperation(ctx context.Context, components *repository.Service, component *repository.Component, method string) (*openapi3.Operation, error) {
	body, err := g.requestBody(component, method)
	if err != nil {
		return nil, err
	}

	parameters, err := g.getAllViewsParameters(component, component.View)
	if err != nil {
		return nil, err
	}

	if err := g.forEachParam(component.Output.Type.Parameters, func(parameter *state.Parameter) (bool, error) {
		if parameter.In.Kind == state.KindComponent {
			method, URI := shared.ExtractPath(parameter.In.Name)
			provider, err := components.Registry().LookupProvider(ctx, &contract.Path{
				URI:    URI,
				Method: method,
			})

			if err != nil {
				return false, err
			}

			paramComponent, err := provider.Component(ctx)
			if err != nil {
				return false, err
			}

			viewsParameters, err := g.getAllViewsParameters(paramComponent, paramComponent.View)
			if err != nil {
				return false, err
			}

			parameters = append(parameters, viewsParameters...)
		}

		return true, nil
	}); err != nil {
		return nil, err
	}

	responses, err := g.responses(component, method)
	if err != nil {
		return nil, err
	}

	operation := &openapi3.Operation{
		Extension:   nil,
		Tags:        nil,
		Summary:     "",
		Description: "",
		OperationID: "",
		Parameters:  dedupe(parameters),
		RequestBody: body,
		Responses:   responses,
	}

	return operation, nil
}

func dedupe(parameters []*openapi3.Parameter) openapi3.Parameters {
	index := map[paramLocation]bool{}
	result := []*openapi3.Parameter{}

	for _, parameter := range parameters {
		aKey := paramLocation{
			name: parameter.Name,
			in:   parameter.In,
		}

		if index[aKey] {
			continue
		}

		index[aKey] = true
		result = append(result, parameter)
	}

	return result
}

func (g *generator) forEachParam(parameters state.Parameters, iterator func(parameter *state.Parameter) (bool, error)) error {
	for _, parameter := range parameters {
		next, err := iterator(parameter)
		if err != nil {
			return err
		}

		if !next {
			continue
		}

		if err = g.forEachParam(parameter.Group, iterator); err != nil {
			return err
		}

		if err = g.forEachParam(parameter.Repeated, iterator); err != nil {
			return err
		}
	}

	return nil
}

func (g *generator) viewParameters(aView *view.View, component *repository.Component) ([]*openapi3.Parameter, error) {
	parameters := make([]*openapi3.Parameter, 0)
	for _, param := range aView.Template.Parameters {
		converted, ok, err := g.convertParam(component, param, "")
		if err != nil {
			return nil, err
		}

		if !ok {
			continue
		}
		parameters = append(parameters, converted...)
	}

	if err := g.appendBuiltInParam(&parameters, component, aView.Selector.CriteriaParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, component, aView.Selector.LimitParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, component, aView.Selector.OffsetParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, component, aView.Selector.PageParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, component, aView.Selector.OrderByParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, component, aView.Selector.FieldsParameter); err != nil {
		return nil, err
	}

	return parameters, nil
}

func (g *generator) appendBuiltInParam(params *[]*openapi3.Parameter, component *repository.Component, param *state.Parameter) error {
	if param == nil {
		return nil
	}

	converted, ok, err := g.convertParam(component, param, param.Description)
	if err != nil {
		return err
	}

	if ok {
		*params = append(*params, converted...)
	}
	return nil
}

func (g *generator) convertParam(component *repository.Component, param *state.Parameter, description string) ([]*openapi3.Parameter, bool, error) {
	if param.In.Kind == state.KindParam {
		return g.convertParam(component, param.Parent(), description)
	}

	if param.In.Kind == state.KindObject {
		var result []*openapi3.Parameter
		for _, parameter := range param.Group {
			convertParam, ok, err := g.convertParam(component, parameter, description)
			if err != nil {
				return nil, false, err
			}

			if ok {
				result = append(result, convertParam...)
			}
		}

		return result, true, nil
	}

	if !IsHttpParamKind(param.In.Kind) {
		return nil, false, nil
	}

	cachedParam, ok := g._parametersIndex[shared.FirstNotEmpty(param.In.Name, param.Name)]
	if ok {
		if cachedParam != nil {
			original := *cachedParam
			g.commonParameters[param.Name] = &original
			*cachedParam = openapi3.Parameter{Ref: "#/components/parameters/" + param.Name}
			g._parametersIndex[param.Name] = nil
		}

		return []*openapi3.Parameter{{Ref: "#/components/parameters/" + param.Name}}, true, nil
	}

	schema, err := g.getOrGenerateSchema(component, param.Schema.Type(), false, "Parameter "+param.Name+" schema", g.typeName(component, param.Schema.Type(), param.Name))
	if err != nil {
		return nil, false, err
	}

	if description == "" {
		description = param.Description
	}

	if description == "" {
		description = fmt.Sprintf("Parameter %v, Located in %v with name %v", param.Name, param.In.Kind, param.In.Name)
	}

	convertedParam := &openapi3.Parameter{
		Name:        shared.FirstNotEmpty(param.In.Name, param.Name),
		In:          string(param.In.Kind),
		Description: description,
		Style:       param.Style,
		Required:    param.Required != nil && *param.Required,
		Schema:      schema,
	}

	g._parametersIndex[param.Name] = convertedParam
	return []*openapi3.Parameter{convertedParam}, true, nil
}

func IsHttpParamKind(kind state.Kind) bool {
	switch kind {
	case state.KindPath, state.KindQuery, state.KindHeader, state.KindCookie:
		return true
	}

	return false
}

func (g *generator) getViewPrefixes(mainView bool, route *Route, aView *view.View) []string {
	var prefixes []string
	nsViews := view.IndexViews(route.View)
	nsView := nsViews.ByName(aView.Name)

	if nsView != nil || mainView {
		prefixes = append(prefixes, nsView.Namespaces...)
	}
	return prefixes
}

func (g *generator) getAllViewsParameters(component *repository.Component, aView *view.View) ([]*openapi3.Parameter, error) {
	params, err := g.viewParameters(aView, component)
	if err != nil {
		return nil, err
	}

	for _, relation := range aView.With {
		relationParams, err := g.getAllViewsParameters(component, &relation.Of.View)
		if err != nil {
			return nil, err
		}
		params = append(params, relationParams...)
	}

	return params, nil
}

func (g *generator) indexParameters(parameters []*openapi3.Parameter) openapi3.ParametersMap {
	result := openapi3.ParametersMap{}
	for i := range parameters {
		result[parameters[i].Name] = parameters[i]
	}
	return result
}

func (g *generator) requestBody(component *repository.Component, method string) (*openapi3.RequestBody, error) {
	if method != http.MethodPost || component.BodyType() == nil {
		return nil, nil
	}

	typeName := g.typeName(component, component.BodyType(), "RequestBody")
	requestBodySchema, err := g.getOrGenerateSchema(component, component.BodyType(), false, typeName, "Request body schema")
	if err != nil {
		return nil, err
	}

	return &openapi3.RequestBody{
		Required: true,
		Content: map[string]*openapi3.MediaType{
			applicationJson: {
				Schema: requestBodySchema,
			},
		},
	}, nil
}

func (g *generator) responses(component *repository.Component, method string) (openapi3.Responses, error) {
	if method == http.MethodOptions {
		return nil, nil
	}

	responseType := component.OutputType()
	if responseType == nil {
		return openapi3.Responses{}, nil
	}

	schemaName := g.typeName(component, responseType, component.View.Name)
	successSchema, err := g.getOrGenerateSchema(component, responseType, true, successSchemaDescription, schemaName)
	if err != nil {
		return nil, err
	}

	responses := openapi3.Responses{}
	responses[200] = &openapi3.Response{
		Description: stringPtr("Success response"),
		Content: map[string]*openapi3.MediaType{
			applicationJson: {
				Schema: successSchema,
			},
		},
	}

	errorSchema, err := g.getOrGenerateSchema(component, errorType, false, errorSchemaDescription, "")
	if err != nil {
		return nil, err
	}

	responses["Default"] = &openapi3.Response{
		Description: stringPtr("Error response. The view and param may be empty, but one of the message or object should be specified"),
		Content: map[string]*openapi3.MediaType{
			applicationJson: {
				Schema: errorSchema,
			},
		}}

	return responses, nil
}

func stringPtr(value string) *string {
	return &value
}
