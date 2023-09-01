package router

import (
	"fmt"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/router/openapi3"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
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
)

func (g *generator) GenerateSpec(info openapi3.Info, route ...*Route) (*openapi3.OpenAPI, error) {
	components := g.generateComponents()

	paths, err := g.generatePaths(route)
	if err != nil {
		return nil, err
	}

	components.Schemas = g.commonSchemas
	components.Parameters = g.commonParameters

	return &openapi3.OpenAPI{
		OpenAPI:      "3.1.0",
		Components:   *components,
		Info:         &info,
		Paths:        paths,
		Security:     nil,
		Servers:      nil,
		Tags:         nil,
		ExternalDocs: nil,
	}, nil
}

func GenerateOpenAPI3Spec(info openapi3.Info, routes ...*Route) (*openapi3.OpenAPI, error) {
	return (&generator{
		_schemasIndex:    map[string]*openapi3.Schema{},
		commonSchemas:    map[string]*openapi3.Schema{},
		commonParameters: map[string]*openapi3.Parameter{},
		_parametersIndex: map[string]*openapi3.Parameter{},
	}).GenerateSpec(info, routes...)
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

func (g *generator) generateSchemas(route *Route) (openapi3.Schemas, error) {
	result := openapi3.Schemas{}

	schemas := g.getViewSchemas(route.View)
	var err error
	for _, schemaWrapper := range schemas {
		typeName := g.typeName(route, schemaWrapper.schema.Type(), schemaWrapper.defaultName)
		kind := schemaWrapper.schema.Type().Kind()
		if kind != reflect.Slice && kind != reflect.Struct {
			continue
		}

		result[schemaWrapper.defaultName], err = g.getOrGenerateSchema(route, schemaWrapper.schema.Type(), !schemaWrapper.resultSchema, typeName, g.typeName(route, schemaWrapper.schema.Type(), schemaWrapper.defaultName))
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (g *generator) typeName(route *Route, schemaType reflect.Type, defaultValue string) string {
	typeName, ok := route._resource.TypeName(schemaType)
	if ok {
		return typeName
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

func (g *generator) getOrGenerateSchema(route *Route, rType reflect.Type, formatFieldName bool, description, schemaName string) (*openapi3.Schema, error) {
	if schemaName == "" {
		return g.generateSchema(route, rType, "", formatFieldName, description, nil, "")
	}

	schema, ok := g._schemasIndex[schemaName]
	if !ok {
		generatedSchema, err := g.generateSchema(route, rType, "", formatFieldName, description, nil, "")
		if err != nil {
			return nil, err
		}
		g._schemasIndex[schemaName] = generatedSchema
		return generatedSchema, err
	}

	if schema != nil {
		originalSchema := *schema
		g.commonSchemas[schemaName] = &originalSchema
		*schema = openapi3.Schema{Ref: "#/components/schema/" + schemaName}
		g._schemasIndex[schemaName] = nil
	}

	return &openapi3.Schema{Ref: "#/components/schema/" + schemaName}, nil
}

func (g *generator) generateSchema(route *Route, rType reflect.Type, dateFormat string, formatFieldName bool, description string, tag *json.DefaultTag, path string) (*openapi3.Schema, error) {
	schema := &openapi3.Schema{
		Description: description,
	}
	if tag != nil {
		schema.Nullable = tag.IsNullable()
	}

	if err := g.addToSchema(schema, route, rType, dateFormat, formatFieldName, tag, path); err != nil {
		return nil, err
	}

	return schema, nil
}

func (g *generator) addToSchema(schema *openapi3.Schema, route *Route, rType reflect.Type, dateFormat string, isOutputSchema bool, tag *json.DefaultTag, path string) error {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	switch rType.Kind() {
	case reflect.Slice:
		var err error
		schema.Items, err = g.generateSchema(route, rType.Elem(), dateFormat, isOutputSchema, "", nil, path)
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

			jsonTag := json.Parse(aField.Tag.Get("json"))
			if jsonTag.FieldName == "-" {
				continue
			}

			fieldPath := aField.Name
			if path != "" {
				fieldPath = path + "." + fieldPath
			}

			if _, ok := route.Output.Excluded()[fieldPath]; ok {
				continue
			}

			if aField.Anonymous {
				if err := g.addToSchema(schema, route, aField.Type, dateFormat, isOutputSchema, tag, fieldPath); err != nil {
					return err
				}
				continue
			}

			defaultTag, err := json.NewDefaultTag(aField)
			if err != nil {
				return err
			}

			fieldName := aField.Name
			if defaultTag.IgnoreCaseFormatter {
				fieldName = aField.Name
			} else if isOutputSchema {
				fieldName = format.CaseUpperCamel.Format(aField.Name, *route.Output.FormatCase())
			}

			schema.Properties[fieldName], err = g.generateSchema(route, aField.Type, defaultTag.Format, isOutputSchema, "", defaultTag, fieldPath)
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

func (g *generator) generatePaths(routes []*Route) (openapi3.Paths, error) {
	paths := openapi3.Paths{}

	for _, route := range routes {
		pathItem := &openapi3.PathItem{}
		operation, err := g.generateOperation(route, route.Method)
		if err != nil {
			return nil, err
		}

		switch route.Method {
		case http.MethodGet:
			pathItem.Get = operation
		case http.MethodPost:
			pathItem.Post = operation
		}

		paths[route.URI] = pathItem
	}
	return paths, nil
}

func (g *generator) generateOperation(route *Route, method string) (*openapi3.Operation, error) {
	body, err := g.requestBody(route, method)
	if err != nil {
		return nil, err
	}

	parameters, err := g.getAllViewsParameters(route, route.View)
	if err != nil {
		return nil, err
	}

	responses, err := g.responses(route, method)
	if err != nil {
		return nil, err
	}

	operation := &openapi3.Operation{
		Extension:   nil,
		Tags:        nil,
		Summary:     "",
		Description: "",
		OperationID: "",
		Parameters:  parameters,
		RequestBody: body,
		Responses:   responses,
	}

	return operation, nil
}

func (g *generator) viewParameters(aView *view.View, route *Route) ([]*openapi3.Parameter, error) {
	parameters := make([]*openapi3.Parameter, 0)
	for _, param := range aView.Template.Parameters {
		converted, ok, err := g.convertParam(route, param, "")
		if err != nil {
			return nil, err
		}

		if !ok {
			continue
		}
		parameters = append(parameters, converted)
	}

	if err := g.appendBuiltInParam(&parameters, route, aView.Selector.CriteriaParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, route, aView.Selector.LimitParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, route, aView.Selector.OffsetParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, route, aView.Selector.PageParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, route, aView.Selector.OrderByParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, route, aView.Selector.FieldsParameter); err != nil {
		return nil, err
	}

	return parameters, nil
}

func (g *generator) appendBuiltInParam(params *[]*openapi3.Parameter, route *Route, param *state.Parameter) error {
	if param == nil {
		return nil
	}

	converted, ok, err := g.convertParam(route, param, param.Description)
	if err != nil {
		return err
	}

	if ok {
		*params = append(*params, converted)
	}
	return nil
}

func (g *generator) convertParam(route *Route, param *state.Parameter, description string) (*openapi3.Parameter, bool, error) {
	if param.In.Kind == state.KindDataView || param.In.Kind == state.KindRequestBody || param.In.Kind == state.KindEnvironment || param.In.Kind == state.KindLiteral {
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

		return &openapi3.Parameter{Ref: "#/components/parameters/" + param.Name}, true, nil
	}

	schema, err := g.getOrGenerateSchema(route, param.Schema.Type(), false, "Parameter "+param.Name+" schema", g.typeName(route, param.Schema.Type(), param.Name))
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
	return convertedParam, true, nil
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

func (g *generator) getAllViewsParameters(route *Route, aView *view.View) ([]*openapi3.Parameter, error) {
	params, err := g.viewParameters(aView, route)
	if err != nil {
		return nil, err
	}

	for _, relation := range aView.With {
		relationParams, err := g.getAllViewsParameters(route, &relation.Of.View)
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

func (g *generator) requestBody(route *Route, method string) (*openapi3.RequestBody, error) {
	if method != http.MethodPost || route.InputType() == nil {
		return nil, nil
	}

	typeName := g.typeName(route, route.InputType(), "RequestBody")
	requestBodySchema, err := g.getOrGenerateSchema(route, route.InputType(), false, typeName, "Request body schema")
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

func (g *generator) responses(route *Route, method string) (openapi3.Responses, error) {
	if method == http.MethodOptions {
		return nil, nil
	}

	responseType := route.OutputType()
	schemaName := g.typeName(route, responseType, route.View.Name)
	successSchema, err := g.getOrGenerateSchema(route, responseType, true, successSchemaDescription, schemaName)
	if err != nil {
		return nil, err
	}

	responses := openapi3.Responses{}
	responses["200"] = &openapi3.Response{
		Description: stringPtr("Success response"),
		Content: map[string]*openapi3.MediaType{
			applicationJson: {
				Schema: successSchema,
			},
		},
	}

	errorSchema, err := g.getOrGenerateSchema(route, errorType, false, errorSchemaDescription, "")
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
