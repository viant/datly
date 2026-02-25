package openapi

import (
	"context"
	"fmt"
	openapi "github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/xdatly/handler/response"
	"net/http"
	"reflect"
)

const (
	errorSchemaDescription = "Error object schema"

	ApplicationJson = "application/json"

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
	errorType = reflect.TypeOf(response.Error{})
)

type (
	generator struct {
		_schemasIndex    map[string]*openapi.Schema
		commonParameters openapi.ParametersMap
		_parametersIndex map[string]*openapi.Parameter
	}

	paramLocation struct {
		name string
		in   string
	}
)

func (g *generator) GenerateSpec(ctx context.Context, repoComponents *repository.Service, info openapi.Info, providers ...*repository.Provider) (*openapi.OpenAPI, error) {
	components := &openapi.Components{}

	schemas, paths, err := g.generatePaths(ctx, repoComponents, providers)
	if err != nil {
		return nil, err
	}

	components.Schemas = schemas.generatedSchemas
	components.Parameters = g.commonParameters

	return &openapi.OpenAPI{
		OpenAPI:    "3.0.1",
		Components: *components,
		Info:       &info,
		Paths:      paths,
	}, nil
}

func GenerateOpenAPI3Spec(ctx context.Context, components *repository.Service, info openapi.Info, providers ...*repository.Provider) (*openapi.OpenAPI, error) {
	return (&generator{
		_schemasIndex:    map[string]*openapi.Schema{},
		commonParameters: map[string]*openapi.Parameter{},
		_parametersIndex: map[string]*openapi.Parameter{},
	}).GenerateSpec(ctx, components, info, providers...)
}

func dedupe(parameters []*openapi.Parameter) openapi.Parameters {
	index := map[paramLocation]bool{}
	var result []*openapi.Parameter

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

		if err = g.forEachParam(parameter.Object, iterator); err != nil {
			return err
		}

		if err = g.forEachParam(parameter.Repeated, iterator); err != nil {
			return err
		}
	}

	return nil
}

func (g *generator) viewParameters(ctx context.Context, aView *view.View, component *ComponentSchema) ([]*openapi.Parameter, error) {
	parameters := make([]*openapi.Parameter, 0)
	for _, param := range aView.Template.Parameters {
		converted, ok, err := g.convertParam(ctx, component, param, "")
		if err != nil {
			return nil, err
		}

		if !ok {
			continue
		}
		parameters = append(parameters, converted...)
	}
	if err := g.appendBuiltInParam(ctx, &parameters, component, aView.Selector.CriteriaParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(ctx, &parameters, component, aView.Selector.LimitParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(ctx, &parameters, component, aView.Selector.OffsetParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(ctx, &parameters, component, aView.Selector.PageParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(ctx, &parameters, component, aView.Selector.OrderByParameter); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(ctx, &parameters, component, aView.Selector.FieldsParameter); err != nil {
		return nil, err
	}

	return parameters, nil
}

func (g *generator) appendBuiltInParam(ctx context.Context, params *[]*openapi.Parameter, component *ComponentSchema, param *state.Parameter) error {
	if param == nil {
		return nil
	}

	converted, ok, err := g.convertParam(ctx, component, param, param.Description)
	if err != nil {
		return err
	}

	if ok {
		*params = append(*params, converted...)
	}
	return nil
}

func (g *generator) convertParam(ctx context.Context, component *ComponentSchema, param *state.Parameter, description string) ([]*openapi.Parameter, bool, error) {

	docs := component.component.Docs()
	parameters := docs.Parameters
	if len(parameters) > 0 && param.Description == "" {
		if desc, ok := parameters.ByName(param.Name); ok {
			param.Description = desc
			description = desc
		}
	}
	if param.In.Kind == state.KindParam {
		baseParam := component.component.LookupParameter(param.In.Name)
		return g.convertParam(ctx, component, baseParam, description)
	}

	if param.In.Kind == state.KindObject {
		var result []*openapi.Parameter
		for _, parameter := range param.Object {
			convertParam, ok, err := g.convertParam(ctx, component, parameter, description)
			if err != nil {
				return nil, false, err
			}

			if ok {
				result = append(result, convertParam...)
			}
		}

		return result, true, nil
	}

	if !param.IsHTTPParameter() {
		return nil, false, nil
	}

	cachedParam, ok := g._parametersIndex[shared.FirstNotEmpty(param.In.Name, param.Name)]
	if ok {
		if cachedParam != nil {
			original := *cachedParam
			g.commonParameters[param.Name] = &original
			*cachedParam = openapi.Parameter{Ref: "#/components/parameters/" + param.Name}
			g._parametersIndex[param.Name] = nil
		}

		return []*openapi.Parameter{{Ref: "#/components/parameters/" + param.Name}}, true, nil
	}

	table := ""
	if param.Tag != "" {
		if datlyTags, _ := tags.Parse(reflect.StructTag(param.Tag), nil, tags.ViewTag); datlyTags != nil && datlyTags.View != nil {
			table = datlyTags.View.Table
		}

	}
	schema, err := component.GenerateSchema(ctx, component.SchemaWithTag(param.Name, param.Schema.Type(), "Parameter "+param.Name+" schema", component.component.IOConfig(), Tag{
		Format:     param.DateFormat,
		IsNullable: !param.IsRequired(),
		Table:      table,
	}))

	if err != nil {
		return nil, false, err
	}

	if description == "" {
		description = param.Description
	}

	if description == "" {
		description = fmt.Sprintf("Parameter %v, Located in %v with name %v", param.Name, param.In.Kind, param.In.Name)
	}

	convertedParam := &openapi.Parameter{
		Name:        shared.FirstNotEmpty(param.In.Name, param.Name),
		In:          string(param.In.Kind),
		Description: description,
		Style:       param.Style,
		Required:    param.IsRequired(),
		Schema:      schema,
	}

	g._parametersIndex[param.Name] = convertedParam
	return []*openapi.Parameter{convertedParam}, true, nil
}

func (g *generator) getAllViewsParameters(ctx context.Context, component *ComponentSchema, aView *view.View) ([]*openapi.Parameter, error) {
	params, err := g.viewParameters(ctx, aView, component)
	if err != nil {
		return nil, err
	}

	for _, relation := range aView.With {
		relationParams, err := g.getAllViewsParameters(ctx, component, &relation.Of.View)
		if err != nil {
			return nil, err
		}
		params = append(params, relationParams...)
	}

	return params, nil
}

func (g *generator) indexParameters(parameters []*openapi.Parameter) openapi.ParametersMap {
	result := openapi.ParametersMap{}
	for i := range parameters {
		result[parameters[i].Name] = parameters[i]
	}
	return result
}

func (g *generator) requestBody(ctx context.Context, component *ComponentSchema) (*openapi.RequestBody, error) {
	if component.component.BodyType() == nil || component.component.Path.Method == "GET" {
		return nil, nil
	}
	bodySchema, err := component.RequestBody(ctx)
	if err != nil {
		return nil, err
	}

	requestBodySchema, err := component.GetOrGenerateSchema(ctx, bodySchema)
	if err != nil {
		return nil, err
	}

	return &openapi.RequestBody{
		Required: true,
		Content: map[string]*openapi.MediaType{
			ApplicationJson: {
				Schema: requestBodySchema,
			},
		},
	}, nil
}

func (g *generator) responses(ctx context.Context, component *ComponentSchema) (openapi.Responses, error) {
	method := component.component.Method
	if method == http.MethodOptions {
		return openapi.Responses{}, nil
	}

	responseSchema, err := component.ResponseBody(ctx)
	if err != nil {
		return nil, err
	}

	schema, err := component.GetOrGenerateSchema(ctx, responseSchema)
	if err != nil {
		return nil, err
	}

	responses := openapi.Responses{}
	openapi.SetResponse(responses, openapi.ResponseOK, &openapi.Response{
		Description: stringPtr("Success response"),
		Content: map[string]*openapi.MediaType{
			ApplicationJson: {
				Schema: schema,
			},
		},
	})

	errorSchema, err := component.GetOrGenerateSchema(ctx, component.ReflectSchema("ErrorResponse", errorType, errorSchemaDescription, component.component.IOConfig()))
	if err != nil {
		return nil, err
	}

	openapi.SetResponse(responses, openapi.ResponseDefault, &openapi.Response{
		Description: stringPtr("Error response. The view and param may be empty, but one of the message or object should be specified"),
		Content: map[string]*openapi.MediaType{
			ApplicationJson: {
				Schema: errorSchema,
			},
		}})

	return responses, nil
}

func stringPtr(value string) *string {
	return &value
}
