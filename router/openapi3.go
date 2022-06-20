package router

import (
	"fmt"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/router/openapi3"
	"github.com/viant/datly/view"
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
	errorType = reflect.TypeOf(Error{})
)

type (
	generator struct {
		commonSchemas openapi3.Schemas
		_schemasIndex map[string]*openapi3.Schema
	}

	schemaNamed struct {
		schema       *view.Schema
		defaultName  string
		resultSchema bool
	}
)

func (g *generator) GenerateSpec(route *Route) (*openapi3.OpenAPI, error) {
	components, err := g.generateComponents(route)
	if err != nil {
		return nil, err
	}

	paths, err := g.generatePaths(route)
	if err != nil {
		return nil, err
	}

	return &openapi3.OpenAPI{
		OpenAPI:      "3.1.0",
		Components:   *components,
		Info:         &route.Info,
		Paths:        paths,
		Security:     nil,
		Servers:      nil,
		Tags:         nil,
		ExternalDocs: nil,
	}, nil
}

func GenerateOpenAPI3Spec(route *Route) (*openapi3.OpenAPI, error) {
	return (&generator{
		_schemasIndex: map[string]*openapi3.Schema{},
	}).GenerateSpec(route)
}

func (g *generator) generateComponents(route *Route) (*openapi3.Components, error) {
	schemas, err := g.generateSchemas(route)
	if err != nil {
		return nil, err
	}

	parameters, err := g.getAllViewsParameters(route, route.View, true)
	if err != nil {
		return nil, err
	}

	return &openapi3.Components{
		Extension: nil,
		//TODO: all schemas or just resource schema. If resource schema then they may differ if case format is specified.
		Schemas: schemas,
		//TODO: view params or resource params
		Parameters:      g.indexParameters(parameters),
		Headers:         nil,
		RequestBodies:   nil,
		Responses:       nil,
		SecuritySchemes: nil,
		Examples:        nil,
		Links:           nil,
		Callbacks:       nil,
	}, nil
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

		result[schemaWrapper.defaultName], err = g.generateSchema(route, schemaWrapper.schema.Type(), "", !schemaWrapper.resultSchema, typeName, nil, "")
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

	g.addSchemaParam(&schemas, aView.Selector.CriteriaParam)
	g.addSchemaParam(&schemas, aView.Selector.OrderByParam)
	g.addSchemaParam(&schemas, aView.Selector.OffsetParam)
	g.addSchemaParam(&schemas, aView.Selector.LimitParam)
	for _, parameter := range aView.Template.Parameters {
		g.addSchemaParam(&schemas, parameter)
	}
	return schemas
}

func (g *generator) addSchemaParam(schemas *[]*schemaNamed, param *view.Parameter) {
	if param == nil {
		return
	}
	*schemas = append(*schemas, &schemaNamed{schema: param.Schema, defaultName: param.Name})
}

func (g *generator) generateSchema(route *Route, rType reflect.Type, dateFormat string, formatFieldName bool, path string, tag *json.DefaultTag, description string) (*openapi3.Schema, error) {
	schema := &openapi3.Schema{
		Description: description,
	}
	if tag != nil {
		schema.Nullable = tag.IsNullable()
	}

	if err := g.addToSchema(schema, route, rType, dateFormat, formatFieldName, tag); err != nil {
		return nil, err
	}

	return schema, nil
}

func (g *generator) addToSchema(schema *openapi3.Schema, route *Route, rType reflect.Type, dateFormat string, formatFieldName bool, tag *json.DefaultTag) error {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	switch rType.Kind() {
	case reflect.Slice:
		var err error
		schema.Items, err = g.generateSchema(route, rType, dateFormat, formatFieldName, "", nil, "")
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

			if aField.Anonymous {
				if err := g.addToSchema(schema, route, aField.Type, dateFormat, formatFieldName, tag); err != nil {
					return err
				}
				continue
			}

			defaultTag, err := json.NewDefaultTag(aField)
			if err != nil {
				return err
			}

			fieldName := aField.Name
			if formatFieldName {
				fieldName = format.CaseUpperCamel.Format(aField.Name, route._caser)
			}

			schema.Properties[fieldName], err = g.generateSchema(route, aField.Type, defaultTag.Format, formatFieldName, fieldName, defaultTag, "")
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

func (g *generator) generatePaths(route *Route) (openapi3.Paths, error) {
	paths := openapi3.Paths{}

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
	return paths, nil
}

func (g *generator) generateOperation(route *Route, method string) (*openapi3.Operation, error) {
	body, err := g.requestBody(route, method)
	if err != nil {
		return nil, err
	}

	parameters, err := g.getAllViewsParameters(route, route.View, true)
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

func (g *generator) viewParameters(aView *view.View, route *Route, mainView bool) ([]*openapi3.Parameter, error) {
	parameters := make([]*openapi3.Parameter, 0)
	for _, param := range aView.Template.Parameters {
		if param.In.Kind == view.DataViewKind {
			continue
		}

		converted, err := g.convertParam(route, param, "")
		if err != nil {
			return nil, err
		}
		parameters = append(parameters, converted)
	}

	if err := g.appendBuiltInParam(&parameters, route, aView, aView.Selector.CriteriaParam, Criteria, mainView); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, route, aView, aView.Selector.LimitParam, Limit, mainView); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, route, aView, aView.Selector.OffsetParam, Offset, mainView); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, route, aView, aView.Selector.OrderByParam, OrderBy, mainView); err != nil {
		return nil, err
	}

	if err := g.appendBuiltInParam(&parameters, route, aView, aView.Selector.FieldsParam, Fields, mainView); err != nil {
		return nil, err
	}

	return parameters, nil
}

func (g *generator) appendBuiltInParam(params *[]*openapi3.Parameter, route *Route, aView *view.View, param *view.Parameter, paramName QueryParam, mainView bool) error {
	switch paramName {
	case Criteria:
		if !aView.CanUseSelectorCriteria() {
			return nil
		}
	case Limit:
		if !aView.CanUseSelectorLimit() {
			return nil
		}
	case OrderBy:
		if !aView.CanUseSelectorOrderBy() {
			return nil
		}
	case Offset:
		if !aView.CanUseSelectorOffset() {
			return nil
		}
	}

	if param == nil {
		if err := g.appendDefaultParam(params, route, aView, paramName, mainView); err != nil {
			return err
		}
	} else {
		if param.In.Kind != view.DataViewKind {
			converted, err := g.convertParam(route, param, paramName.Description(aView.Name))
			if err != nil {
				return err
			}
			*params = append(*params, converted)
		}
	}
	return nil
}

func (g *generator) convertParam(route *Route, param *view.Parameter, description string) (*openapi3.Parameter, error) {
	schema, err := g.generateSchema(route, param.Schema.Type(), "", false, g.typeName(route, param.Schema.Type(), param.Name), nil, "Parameter "+param.Name+" schema")
	if err != nil {
		return nil, err
	}

	if description == "" {
		description = param.Description
	}

	elems := &openapi3.Parameter{
		Name:        param.Name,
		In:          string(param.In.Kind),
		Description: description,
		Style:       param.Style,
		Required:    param.Required == nil || *param.Required,
		Schema:      schema,
	}
	return elems, nil
}

func (g *generator) appendDefaultParam(params *[]*openapi3.Parameter, route *Route, aView *view.View, paramName QueryParam, mainView bool) error {
	paramType := paramName.ParamType()
	prefixes := g.getViewPrefixes(mainView, route, aView)

	for _, prefix := range prefixes {
		schema, err := g.generateSchema(route, paramType, "", false, "", nil, "")
		if err != nil {
			return err
		}
		*params = append(*params, &openapi3.Parameter{
			Name:        prefix + string(paramName),
			In:          string(view.QueryKind),
			Description: paramName.Description(aView.Name),
			Style:       "",
			Schema:      schema,
			Example:     nil,
			Examples:    nil,
			Content:     nil,
		})
	}
	return nil
}

func (g *generator) getViewPrefixes(mainView bool, route *Route, aView *view.View) []string {
	var prefixes []string
	if mainView {
		prefixes = append(prefixes, "")
	}

	prefix, ok := route.PrefixByView(aView)
	if ok {
		prefixes = append(prefixes, prefix)
	}
	return prefixes
}

func (g *generator) getAllViewsParameters(route *Route, aView *view.View, mainView bool) ([]*openapi3.Parameter, error) {
	params, err := g.viewParameters(aView, route, mainView)
	if err != nil {
		return nil, err
	}

	for _, relation := range aView.With {
		relationParams, err := g.getAllViewsParameters(route, &relation.Of.View, false)
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
	if method != http.MethodPost || route._requestBodyParam == nil {
		return nil, nil
	}

	typeName := g.typeName(route, route._requestBodyParam.Schema.Type(), "RequestBody")
	requestBodySchema, err := g.generateSchema(route, route._requestBodyParam.Schema.Type(), "", false, typeName, nil, "Request body schema")
	if err != nil {
		return nil, err
	}

	return &openapi3.RequestBody{
		Required: route._requestBodyParam.IsRequired(),
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

	responseType := route.responseType()
	schemaName := g.typeName(route, responseType, route.View.Name)
	successSchema, err := g.generateSchema(route, responseType, "", true, schemaName, nil, successSchemaDescription)
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

	errorSchema, err := g.generateSchema(route, errorType, "", false, "", nil, errorSchemaDescription)
	if err != nil {
		return nil, err
	}

	responses["401"] = &openapi3.Response{
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
