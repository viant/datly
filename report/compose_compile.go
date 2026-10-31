package report

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/report/cubecompose"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	"reflect"
	"strings"
)

// deriveCompose builds a distinct typed contract over the same source cube.
func (d *reportDeriver) deriveCompose(source Source, route *spec.Route, identity reportIdentity, metadata *metadata, sourcePlan *Plan) (*Derived, *x.Type, error) {
	config := metadata.settings.Compose.Normalize()
	if config == nil || !config.Enabled {
		return nil, nil, nil
	}
	identity.key.Name += "Compose"
	identity.route.Path += "/compose"
	identity.typeName = identity.key.Name + "Input"
	if err := d.reserve(identity); err != nil {
		return nil, nil, err
	}
	filterType, err := (&inputCompiler{metadata: metadata}).filterType()
	if err != nil {
		return nil, nil, err
	}
	frameType, err := (xshape.Runtime{}).Struct([]xshape.RuntimeField{
		inputField("Filters", filterType, "Typed source cube filters; omitted values may inherit"),
		inputField("InheritFrom", reflect.TypeFor[*int](), "One-based index of a preceding cube; inherit omitted filters only"),
		inputField("Align", reflect.TypeFor[string](), "Optional elapsed alignment interpreted by source predicates"),
	})
	if err != nil {
		return nil, nil, err
	}
	inputFields := []xshape.RuntimeField{
		inputField("Cubes", reflect.SliceOf(frameType), fmt.Sprintf("Ordered cube frames, minimum 1, maximum %d", config.MaxCubes)),
		{Name: "SQL", Type: reflect.TypeFor[string](), Tag: fieldTag("sql", "Validated wrapper SELECT; cubes[N-1] maps exactly to $CubeSQLN AS tN. Start FROM $CubeSQL1 AS t1 and join subsequent frames in order.")},
		inputField("ResultLimit", reflect.TypeFor[*int](), "Optional semantic ordered-prefix size, independent of transport LIMIT/OFFSET; requires one explicitly ordered flat cube selection"),
		inputField("Page", reflect.TypeFor[*cubecompose.Page](), "Optional transport window; explicit limit must be positive, offset nonnegative, and their sum within the native budget; omission preserves SQL LIMIT/OFFSET"),
	}
	var values *cubecompose.ValuesContract
	if config.ValuesType != "" {
		resolver, resolveErr := typecatalog.NewResolver(d.compiler.config.Types, d.compiler.config.Authority, (&inputCompiler{component: source.Component}).resolutionContext())
		if resolveErr != nil {
			return nil, nil, resolveErr
		}
		typ, resolveErr := (xshape.Runtime{Lookup: resolver.Type}).Type(config.ValuesType)
		if resolveErr != nil {
			return nil, nil, resolveErr
		}
		values, resolveErr = cubecompose.NewValuesContract(typ)
		if resolveErr != nil {
			return nil, nil, resolveErr
		}
		inputFields = append(inputFields, inputField("Values", reflect.TypeFor[json.RawMessage](), "Typed scalar wrapper values; supplied values must be non-null"))
	}
	inputType, err := (xshape.Runtime{}).Struct(inputFields)
	if err != nil {
		return nil, nil, err
	}
	descriptor, err := (xshape.Runtime{}).Synthetic(d.reportPackage(source.Component), identity.typeName, inputType)
	if err != nil {
		return nil, nil, err
	}
	rowType := dereference(source.OutputType)
	if rowType != nil && rowType.Kind() == reflect.Struct {
		bindings, err := dtag.NewBindingIndex(rowType)
		if err != nil {
			return nil, nil, err
		}
		for _, parameter := range source.Component.Parameters {
			if parameter != nil && parameter.Source.Kind == "output" && parameter.Source.Name == "view" {
				field, ok, err := bindings.Resolve(parameter)
				if err != nil {
					return nil, nil, err
				}
				if !ok {
					return nil, nil, fmt.Errorf("compose source output holder %s is missing", parameter.Name)
				}
				rowType = dereference(field.Type)
				break
			}
		}
	}
	if rowType != nil && rowType.Kind() == reflect.Slice {
		rowType = dereference(rowType.Elem())
	}
	if rowType == nil || rowType.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("compose source requires typed row output")
	}
	var fields []cubecompose.Field
	for _, section := range []struct {
		fields []field
		role   cubecompose.Role
	}{{metadata.dimensions, cubecompose.Dimension}, {metadata.measures, cubecompose.Measure}} {
		for _, column := range section.fields {
			// Resolve the original column identity before formatting it for the
			// cube selector: existing row fields may retain underscores.
			rowField, ok := typecatalog.FieldByName(rowType, column.publicName)
			if !ok {
				return nil, nil, fmt.Errorf("compose column %s requires a resolved row field", column.publicName)
			}
			fields = append(fields, cubecompose.Field{Name: column.sqlName, SQLName: column.sqlName, Type: rowField.Type, Role: section.role, Tag: rowField.Tag})
			if column.sqlName != column.name {
				fields = append(fields, cubecompose.Field{Name: column.name, SQLName: column.sqlName, Type: rowField.Type, Role: section.role, Tag: rowField.Tag})
			}
		}
	}
	catalog, err := cubecompose.NewCatalog(fields...)
	if err != nil {
		return nil, nil, err
	}
	cubesParam := bodyParam("Cubes", inputType)
	// Composition frames may inherit required filters from an earlier frame.
	// The source binding validates the effective filters after inheritance.
	cubesParam.WireSchemas = filterWireSchemas("Cubes.Filters", metadata.filters, false, filterType)
	params := []*spec.Parameter{cubesParam, bodyParam("SQL", inputType)}
	required := true
	for _, param := range params {
		param.Required = &required
	}
	params = append(params, bodyParam("ResultLimit", inputType), bodyParam("Page", inputType))
	if values != nil {
		parameter := bodyParam("Values", inputType)
		parameter.WireTypeExpr = values.WireType().String()
		params = append(params, parameter)
	}
	component := &spec.Component{
		Key: identity.key, Name: identity.key.Name, Description: strings.TrimSpace(source.Component.Description + " cube composition"),
		Settings: &spec.Settings{InputType: descriptor.Key()}, TypeContext: source.Component.TypeContext.Clone(), Parameters: params,
		Routes: []*spec.Route{{Method: identity.route.Method, Path: identity.route.Path, Internal: route.Internal, APIKeyHeader: route.APIKeyHeader, APIKeyValue: route.APIKeyValue}},
	}
	if source.Component.Settings != nil {
		// Preserve activation of the source's native presentation encoder.
		component.Settings.CaseFormat = source.Component.Settings.CaseFormat
	}
	if config.MCPTool == nil || *config.MCPTool {
		component.Routes[0].MCP = []*spec.MCPExposure{{Kind: spec.MCPExposureTool, CaseFormat: spec.DerivedMCPCaseFormat(route), Name: spec.DerivedMCPToolName(route, identity.key.Name, "CubeCompose"), Description: component.Description}}
	}
	// A separate source plan keeps composition from mutating ordinary cube input.
	plan := &Plan{target: sourcePlan.target, inputType: inputType, outputType: reflect.TypeFor[ComposeResponse]()}
	contract, ok := source.Input.ForRoute(sourcePlan.target.Route)
	if !ok {
		return nil, nil, fmt.Errorf("compose source route contract is missing")
	}
	handler := &composeHandler{inputType: inputType, source: sourcePlan, config: config, catalog: catalog, metadata: metadata, contract: contract, values: values}
	d.commit(identity)
	return &Derived{Component: component, InputType: inputType, OutputType: plan.outputType, Plan: plan, Handler: handler, Type: descriptor}, descriptor, nil
}
