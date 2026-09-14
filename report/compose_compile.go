package report

import (
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
	inputType, err := (xshape.Runtime{}).Struct([]xshape.RuntimeField{
		inputField("Cubes", reflect.SliceOf(frameType), fmt.Sprintf("Ordered cube frames, minimum 1, maximum %d", config.MaxCubes)),
		{Name: "SQL", Type: reflect.TypeFor[string](), Tag: fieldTag("sql", "Validated wrapper SELECT; cubes[N-1] maps exactly to $CubeSQLN AS tN. Start FROM $CubeSQL1 AS t1 and join subsequent frames in order.")},
	})
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
			rowField, ok := typecatalog.FieldByName(rowType, column.fieldName)
			if !ok {
				return nil, nil, fmt.Errorf("compose column %s requires a resolved row field", column.publicName)
			}
			fields = append(fields, cubecompose.Field{Name: column.sqlName, SQLName: column.sqlName, Type: rowField.Type, Role: section.role})
			if column.sqlName != column.name {
				fields = append(fields, cubecompose.Field{Name: column.name, SQLName: column.sqlName, Type: rowField.Type, Role: section.role})
			}
		}
	}
	catalog, err := cubecompose.NewCatalog(fields...)
	if err != nil {
		return nil, nil, err
	}
	params := []*spec.Parameter{bodyParam("Cubes", inputType), bodyParam("SQL", inputType)}
	required := true
	for _, param := range params {
		param.Required = &required
	}
	component := &spec.Component{
		Key: identity.key, Name: identity.key.Name, Description: strings.TrimSpace(source.Component.Description + " cube composition"),
		Settings: &spec.Settings{InputType: descriptor.Key()}, TypeContext: source.Component.TypeContext.Clone(), Parameters: params,
		Routes: []*spec.Route{{Method: identity.route.Method, Path: identity.route.Path, APIKeyHeader: route.APIKeyHeader, APIKeyValue: route.APIKeyValue}},
	}
	if config.MCPTool == nil || *config.MCPTool {
		component.Routes[0].MCP = []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: identity.key.Name, Description: component.Description}}
	}
	// A separate source plan keeps composition from mutating ordinary cube input.
	plan := &Plan{target: sourcePlan.target, inputType: inputType, outputType: reflect.TypeFor[ComposeResponse]()}
	contract, ok := source.Input.ForRoute(sourcePlan.target.Route)
	if !ok {
		return nil, nil, fmt.Errorf("compose source route contract is missing")
	}
	handler := &composeHandler{inputType: inputType, source: sourcePlan, config: config, catalog: catalog, metadata: metadata, contract: contract}
	d.commit(identity)
	return &Derived{Component: component, InputType: inputType, OutputType: plan.outputType, Plan: plan, Handler: handler, Type: descriptor}, descriptor, nil
}
