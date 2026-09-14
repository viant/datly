package openapi

import (
	"fmt"
	documentation "github.com/viant/datly/documentation"
	"reflect"
	"strings"

	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
)

type pathBuilder struct {
	document   *openapi3.OpenAPI
	inputs     *registry.InputCatalog
	schemas    *schemaBuilder
	shapes     map[string]string
	operations map[string]bool
}

func (b *pathBuilder) add(entry *registry.RegisteredComponent, endpoint *spec.Route) error {
	template, err := route.CompilePathTemplate(endpoint.Path)
	if err != nil {
		return err
	}
	path := template.Path()
	shape := path
	for _, name := range template.Parameters() {
		shape = strings.ReplaceAll(shape, "{"+name+"}", "{}")
	}
	if previous, ok := b.shapes[shape]; ok && previous != path {
		return fmt.Errorf("equivalent OpenAPI paths %q and %q", previous, path)
	}
	b.shapes[shape] = path
	fields, err := b.inputs.FieldsFor(entry.Component.Key, spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path})
	if err != nil {
		return err
	}
	for name, schema := range entry.Documentation.Schemas() {
		if previous := b.document.Components.Schemas[name]; previous != nil && !reflect.DeepEqual(previous, schema) {
			return fmt.Errorf("authored schema identity collision %s", name)
		}
		b.document.Components.Schemas[name] = schema
	}
	schemas := newSchemaBuilder(b.document.Components.Schemas)
	schemas.identities = b.schemas.identities
	schemas.docs = entry.Documentation
	schemas.scope = entry.Component.Key.String()
	annotation := entry.Documentation.Operation(endpoint.Path, documentation.Annotation{Description: entry.Component.Description, Example: entry.Component.Example})
	operation := &openapi3.Operation{Description: annotation.Description, Summary: endpoint.Name}
	// Include route identity: component names and route names are not globally unique.
	operation.OperationID = strings.ToUpper(endpoint.Method) + ":" + path
	if b.operations[operation.OperationID] {
		return fmt.Errorf("duplicate route %s %s", endpoint.Method, path)
	}
	b.operations[operation.OperationID] = true
	inputs := inputBuilder{schemas: schemas, template: template, endpoint: endpoint}
	if err = inputs.build(fields, operation); err != nil {
		return err
	}
	responses := responseBuilder{schemas: schemas}
	operation.Responses, err = responses.build(entry, endpoint)
	if err != nil {
		return err
	}
	if err = (&securityBuilder{schemes: b.document.Components.SecuritySchemes}).apply(endpoint, fields, operation); err != nil {
		return err
	}
	item := b.document.Paths[path]
	if item == nil {
		item = &openapi3.PathItem{}
		b.document.Paths[path] = item
	}
	switch strings.ToUpper(strings.TrimSpace(endpoint.Method)) {
	case "GET":
		item.Get = operation
	case "POST":
		item.Post = operation
	case "PUT":
		item.Put = operation
	case "PATCH":
		item.Patch = operation
	case "DELETE":
		item.Delete = operation
	case "HEAD":
		item.Head = operation
	case "OPTIONS":
		item.Options = operation
	case "TRACE":
		item.Trace = operation
	default:
		return fmt.Errorf("method %q cannot be represented in OpenAPI 3.0.1", endpoint.Method)
	}
	return nil
}
