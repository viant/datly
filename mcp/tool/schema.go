package tool

import (
	docs "github.com/viant/datly/documentation"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"reflect"
)

type schemaProjector struct{ docs *docs.Snapshot }

func schemaForType(t reflect.Type, _ map[reflect.Type]bool) (map[string]interface{}, error) {
	return (&schemaProjector{}).argument(t, "", "")
}
func (p *schemaProjector) argument(t reflect.Type, path, property string) (map[string]interface{}, error) {
	return (jsonschema.Reflector{Annotate: func(path string, field reflect.StructField) (string, any) {
		annotation := p.docs.StructField(path, field)
		var example any
		if annotation.Example != "" {
			example = annotation.Example
		}
		return annotation.Description, example
	}}).Compile(jsonschema.Request{Type: t, Path: path, Property: property})
}
