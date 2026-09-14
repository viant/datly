package openapi

import (
	"fmt"
	documentation "github.com/viant/datly/documentation"
	"strings"

	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

type responseBuilder struct{ schemas *schemaBuilder }

func (b *responseBuilder) build(entry *registry.RegisteredComponent, endpoint *spec.Route) (openapi3.Responses, error) {
	if entry.Output.TransportReady() {
		responses := entry.Documentation.Responses(endpoint.Path)
		if len(responses) == 0 {
			return nil, fmt.Errorf("transport-ready output requires authored response documentation for %s", endpoint.Path)
		}
		if strings.EqualFold(endpoint.Method, "HEAD") {
			for _, response := range responses {
				response.Content = nil
			}
		}
		return responses, nil
	}
	if len(entry.Documentation.Responses(endpoint.Path)) > 0 {
		return nil, fmt.Errorf("authored response schema cannot replace typed output authority for %s", endpoint.Path)
	}
	wire, err := entry.Output.Wire(endpoint.Marshaller)
	if err != nil {
		return nil, err
	}
	var schema *openapi3.Schema
	if wire.JSON != nil {
		schema, err = b.schemas.wire(wire.JSON)
	} else {
		schema, err = b.schemas.schema(wire.Type, false)
	}
	if err != nil {
		return nil, err
	}
	if wire.Binary {
		schema.Format = "binary"
	}
	description := "Success response"
	response := &openapi3.Response{Description: &description}
	if wire.ContentDisposition != "" {
		response.Headers = openapi3.Headers{"Content-Disposition": {Schema: &openapi3.Schema{Type: "string", Enum: []interface{}{wire.ContentDisposition}}}}
	}
	if !strings.EqualFold(endpoint.Method, "HEAD") {
		media := &openapi3.MediaType{Schema: schema}
		annotation := entry.Documentation.Operation(endpoint.Path, documentation.Annotation{Example: entry.Component.Example})
		if annotation.Example != "" {
			media.Example = annotation.Example
		}
		response.Content = openapi3.Content{wire.ContentType: media}
	}
	// The ordinary adapter defaults to 200. Handler-selected statuses and error
	// bodies are dynamic, so no speculative error responses are advertised.
	return openapi3.Responses{"200": response}, nil
}
